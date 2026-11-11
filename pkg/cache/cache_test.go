package cache_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/sdcio/cache/pkg/cache"
	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/store/badgerdb"
	"github.com/sdcio/cache/pkg/store/filesystem"
	"github.com/sdcio/cache/pkg/types"
)

const testTimeout = 5 * time.Second

type storeInit func(basepath string) (func(string) (store.Store, error), error)

func newCache(t *testing.T, initFn storeInit) *cache.Cache {
	t.Helper()
	storeInitializer, err := initFn(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c, err := cache.NewCache(storeInitializer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { returnsWithin(t, "Cache.Close", c.Close) })
	return c
}

func createInstanceWithIntents(t *testing.T, c *cache.Cache, name string, n int) {
	t.Helper()
	ctx := context.Background()
	if err := c.InstanceCreate(ctx, name); err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if err := c.InstanceIntentModify(ctx, name, fmt.Sprintf("intent-%02d", i), []byte("data")); err != nil {
			t.Fatal(err)
		}
	}
}

// returnsWithin reports a test error if fn fails or does not return within testTimeout.
func returnsWithin(t *testing.T, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("%s: %v", what, err)
		}
	case <-time.After(testTimeout):
		t.Errorf("%s did not return within %v: cache is deadlocked", what, testTimeout)
	}
}

func receiveIntent(t *testing.T, intentChan <-chan *types.Intent, errChan <-chan error) *types.Intent {
	t.Helper()
	select {
	case intent := <-intentChan:
		return intent
	case err := <-errChan:
		t.Fatalf("unexpected stream error: %v", err)
	case <-time.After(testTimeout):
		t.Fatalf("no intent received within %v", testTimeout)
	}
	return nil
}

func TestInstanceIntentGetAllAbandonedConsumer(t *testing.T) {
	tests := []struct {
		store     string
		initFn    storeInit
		cancelCtx bool
	}{
		{"filesystem", filesystem.PreConfigureFilesystemInitFunc, true},
		{"filesystem", filesystem.PreConfigureFilesystemInitFunc, false},
		{"badgerdb", badgerdb.PreconfigureBadgerDbInitFunc, true},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/cancelCtx=%t", tt.store, tt.cancelCtx), func(t *testing.T) {
			c := newCache(t, tt.initFn)
			createInstanceWithIntents(t, c, "victim", 20)
			createInstanceWithIntents(t, c, "other", 0)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			intentChan := make(chan *types.Intent)
			errChan := make(chan error)
			producerDone := make(chan struct{})
			go func() {
				defer close(producerDone)
				c.InstanceIntentGetAll(ctx, "victim", nil, intentChan, errChan)
			}()
			waitProducerExit := func() {
				t.Helper()
				select {
				case <-producerDone:
				case <-time.After(testTimeout):
					t.Errorf("InstanceIntentGetAll did not return within %v after ctx was cancelled", testTimeout)
				}
			}

			receiveIntent(t, intentChan, errChan)

			// From here on, intentChan and errChan are never read again.
			if tt.cancelCtx {
				cancel()
				waitProducerExit()
			}

			returnsWithin(t, "InstanceDelete of the streamed instance", func() error {
				return c.InstanceDelete(context.Background(), "victim")
			})
			returnsWithin(t, "InstanceIntentModify on another instance", func() error {
				return c.InstanceIntentModify(context.Background(), "other", "intent", []byte("data"))
			})

			if !tt.cancelCtx {
				cancel()
				waitProducerExit()
			}
		})
	}
}

func TestInstanceIntentGetAllMissingInstance(t *testing.T) {
	c := newCache(t, filesystem.PreConfigureFilesystemInitFunc)
	intentChan := make(chan *types.Intent)
	errChan := make(chan error)
	go c.InstanceIntentGetAll(t.Context(), "missing", nil, intentChan, errChan)

	select {
	case err := <-errChan:
		if !errors.Is(err, cache.ErrorCacheInstanceNotFound) {
			t.Fatalf("got error %v, want %v", err, cache.ErrorCacheInstanceNotFound)
		}
	case <-time.After(testTimeout):
		t.Fatal("no error received for missing instance")
	}

	select {
	case err, ok := <-errChan:
		if ok {
			t.Errorf("unexpected second error: %v", err)
		}
	case <-time.After(testTimeout):
		t.Error("errChan was not closed")
	}
	select {
	case intent, ok := <-intentChan:
		if ok {
			t.Errorf("unexpected intent: %v", intent)
		}
	case <-time.After(testTimeout):
		t.Error("intentChan was not closed")
	}
}

func TestFilesystemIntentGetAllSkipsIntentDeletedMidStream(t *testing.T) {
	c := newCache(t, filesystem.PreConfigureFilesystemInitFunc)
	createInstanceWithIntents(t, c, "victim", 20)

	intentChan := make(chan *types.Intent)
	errChan := make(chan error, 1)
	go c.InstanceIntentGetAll(t.Context(), "victim", nil, intentChan, errChan)

	got := []string{receiveIntent(t, intentChan, errChan).Name()}
	returnsWithin(t, "InstanceIntentDelete during stream", func() error {
		return c.InstanceIntentDelete(context.Background(), "victim", "intent-05", false)
	})
	for intent := range intentChan {
		got = append(got, intent.Name())
	}
	if err := <-errChan; err != nil {
		t.Fatalf("stream failed: %v", err)
	}
	if len(got) != 19 || slices.Contains(got, "intent-05") {
		t.Errorf("got intents %v, want intent-00..intent-19 without intent-05", got)
	}
}

// blockingStore's Delete blocks until release is closed; other Store methods are unused.
type blockingStore struct {
	store.Store
	deleting chan struct{}
	release  chan struct{}
}

func (s *blockingStore) Delete() error {
	close(s.deleting)
	<-s.release
	return nil
}

func (s *blockingStore) Close() error { return nil }

func TestInstanceCreateWhileDeleteInProgress(t *testing.T) {
	ctx := t.Context()
	deleting := make(chan struct{})
	release := make(chan struct{})
	c, err := cache.NewCache(func(string) (store.Store, error) {
		return &blockingStore{deleting: deleting, release: release}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.InstanceCreate(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	deleteErr := make(chan error, 1)
	go func() { deleteErr <- c.InstanceDelete(ctx, "a") }()
	select {
	case <-deleting:
	case <-time.After(testTimeout):
		t.Fatal("store Delete was not called")
	}

	returnsWithin(t, "InstanceCreate of another instance while a store delete is stuck", func() error {
		return c.InstanceCreate(ctx, "b")
	})
	returnsWithin(t, "InstanceCreate of the instance being deleted", func() error {
		if err := c.InstanceCreate(ctx, "a"); !errors.Is(err, cache.ErrorCacheInstanceAlreadyExists) {
			return fmt.Errorf("got error %v, want %v", err, cache.ErrorCacheInstanceAlreadyExists)
		}
		return nil
	})

	close(release)
	select {
	case err := <-deleteErr:
		if err != nil {
			t.Fatalf("InstanceDelete: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("InstanceDelete did not return after the store delete finished")
	}
	if err := c.InstanceCreate(ctx, "a"); err != nil {
		t.Fatalf("InstanceCreate after delete finished: %v", err)
	}
}
