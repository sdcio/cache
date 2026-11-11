package cache_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

var stores = []struct {
	name   string
	initFn storeInit
}{
	{"filesystem", filesystem.PreConfigureFilesystemInitFunc},
	{"badgerdb", badgerdb.PreconfigureBadgerDbInitFunc},
}

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
	for _, s := range stores {
		for _, cancelCtx := range []bool{true, false} {
			for _, errChanCap := range []int{0, 1} {
				t.Run(fmt.Sprintf("%s/cancelCtx=%t/errChanCap=%d", s.name, cancelCtx, errChanCap), func(t *testing.T) {
					c := newCache(t, s.initFn)
					createInstanceWithIntents(t, c, "victim", 20)
					createInstanceWithIntents(t, c, "other", 0)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					intentChan := make(chan *types.Intent)
					errChan := make(chan error, errChanCap)
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
							return
						}
						if err := <-errChan; errChanCap > 0 && !errors.Is(err, context.Canceled) {
							t.Errorf("buffered errChan got %v, want an error wrapping context.Canceled", err)
						}
					}

					receiveIntent(t, intentChan, errChan)

					// From here on, the consumer stops reading.
					if cancelCtx {
						cancel()
						waitProducerExit()
					}

					returnsWithin(t, "InstanceDelete of the streamed instance", func() error {
						return c.InstanceDelete(context.Background(), "victim")
					})
					returnsWithin(t, "InstanceIntentModify on another instance", func() error {
						return c.InstanceIntentModify(context.Background(), "other", "intent", []byte("data"))
					})

					if !cancelCtx {
						cancel()
						waitProducerExit()
					}
				})
			}
		}
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

func TestInstanceIntentGetAllIsSnapshot(t *testing.T) {
	for _, s := range stores {
		t.Run(s.name, func(t *testing.T) {
			c := newCache(t, s.initFn)
			createInstanceWithIntents(t, c, "victim", 20)

			intentChan := make(chan *types.Intent)
			errChan := make(chan error, 1)
			go c.InstanceIntentGetAll(t.Context(), "victim", nil, intentChan, errChan)

			first := receiveIntent(t, intentChan, errChan)
			got := map[string]string{first.Name(): string(first.Data())}
			returnsWithin(t, "intent writes during the stream", func() error {
				ctx := context.Background()
				return errors.Join(
					c.InstanceIntentDelete(ctx, "victim", "intent-05", false),
					c.InstanceIntentModify(ctx, "victim", "intent-10", []byte("modified")),
					c.InstanceIntentModify(ctx, "victim", "intent-20", []byte("data")),
				)
			})
			for intent := range intentChan {
				got[intent.Name()] = string(intent.Data())
			}
			if err := <-errChan; err != nil {
				t.Fatalf("stream failed: %v", err)
			}

			want := map[string]string{}
			for i := range 20 {
				want[fmt.Sprintf("intent-%02d", i)] = "data"
			}
			if !maps.Equal(got, want) {
				t.Errorf("stream is not a snapshot taken when it started:\ngot  %v\nwant %v", got, want)
			}
		})
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

func TestInstanceCreateWaitsForDeleteOfSameName(t *testing.T) {
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
	returnsWithin(t, "InstanceCreate of the instance being deleted, with a short ctx", func() error {
		shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if err := c.InstanceCreate(shortCtx, "a"); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("got error %v, want it to wait for the delete until ctx expires", err)
		}
		return nil
	})

	createErr := make(chan error, 1)
	go func() { createErr <- c.InstanceCreate(ctx, "a") }()
	close(release)
	for what, ch := range map[string]chan error{"InstanceDelete": deleteErr, "InstanceCreate queued behind the delete": createErr} {
		select {
		case err := <-ch:
			if err != nil {
				t.Errorf("%s: %v", what, err)
			}
		case <-time.After(testTimeout):
			t.Errorf("%s did not return after the store delete finished", what)
		}
	}
	if !c.InstanceExists(ctx, "a") {
		t.Error("instance a does not exist after delete and re-create")
	}
}

func TestInstanceCreateDoesNotBlockOtherInstances(t *testing.T) {
	ctx := t.Context()
	initializing := make(chan struct{})
	release := make(chan struct{})
	c, err := cache.NewCache(func(name string) (store.Store, error) {
		if name == "slow" {
			close(initializing)
			<-release
		}
		return &blockingStore{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	createErr := make(chan error, 1)
	go func() { createErr <- c.InstanceCreate(ctx, "slow") }()
	select {
	case <-initializing:
	case <-time.After(testTimeout):
		t.Fatal("store initializer was not called")
	}

	returnsWithin(t, "InstanceCreate of another instance while a store is initializing", func() error {
		return c.InstanceCreate(ctx, "fast")
	})

	close(release)
	select {
	case err := <-createErr:
		if err != nil {
			t.Fatalf("InstanceCreate: %v", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("InstanceCreate did not return after the store was initialized")
	}
}
