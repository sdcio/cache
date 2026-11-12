package filesystem

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"sync"

	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/types"
)

var errStoreClosed = errors.New("filesystem store is closed")

type filesystem struct {
	path string
	// sem is a mutex (capacity 1) whose wait can be abandoned on ctx cancel or store close.
	sem       chan struct{}
	done      chan struct{} // closed by Close and Delete
	closeOnce sync.Once
}

func NewFilesystemStore(path string) (*filesystem, error) {
	f := &filesystem{
		path: path,
		sem:  make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	err := os.MkdirAll(f.getIntentsPath(), 0755)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *filesystem) lock(ctx context.Context) error {
	select {
	case f.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-f.done:
		return errStoreClosed
	}
	// select picks randomly when sem and done are both ready, so check done again.
	select {
	case <-f.done:
		f.unlock()
		return errStoreClosed
	default:
		return nil
	}
}

func (f *filesystem) unlock() {
	<-f.sem
}

// shutdown ends current and future operations and returns holding the lock.
func (f *filesystem) shutdown() {
	f.closeOnce.Do(func() { close(f.done) })
	f.sem <- struct{}{}
}

func (f *filesystem) IntentModify(ctx context.Context, intentName string, data []byte) error {
	if err := f.lock(ctx); err != nil {
		return err
	}
	defer f.unlock()

	err := os.MkdirAll(f.getIntentPath(intentName), 0755)
	if err != nil {
		return err
	}
	err = os.WriteFile(f.getIntentDataPath(intentName), data, 0755)
	if err != nil {
		return err
	}
	return nil
}

func (f *filesystem) IntentExists(ctx context.Context, intentName string) (bool, error) {
	if err := f.lock(ctx); err != nil {
		return false, err
	}
	defer f.unlock()
	return f.intentExists(intentName)
}

func (f *filesystem) IntentsList(ctx context.Context) ([]string, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	return f.intentsList()
}

func (f *filesystem) IntentGet(ctx context.Context, intentName string) ([]byte, error) {
	if err := f.lock(ctx); err != nil {
		return nil, err
	}
	defer f.unlock()
	return f.intentGet(intentName)
}

// IntentGetAll holds the lock for the whole stream, so the stream is consistent and writes wait for it.
func (f *filesystem) IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error) {
	store.FinishIntentStream(ctx, f.done, f.intentGetAll(ctx, excludeIntentNames, intentChan), intentChan, errChan)
}

func (f *filesystem) intentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent) error {
	if err := f.lock(ctx); err != nil {
		return err
	}
	defer f.unlock()

	intentNames, err := f.intentsList()
	if err != nil {
		return err
	}
	for _, intentName := range intentNames {
		if slices.Contains(excludeIntentNames, intentName) {
			continue
		}
		data, err := f.intentGet(intentName)
		if err != nil {
			return err
		}
		select {
		case intentChan <- types.NewIntent(intentName, data):
		case <-ctx.Done():
			return fmt.Errorf("streaming canceled: %w", ctx.Err())
		case <-f.done:
			return errStoreClosed
		}
	}
	return nil
}

// errorIntentNonExists requires the caller to hold the lock
func (f *filesystem) errorIntentNonExists(intentName string) error {
	exists, err := f.intentExists(intentName)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("intent %q does not exist", intentName)
	}
	return nil
}

// intentExists requires the caller to hold the lock
func (f *filesystem) intentExists(intentName string) (bool, error) {
	_, err := os.Stat(f.getIntentDataPath(intentName))

	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (f *filesystem) intentGet(intentName string) ([]byte, error) {
	err := f.errorIntentNonExists(intentName)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(f.getIntentDataPath(intentName))
}

func (f *filesystem) intentsList() ([]string, error) {
	content, err := os.ReadDir(f.getIntentsPath())
	if err != nil {
		return nil, err
	}
	result := make([]string, 0, len(content))
	for _, entry := range content {
		result = append(result, entry.Name())
	}
	return result, nil
}

func (f *filesystem) IntentDelete(ctx context.Context, intentName string, IgnoreNonExisting bool) error {
	if err := f.lock(ctx); err != nil {
		return err
	}
	defer f.unlock()
	err := f.errorIntentNonExists(intentName)
	if err != nil {
		if IgnoreNonExisting {
			return nil
		}
		return err
	}
	return os.RemoveAll(f.getIntentPath(intentName))
}

// Delete the Store. Implicitly closes store
func (f *filesystem) Delete() error {
	f.shutdown()
	defer f.unlock()
	return os.RemoveAll(f.path)
}

// Close the Store
func (f *filesystem) Close() error {
	f.shutdown()
	f.unlock()
	return nil
}

func (f *filesystem) getIntentDataPath(intentName string) string {
	return path.Join(f.getIntentPath(intentName), "data")
}

func (f *filesystem) getIntentPath(intentName string) string {
	return path.Join(f.getIntentsPath(), intentName)
}

func (f *filesystem) getIntentsPath() string {
	return path.Join(f.path, "intents")
}

func PreConfigureFilesystemInitFunc(basepath string) (func(cachename string) (store.Store, error), error) {
	_, err := os.Stat(basepath)
	if err != nil {
		err := os.MkdirAll(basepath, 0755)
		if err != nil {
			return nil, err
		}
	}

	return func(cacheName string) (store.Store, error) {
		return NewFilesystemStore(path.Join(basepath, cacheName))
	}, nil
}
