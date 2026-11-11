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
	m    *sync.Mutex
	// closed stops stale handles from touching a path that a new store may now own; guarded by m.
	closed bool
}

func NewFilesystemStore(path string) (*filesystem, error) {
	f := &filesystem{
		path: path,
		m:    &sync.Mutex{},
	}
	err := os.MkdirAll(f.getIntentsPath(), 0755)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *filesystem) IntentModify(ctx context.Context, intentName string, data []byte) error {
	f.m.Lock()
	defer f.m.Unlock()
	if f.closed {
		return errStoreClosed
	}

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
	f.m.Lock()
	defer f.m.Unlock()
	if f.closed {
		return false, errStoreClosed
	}
	return f.intentExists(intentName)
}

func (f *filesystem) IntentsList(ctx context.Context) ([]string, error) {
	f.m.Lock()
	defer f.m.Unlock()
	if f.closed {
		return nil, errStoreClosed
	}
	return f.intentsList()
}

func (f *filesystem) IntentGet(ctx context.Context, intentName string) ([]byte, error) {
	f.m.Lock()
	defer f.m.Unlock()
	if f.closed {
		return nil, errStoreClosed
	}
	return f.intentGet(intentName)
}

func (f *filesystem) IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error) {
	intents, err := f.intentsSnapshot(ctx, excludeIntentNames)
	store.StreamIntents(ctx, intents, err, intentChan, errChan)
}

// intentsSnapshot reads all intents under one lock, so streaming them needs no lock.
func (f *filesystem) intentsSnapshot(ctx context.Context, excludeIntentNames []string) ([]*types.Intent, error) {
	f.m.Lock()
	defer f.m.Unlock()
	if f.closed {
		return nil, errStoreClosed
	}

	intentNames, err := f.intentsList()
	if err != nil {
		return nil, err
	}
	intents := make([]*types.Intent, 0, len(intentNames))
	for _, intentName := range intentNames {
		if slices.Contains(excludeIntentNames, intentName) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("streaming canceled: %w", err)
		}
		data, err := f.intentGet(intentName)
		if err != nil {
			return nil, err
		}
		intents = append(intents, types.NewIntent(intentName, data))
	}
	return intents, nil
}

// errorIntentNonExists requires the caller to hold f.m mutex
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

// intentExists requires the caller to hold f.m mutex
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
	f.m.Lock()
	defer f.m.Unlock()
	if f.closed {
		return errStoreClosed
	}
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
	f.m.Lock()
	defer f.m.Unlock()
	f.closed = true
	return os.RemoveAll(f.path)
}

// Close the Store
func (f *filesystem) Close() error {
	f.m.Lock()
	defer f.m.Unlock()
	f.closed = true
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
