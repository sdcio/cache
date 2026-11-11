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

type filesystem struct {
	path string
	m    *sync.Mutex
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
	return f.intentExists(intentName)
}

func (f *filesystem) IntentsList(ctx context.Context) ([]string, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.intentsList()
}

func (f *filesystem) IntentGet(ctx context.Context, intentName string) ([]byte, error) {
	f.m.Lock()
	defer f.m.Unlock()
	return f.intentGet(intentName)
}

func (f *filesystem) IntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error) {
	defer close(intentChan)
	defer close(errChan)

	if err := f.intentGetAll(ctx, excludeIntentNames, intentChan); err != nil {
		select {
		case errChan <- err:
		case <-ctx.Done():
		}
	}
}

// intentGetAll never holds f.m while sending, so a stalled consumer cannot block the store.
func (f *filesystem) intentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent) error {
	f.m.Lock()
	intentNames, err := f.intentsList()
	f.m.Unlock()
	if err != nil {
		return err
	}

	for _, intentName := range intentNames {
		if slices.Contains(excludeIntentNames, intentName) {
			continue
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("streaming canceled: %v", err)
		}
		f.m.Lock()
		data, err := os.ReadFile(f.getIntentDataPath(intentName))
		f.m.Unlock()
		if errors.Is(err, os.ErrNotExist) {
			continue // deleted since listing
		}
		if err != nil {
			return err
		}
		select {
		case intentChan <- types.NewIntent(intentName, data):
		case <-ctx.Done():
			return fmt.Errorf("streaming canceled: %v", ctx.Err())
		}
	}
	return nil
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
	return os.RemoveAll(f.path)
}

// Close the Store
func (f *filesystem) Close() error {
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
