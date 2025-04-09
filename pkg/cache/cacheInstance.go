package cache

import (
	"context"

	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/types"
)

type cacheInstance struct {
	store store.Store
}

func newCacheInstance(store store.Store) (*cacheInstance, error) {
	ci := &cacheInstance{
		store: store,
	}
	return ci, nil
}

func (ci *cacheInstance) IntentsList(ctx context.Context) ([]string, error) {
	return ci.store.IntentsList(ctx)
}

func (ci *cacheInstance) IntentGet(ctx context.Context, intentName string) ([]byte, error) {
	return ci.store.IntentGet(ctx, intentName)
}

func (ci *cacheInstance) Close() error {
	return ci.store.Close()
}

func (ci *cacheInstance) Delete(ctx context.Context) error {
	return ci.store.Delete()
}

func (ci *cacheInstance) IntentModify(ctx context.Context, intentName string, data []byte) error {
	return ci.store.IntentModify(ctx, intentName, data)
}

func (ci *cacheInstance) InstanceIntentDelete(ctx context.Context, intentName string) error {
	return ci.store.IntentDelete(ctx, intentName)
}

func (ci *cacheInstance) InstanceIntentExists(ctx context.Context, intentName string) (bool, error) {
	return ci.store.IntentExists(ctx, intentName)
}

func (ci *cacheInstance) InstanceIntentGetAll(ctx context.Context, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error) {
	ci.store.IntentGetAll(ctx, excludeIntentNames, intentChan, errChan)
}
