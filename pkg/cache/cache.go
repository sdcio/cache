package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/types"
)

type Cache struct {
	instances      map[string]*cacheInstance
	instancesMutex *sync.RWMutex

	storeInitializer func(instanceName string) (store.Store, error)
}

func NewCache(storeInitializer func(instanceName string) (store.Store, error)) (*Cache, error) {
	c := &Cache{
		instances:        map[string]*cacheInstance{},
		storeInitializer: storeInitializer,
		instancesMutex:   &sync.RWMutex{},
	}

	return c, nil
}

func (c *Cache) getCacheInstance(cacheName string) (*cacheInstance, error) {
	c.instancesMutex.RLock()
	defer c.instancesMutex.RUnlock()
	ci, exists := c.instances[cacheName]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrorCacheInstanceNotFound, cacheName)
	}
	return ci, nil
}

func (c *Cache) InstanceDelete(ctx context.Context, cacheInstanceName string) error {
	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()

	ci, exists := c.instances[cacheInstanceName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrorCacheInstanceNotFound, cacheInstanceName)
	}

	err := ci.Delete(ctx)
	if err != nil {
		return err
	}

	delete(c.instances, cacheInstanceName)

	return nil
}

func (c *Cache) InstanceClose(ctx context.Context, cacheInstanceName string) error {
	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()

	ci, exists := c.instances[cacheInstanceName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrorCacheInstanceNotFound, cacheInstanceName)
	}

	ci.Close()
	delete(c.instances, cacheInstanceName)

	return nil
}

func (c *Cache) InstanceCreate(ctx context.Context, cacheInstanceName string) error {
	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()

	if _, exists := c.instances[cacheInstanceName]; exists {
		return fmt.Errorf("%w: %s", ErrorCacheInstanceAlreadyExists, cacheInstanceName)
	}

	store, err := c.storeInitializer(cacheInstanceName)
	if err != nil {
		return err
	}
	ci, err := newCacheInstance(store)
	if err != nil {
		return err
	}

	c.instances[cacheInstanceName] = ci

	return nil
}

func (c *Cache) InstanceExists(ctx context.Context, cacheName string) bool {
	c.instancesMutex.RLock()
	defer c.instancesMutex.RUnlock()
	_, exists := c.instances[cacheName]
	return exists
}

func (c *Cache) InstancesList(ctx context.Context) []string {
	result := make([]string, 0, len(c.instances))
	for name := range c.instances {
		result = append(result, name)
	}
	return result
}

func (c *Cache) InstanceIntentGet(ctx context.Context, cacheName string, intentName string) ([]byte, error) {
	ci, err := c.getCacheInstance(cacheName)
	if err != nil {
		return nil, err
	}
	return ci.IntentGet(ctx, intentName)
}

func (c *Cache) InstanceIntentsList(ctx context.Context, cacheName string) ([]string, error) {
	ci, err := c.getCacheInstance(cacheName)
	if err != nil {
		return nil, err
	}

	return ci.IntentsList(ctx)
}

func (c *Cache) InstanceIntentModify(ctx context.Context, cacheName string, intentName string, data []byte) error {
	ci, err := c.getCacheInstance(cacheName)
	if err != nil {
		return err
	}

	return ci.IntentModify(ctx, intentName, data)
}

func (c *Cache) InstanceIntentDelete(ctx context.Context, cacheName string, intentName string, IgnoreNonExisting bool) error {
	ci, err := c.getCacheInstance(cacheName)
	if err != nil {
		return err
	}
	return ci.InstanceIntentDelete(ctx, intentName, IgnoreNonExisting)
}

func (c *Cache) InstanceIntentExists(ctx context.Context, cacheName string, intentName string) (bool, error) {
	ci, err := c.getCacheInstance(cacheName)
	if err != nil {
		return false, err
	}
	return ci.InstanceIntentExists(ctx, intentName)
}

func (c *Cache) InstanceIntentGetAll(ctx context.Context, cacheName string, excludeIntentNames []string, intentChan chan<- *types.Intent, errChan chan<- error) {
	ci, err := c.getCacheInstance(cacheName)
	if err != nil {
		errChan <- err
		return
	}
	ci.InstanceIntentGetAll(ctx, excludeIntentNames, intentChan, errChan)
}

func (c *Cache) Close() error {
	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()

	for name, instance := range c.instances {
		err := instance.Close()
		if err != nil {
			return err
		}
		delete(c.instances, name)
	}

	return nil
}
