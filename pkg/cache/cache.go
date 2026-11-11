package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/types"
)

type Cache struct {
	instances map[string]*cacheInstance
	// removing holds names taken out of instances whose store is still being closed or deleted.
	removing       map[string]struct{}
	instancesMutex *sync.RWMutex

	storeInitializer func(instanceName string) (store.Store, error)
}

func NewCache(storeInitializer func(instanceName string) (store.Store, error)) (*Cache, error) {
	c := &Cache{
		instances:        map[string]*cacheInstance{},
		removing:         map[string]struct{}{},
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
	return c.removeInstance(cacheInstanceName, func(ci *cacheInstance) error {
		return ci.Delete(ctx)
	})
}

func (c *Cache) InstanceClose(ctx context.Context, cacheInstanceName string) error {
	return c.removeInstance(cacheInstanceName, (*cacheInstance).Close)
}

// removeInstance runs fn without holding instancesMutex so a stuck store cannot block other instances.
// The name stays reserved until fn returns, and the instance is restored if fn fails.
func (c *Cache) removeInstance(cacheInstanceName string, fn func(*cacheInstance) error) error {
	c.instancesMutex.Lock()
	ci, exists := c.instances[cacheInstanceName]
	if !exists {
		c.instancesMutex.Unlock()
		return fmt.Errorf("%w: %s", ErrorCacheInstanceNotFound, cacheInstanceName)
	}
	delete(c.instances, cacheInstanceName)
	c.removing[cacheInstanceName] = struct{}{}
	c.instancesMutex.Unlock()

	err := fn(ci)

	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()
	delete(c.removing, cacheInstanceName)
	if err != nil {
		c.instances[cacheInstanceName] = ci
	}
	return err
}

func (c *Cache) InstanceCreate(ctx context.Context, cacheInstanceName string) error {
	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()

	if _, exists := c.instances[cacheInstanceName]; exists {
		return fmt.Errorf("%w: %s", ErrorCacheInstanceAlreadyExists, cacheInstanceName)
	}
	if _, removing := c.removing[cacheInstanceName]; removing {
		return fmt.Errorf("%w: %s is still being removed", ErrorCacheInstanceAlreadyExists, cacheInstanceName)
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
	c.instancesMutex.RLock()
	defer c.instancesMutex.RUnlock()
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
		// Honour the store.Store contract: close both channels, errChan first.
		defer close(intentChan)
		defer close(errChan)
		select {
		case errChan <- err:
		case <-ctx.Done():
		}
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
