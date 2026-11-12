package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sdcio/cache/pkg/store"
	"github.com/sdcio/cache/pkg/types"
)

type Cache struct {
	instances map[string]*cacheInstance
	// pending holds names with a create, close or delete in flight; the channel is closed when it ends.
	pending        map[string]chan struct{}
	instancesMutex *sync.RWMutex

	storeInitializer func(instanceName string) (store.Store, error)
}

func NewCache(storeInitializer func(instanceName string) (store.Store, error)) (*Cache, error) {
	c := &Cache{
		instances:        map[string]*cacheInstance{},
		pending:          map[string]chan struct{}{},
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
	return c.removeInstance(ctx, cacheInstanceName, func(ci *cacheInstance) error {
		return ci.Delete(ctx)
	})
}

func (c *Cache) InstanceClose(ctx context.Context, cacheInstanceName string) error {
	return c.removeInstance(ctx, cacheInstanceName, (*cacheInstance).Close)
}

// reserve waits until no create, close or delete is in flight for name, then marks one as in flight.
// Store I/O for name then runs without instancesMutex, so other instances never wait on it.
func (c *Cache) reserve(ctx context.Context, name string) error {
	c.instancesMutex.Lock()
	for {
		done, busy := c.pending[name]
		if !busy {
			c.pending[name] = make(chan struct{})
			c.instancesMutex.Unlock()
			return nil
		}
		c.instancesMutex.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		c.instancesMutex.Lock()
	}
}

func (c *Cache) release(name string) {
	c.instancesMutex.Lock()
	defer c.instancesMutex.Unlock()
	close(c.pending[name])
	delete(c.pending, name)
}

// removeInstance unmaps the instance and runs fn on it; the instance is restored if fn fails.
func (c *Cache) removeInstance(ctx context.Context, cacheInstanceName string, fn func(*cacheInstance) error) error {
	if err := c.reserve(ctx, cacheInstanceName); err != nil {
		return err
	}
	defer c.release(cacheInstanceName)

	c.instancesMutex.Lock()
	ci, exists := c.instances[cacheInstanceName]
	delete(c.instances, cacheInstanceName)
	c.instancesMutex.Unlock()
	if !exists {
		return fmt.Errorf("%w: %s", ErrorCacheInstanceNotFound, cacheInstanceName)
	}

	if err := fn(ci); err != nil {
		c.instancesMutex.Lock()
		c.instances[cacheInstanceName] = ci
		c.instancesMutex.Unlock()
		return err
	}
	return nil
}

func (c *Cache) InstanceCreate(ctx context.Context, cacheInstanceName string) error {
	if err := c.reserve(ctx, cacheInstanceName); err != nil {
		return err
	}
	defer c.release(cacheInstanceName)

	if c.InstanceExists(ctx, cacheInstanceName) {
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

	c.instancesMutex.Lock()
	c.instances[cacheInstanceName] = ci
	c.instancesMutex.Unlock()

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
		store.FinishIntentStream(ctx, nil, err, intentChan, errChan)
		return
	}
	ci.InstanceIntentGetAll(ctx, excludeIntentNames, intentChan, errChan)
}

// Close closes every instance, continuing past failures; instances that fail to close stay registered.
func (c *Cache) Close() error {
	var errs []error
	for _, name := range c.InstancesList(context.Background()) {
		err := c.InstanceClose(context.Background(), name)
		if err != nil && !errors.Is(err, ErrorCacheInstanceNotFound) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
