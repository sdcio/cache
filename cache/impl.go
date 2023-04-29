package cache

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/iptecharch/cache/config"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type cache[T proto.Message] struct {
	cfg *config.CacheConfig

	m      *sync.RWMutex
	caches map[string]*cacheInstance[T]
	bFn    func() T
}

func (c *cache[T]) Init(ctx context.Context) error {
	if c.cfg.Dir == "" {
		log.Info("initialized in ephemeral mode")
		// no persistent caches
		return nil
	}
	log.Info("initializing in persistent mode")
	if _, err := os.Stat(c.cfg.Dir); os.IsNotExist(err) {
		// caches directory does not exist, create it
		err = os.MkdirAll(c.cfg.Dir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	dirs, err := os.ReadDir(c.cfg.Dir)
	if err != nil {
		return err
	}
	log.Info("loading caches...")
	wg := new(sync.WaitGroup)
	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		cacheName := dir.Name()
		log.Debugf("initializing cache %q", cacheName)

		ccfg := &CacheInstanceConfig{
			Name:      cacheName,
			StoreType: c.cfg.StoreType,
			Ephemeral: false,
			Dir:       c.cfg.Dir,
		}

		ci := newCacheInstance(ccfg, c.bFn)

		err = ci.initFromStore(ctx, wg)
		if err != nil {
			return err
		}

		ci.m.Lock()
		c.caches[cacheName] = ci
		ci.m.Unlock()
	}
	wg.Wait()
	log.Info("caches loaded")
	return nil
}

func (c *cache[T]) Candidates(ctx context.Context, name string) ([]string, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	ci.m.RLock()
	defer ci.m.RUnlock()
	rs := make([]string, 0, len(ci.candidates))
	for n := range ci.candidates {
		rs = append(rs, n)
	}
	return rs, nil
}

func (c *cache[T]) List(ctx context.Context) []string {
	c.m.RLock()
	defer c.m.RUnlock()
	ls := make([]string, 0, len(c.caches))
	for n := range c.caches {
		ls = append(ls, n)
	}
	return ls
}

func (c *cache[T]) Create(ctx context.Context, cfg *CacheInstanceConfig) error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.cfg.MaxCaches > 0 && len(c.caches) == c.cfg.MaxCaches {
		return fmt.Errorf("failed to create cache %q: max caches reached: %d", cfg.Name, c.cfg.MaxCaches)
	}
	if _, ok := c.caches[cfg.Name]; ok {
		return fmt.Errorf("cache %q already exists", cfg.Name)
	}

	ci, err := createCacheInstance(ctx, cfg, c.bFn)
	if err != nil {
		return err
	}
	c.caches[cfg.Name] = ci
	return nil
}

func (c *cache[T]) GetDetails(ctx context.Context, name string) (*CacheInstanceConfig, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	ci, ok := c.caches[name]
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	return ci.cfg, nil
}

func (c *cache[T]) Delete(ctx context.Context, name string) error {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	// delete candidate
	if cname != "" {
		return ci.deleteCandidate(ctx, cname)
	}
	// delete cache
	err := ci.delete(ctx, name)
	if err != nil {
		return err
	}
	c.m.Lock()
	delete(c.caches, name)
	c.m.Unlock()
	return nil
}

func (c *cache[T]) Clone(ctx context.Context, name, cname string) (string, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return "", fmt.Errorf("cache %q does not exist", name)
	}
	_, ok = c.getCacheInstance(ctx, cname)
	if ok {
		return "", fmt.Errorf("cache %q already exists", name)
	}
	clone, err := ci.clone(ctx, cname)
	if err != nil {
		return "", err
	}
	c.m.Lock()
	c.caches[cname] = clone
	c.m.Unlock()
	return cname, nil
}

func (c *cache[T]) CreateCandidate(ctx context.Context, name, cname string) (string, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return "", fmt.Errorf("cache %q does not exist", name)
	}
	err := ci.createCandidate(ctx, cname)
	return cname, err
}

func (c *cache[T]) WriteValue(ctx context.Context, name string, store Store, p []string, v T) error {
	var cname string
	name, cname = splitCacheName(name)
	switch store {
	case StoreConfig:
	case StoreState:
		if cname != "" {
			return fmt.Errorf("state store does not have candidates")
		}
	}
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	return ci.writeValue(ctx, cname, store, p, v)
}

func (c *cache[T]) ReadValue(ctx context.Context, name string, store Store, p []string) ([]*Entry[T], error) {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}

	return ci.readValue(ctx, cname, store, p)
}

func (c *cache[T]) ReadValueCh(ctx context.Context, name string, store Store, p []string) (chan *Entry[T], error) {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}

	return ci.readValueCh(ctx, cname, store, p)
}

func (c *cache[T]) DeleteValue(ctx context.Context, name string, store Store, p []string) error {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}

	return ci.deleteValue(ctx, cname, store, p)
}

func (c *cache[T]) Diff(ctx context.Context, name, candidate string) ([][]string, []*Entry[T], error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, nil, fmt.Errorf("cache %q does not exist", name)
	}
	return ci.diff(candidate)
}

func (c *cache[T]) Discard(ctx context.Context, name, candidate string) error {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	ci.discard(candidate)
	return nil
}

func (c *cache[T]) Close() error {
	c.m.RLock()
	defer c.m.RUnlock()
	for _, ci := range c.caches {
		ci.close()
	}
	return nil
}

func (c *cache[T]) getCacheInstance(ctx context.Context, name string) (*cacheInstance[T], bool) {
	c.m.RLock()
	defer c.m.RUnlock()
	ci, ok := c.caches[name]
	return ci, ok
}

func splitCacheName(name string) (string, string) {
	name = strings.Trim(name, "/")
	var cname string
	if i := strings.Index(name, "/"); i > 0 {
		cname = name[i+1:]
		name = name[:i]
	}
	return name, cname
}
