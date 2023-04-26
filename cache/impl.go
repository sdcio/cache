package cache

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/iptecharch/cache/config"
	"github.com/iptecharch/schema-server/datastore/ctree"
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
		ci := initCacheInstance(ctx, ccfg, c.bFn)
		err = ci.store.LoadCache(ctx, cacheName)
		if err != nil {
			return err
		}
		mcfg, err := ci.store.GetCacheConfig(ctx, cacheName)
		if err != nil {
			return err
		}
		log.Infof("got cache %q with config: %+v\n", cacheName, mcfg)

		var ok bool
		ccfg.Cached, ok = mcfg["cached"].(bool)
		if !ok {
			log.Errorf("cache %q not loaded: invalid config: %#v", cacheName, mcfg)
			continue
		}
		//
		ci.m.Lock()
		c.caches[cacheName] = ci
		ci.m.Unlock()
		// if it's supposed to be cached,
		// load KVs into the tree
		if ccfg.Cached {
			wg.Add(2)
			go func() {
				defer wg.Done()
				n, err := ci.loadAll(ctx, cacheName, "config")
				if err != nil {
					log.Errorf("Failed to load cache %s config bucket", cacheName)
					return
				}
				log.Infof("cache=%s, bucket=%s: loaded %d KV", cacheName, "config", n)
			}()
			go func() {
				defer wg.Done()
				n, err := ci.loadAll(ctx, cacheName, "state")
				if err != nil {
					log.Errorf("Failed to load cache %s state bucket", cacheName)
					return
				}
				log.Debugf("cache=%s, bucket=%s: loaded %d KV", cacheName, "state", n)
			}()
		}
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

	ci, err := newCacheInstance(ctx, cfg, c.bFn)
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
	if cname == "" {
		// delete cache
		c.m.Lock()
		defer c.m.Unlock()
		delete(c.caches, name)
		err := ci.store.DeleteCache(ctx, name)
		return err
	}
	// delete candidate
	ci.m.Lock()
	defer ci.m.Unlock()
	delete(ci.candidates, cname)
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
	cCfg, err := ci.config.Clone()
	if err != nil {
		return "", err
	}
	cState, err := ci.state.Clone()
	if err != nil {
		return "", err
	}
	clone, err := newCacheInstance(ctx,
		&CacheInstanceConfig{
			Name:      cname,
			StoreType: ci.cfg.StoreType,
			Ephemeral: ci.cfg.Ephemeral,
			Cached:    ci.cfg.Cached,
			Dir:       ci.cfg.Dir,
		}, c.bFn)
	if err != nil {
		return "", err
	}
	clone.config = cCfg
	clone.state = cState

	err = ci.store.Clone(ctx, name, cname)
	if err != nil {
		return "", err
	}
	c.m.Lock()
	defer c.m.Unlock()
	c.caches[cname] = clone
	return cname, nil
}

func (c *cache[T]) CreateCandidate(ctx context.Context, name, cname string) (string, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return "", fmt.Errorf("cache %q does not exist", name)
	}
	ci.m.Lock()
	defer ci.m.Unlock()
	_, ok = ci.candidates[cname]
	if ok {
		return "", fmt.Errorf("candidate %q in cache %q already exists", cname, name)
	}

	ci.candidates[cname] = newCandidate[T]()
	return cname, nil
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

	var f *candidate[T]
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		f, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, name)
		}
	}
	var err error
	es := make([]*Entry[T], 0)
	found := make(map[string]struct{})
	// check if the path exists in the candidate
	if cname != "" {
		err = f.updates.Query(p,
			func(path []string, _ *ctree.Leaf, val interface{}) error {
				vt := val.(T)
				e := &Entry[T]{
					P: path,
					V: vt,
				}
				es = append(es, e)
				found[strings.Join(path, ",")] = struct{}{}
				return nil
			})
		if err != nil {
			return nil, err
		}
	}

	if ci.cfg.Cached || ci.cfg.Ephemeral {
		// read from main cache
		err = ci.config.Query(p,
			func(path []string, _ *ctree.Leaf, val interface{}) error {
				if cname != "" {
					// check if the path has been deleted
					f.m.RLock()
					_, ok := f.deletes[strings.Join(path, ",")]
					f.m.RUnlock()
					if ok {
						return nil
					}
					if _, ok := found[strings.Join(path, ",")]; ok {
						// path read from candidate, exit
						return nil
					}
				}
				// append value from main cache
				vt := val.(T)
				e := &Entry[T]{
					P: path,
					V: vt,
				}
				es = append(es, e)
				return nil
			},
		)
		return es, err
	}
	if !ci.cfg.Cached {
		return ci.readFromStore(ctx, []byte(strings.Join(p, ",")))
	}
	return nil, nil
}

func (c *cache[T]) ReadValueCh(ctx context.Context, name string, store Store, p []string) (chan *Entry[T], error) {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	var f *candidate[T]
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		f, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, name)
		}
	}
	rsCh := make(chan *Entry[T])
	go func() {
		var err error
		// es := make([]*Entry[T], 0)
		found := make(map[string]struct{})
		// check if the path exists in the candidate
		if cname != "" {
			err = f.updates.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					vt := val.(T)
					e := &Entry[T]{
						P: path,
						V: vt,
					}
					rsCh <- e
					found[strings.Join(path, ",")] = struct{}{}
					return nil
				})
			if err != nil {
				log.Errorf("failed to run query: %v", err)
			}
		}
		if ci.cfg.Cached || ci.cfg.Ephemeral {
			// read from main cache
			err = ci.config.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					if cname != "" {
						// check if the path has been deleted
						f.m.RLock()
						_, ok := f.deletes[strings.Join(path, ",")]
						f.m.RUnlock()
						if ok {
							return nil
						}
						if _, ok := found[strings.Join(path, ",")]; ok {
							// path read from candidate, exit
							return nil
						}
					}
					// append value from main cache
					vt := val.(T)
					e := &Entry[T]{
						P: path,
						V: vt,
					}
					rsCh <- e
					return nil
				})
			if err != nil {
				log.Errorf("failed to run query: %v", err)
			}
		}
		if !ci.cfg.Cached {
			err = ci.readFromStoreCh(ctx, []byte(strings.Join(p, ",")), rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
		}
		close(rsCh)
	}()
	return rsCh, nil
}

func (c *cache[T]) DeleteValue(ctx context.Context, name string, store Store, p []string) error {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	if cname == "" {
		bucket := "config"
		if store == StoreState {
			bucket = "state"
		}
		err := ci.store.DeleteValue(ctx, name, bucket, []byte(strings.Join(p, ",")))
		if err != nil {
			return err
		}
		ci.config.Delete(p)
		return nil
	}
	ci.m.Lock()
	defer ci.m.Unlock()
	f, ok := ci.candidates[cname]
	if !ok {
		return fmt.Errorf("candidate %q does not exist in cache %q", cname, name)
	}
	f.updates.Delete(p)

	f.m.Lock()
	defer f.m.Unlock()
	f.deletes[strings.Join(p, ",")] = struct{}{}
	return nil
}

func (c *cache[T]) GetChanges(ctx context.Context, name, candidate string) ([][]string, []*Entry[T], error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, nil, fmt.Errorf("cache %q does not exist", name)
	}
	ci.m.RLock()
	defer ci.m.RUnlock()
	if cand, ok := ci.candidates[candidate]; ok {
		dels := make([][]string, 0, len(cand.deletes))
		es := make([]*Entry[T], 0)
		// deletes
		for p := range cand.deletes {
			dels = append(dels, strings.Split(p, ","))
		}
		// updates
		err := cand.updates.Query([]string{},
			func(path []string, _ *ctree.Leaf, val interface{}) error {
				vt := val.(T)
				e := &Entry[T]{
					P: path,
					V: vt,
				}
				es = append(es, e)
				return nil
			})
		if err != nil {
			return nil, nil, err
		}
		return dels, es, nil
	}
	return nil, nil, fmt.Errorf("candidate %q does not exist for cache %q", candidate, name)
}

func (c *cache[T]) Discard(ctx context.Context, name, candidate string) error {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	ci.m.RLock()
	defer ci.m.RUnlock()
	if cand, ok := ci.candidates[candidate]; ok {
		cand.deletes = make(map[string]struct{})
		cand.updates = &ctree.Tree{}
	}
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
