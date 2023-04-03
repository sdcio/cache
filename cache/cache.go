package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/iptecharch/schema-server/datastore/ctree"
	log "github.com/sirupsen/logrus"
)

type Store int8

const (
	StoreConfig Store = 0
	StoreState  Store = 1
)

type Cache[T any] interface {
	// List cache instances
	List(ctx context.Context) []string
	// Create a new cache instance
	Create(ctx context.Context, name string) error
	// Delete a cache instance or a candidate in a cache instance.
	// the name should be in the format $cache/$candidate to delete a candidate
	Delete(ctx context.Context, name string) error
	// Clone a cache instance
	Clone(ctx context.Context, name, cname string) (string, error)
	// Create a candidate for an existing cache instance
	CreateCandidate(ctx context.Context, name, candidate string) (string, error)
	// Candidates returns the list of candidates created for a cache instance
	Candidates(ctx context.Context, name string) ([]string, error)
	//
	// WriteValue writes a new value in a cache instance.
	// the value can be written into 2 different stores, CONFIG or STATE
	WriteValue(ctx context.Context, name string, store Store, p []string, v T) error
	// ReadValue reads a value from a cache instance.
	ReadValue(ctx context.Context, name string, store Store, p []string) ([]*Entry[T], error)
	// ReadValue reads a value from a cache instance.
	ReadValueCh(ctx context.Context, name string, store Store, p []string) (chan *Entry[T], error)
	// DeleteValue deletes a value from a cache instance.
	DeleteValue(ctx context.Context, name string, store Store, p []string) error
	// GetChanges returns the changes made to a candidate
	GetChanges(ctx context.Context, name, candidate string) ([][]string, []*Entry[T], error)
	// Discard drops the changes made to a candidate
	Discard(ctx context.Context, name, candidate string) error
}

type Entry[T any] struct {
	P []string
	V T
}

func New[T any]() Cache[T] {
	return &cache[T]{
		m:      &sync.RWMutex{},
		caches: map[string]*cacheInstance[T]{},
	}
}

type cache[T any] struct {
	m      *sync.RWMutex
	caches map[string]*cacheInstance[T]
}

type cacheInstance[T any] struct {
	config     *ctree.Tree
	state      *ctree.Tree
	m          *sync.RWMutex
	candidates map[string]*candidate[T]
}

func newCacheInstance[T any]() *cacheInstance[T] {
	return &cacheInstance[T]{
		config:     &ctree.Tree{},
		state:      &ctree.Tree{},
		m:          &sync.RWMutex{},
		candidates: map[string]*candidate[T]{},
	}
}

type candidate[T any] struct {
	tr *ctree.Tree

	m       *sync.RWMutex
	deletes map[string]struct{}
}

func newCandidate[T any]() *candidate[T] {
	return &candidate[T]{
		tr:      &ctree.Tree{},
		m:       new(sync.RWMutex),
		deletes: make(map[string]struct{}),
	}
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

func (c *cache[T]) Create(ctx context.Context, name string) error {
	c.m.Lock()
	defer c.m.Unlock()
	if _, ok := c.caches[name]; ok {
		return fmt.Errorf("cache %q already exists", name)
	}
	c.caches[name] = newCacheInstance[T]()
	return nil
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
		return nil
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
	clone := &cacheInstance[T]{
		config:     cCfg,
		state:      cState,
		m:          &sync.RWMutex{},
		candidates: make(map[string]*candidate[T]),
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

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	if cname == "" {
		return ci.config.Add(p, v)
	}
	ci.m.Lock()
	defer ci.m.Unlock()
	f, ok := ci.candidates[cname]
	if !ok {
		return fmt.Errorf("no such candidate %q in cache %q", name, cname)
	}

	err := f.tr.Add(p, v)
	if err != nil {
		return err
	}
	// remove the added path from deletes in case it was deleted before
	delete(f.deletes, strings.Join(p, ","))
	return nil
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
		err = f.tr.Query(p,
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
		})

	return es, err
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
			err = f.tr.Query(p,
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
		ci.config.Delete(p)
		return nil
	}
	ci.m.Lock()
	defer ci.m.Unlock()
	f, ok := ci.candidates[cname]
	if !ok {
		return fmt.Errorf("candidate %q does not exist in cache %q", cname, name)
	}
	f.tr.Delete(p)

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
		err := cand.tr.Query([]string{},
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
		cand.tr = &ctree.Tree{}
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
