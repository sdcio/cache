package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/iptecharch/cache/store"
	"github.com/iptecharch/schema-server/datastore/ctree"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const (
	configBucketName = "config"
	stateBucketName  = "state"
)

var defaultBuckets = []string{configBucketName, stateBucketName}

type cacheInstance[T proto.Message] struct {
	cfg *CacheInstanceConfig

	config *ctree.Tree
	state  *ctree.Tree

	m          *sync.RWMutex
	candidates map[string]*candidate[T]
	store      store.Store[T]
	bFn        func() T
}

type CacheInstanceConfig struct {
	Name      string
	StoreType string
	Ephemeral bool
	Cached    bool
	Dir       string
}

func createCacheInstance[T proto.Message](ctx context.Context, cfg *CacheInstanceConfig, bfn func() T) (*cacheInstance[T], error) {
	ci := newCacheInstance(cfg, bfn)
	storeConfig := map[string]any{
		"cached": cfg.Cached,
	}
	err := ci.store.CreateCache(ctx, cfg.Name, storeConfig, defaultBuckets...)
	return ci, err
}

func newCacheInstance[T proto.Message](cfg *CacheInstanceConfig, bfn func() T) *cacheInstance[T] {
	ci := &cacheInstance[T]{
		cfg:        cfg,
		config:     &ctree.Tree{},
		state:      &ctree.Tree{},
		m:          &sync.RWMutex{},
		candidates: map[string]*candidate[T]{},
		bFn:        bfn,
	}
	ci.store = store.New[T](cfg.StoreType, cfg.Dir)
	return ci
}

type candidate[T proto.Message] struct {
	updates *ctree.Tree

	m       *sync.RWMutex
	deletes map[string]struct{}
}

func newCandidate[T proto.Message]() *candidate[T] {
	return &candidate[T]{
		updates: &ctree.Tree{},
		m:       new(sync.RWMutex),
		deletes: make(map[string]struct{}),
	}
}

func (ci *cacheInstance[T]) initFromStore(ctx context.Context, wg *sync.WaitGroup) error {
	err := ci.store.LoadCache(ctx, ci.cfg.Name)
	if err != nil {
		return err
	}
	mcfg, err := ci.store.GetCacheConfig(ctx, ci.cfg.Name)
	if err != nil {
		return err
	}
	log.Infof("got cache %q with config: %+v\n", ci.cfg.Name, mcfg)

	var ok bool
	ci.cfg.Cached, ok = mcfg["cached"].(bool)
	if !ok {
		return fmt.Errorf("cache %q not loaded: invalid config: %#v", ci.cfg.Name, mcfg)
	}

	// if it's supposed to be cached,
	// load KVs into the tree
	if ci.cfg.Cached {
		wg.Add(2)
		go func() {
			defer wg.Done()
			n, err := ci.loadAll(ctx, ci.cfg.Name, "config")
			if err != nil {
				log.Errorf("Failed to load cache %s config bucket", ci.cfg.Name)
				return
			}
			log.Infof("cache=%s, bucket=%s: loaded %d KV", ci.cfg.Name, "config", n)
		}()
		go func() {
			defer wg.Done()
			n, err := ci.loadAll(ctx, ci.cfg.Name, "state")
			if err != nil {
				log.Errorf("Failed to load cache %s state bucket", ci.cfg.Name)
				return
			}
			log.Debugf("cache=%s, bucket=%s: loaded %d KV", ci.cfg.Name, "state", n)
		}()
	}
	return nil
}

func (ci *cacheInstance[T]) writeValue(ctx context.Context, cname string, store Store, p []string, v T) error {
	if cname == "" {
		var err error
		bucketName := configBucketName
		switch store {
		case StoreConfig:
			if ci.cfg.Cached || ci.cfg.Ephemeral {
				err = ci.config.Add(p, v)
			}
		case StoreState:
			bucketName = stateBucketName
			if ci.cfg.Cached || ci.cfg.Ephemeral {
				err = ci.state.Add(p, v)
			}
		}
		if err != nil {
			return err
		}
		return ci.store.WriteValue(ctx, ci.cfg.Name, bucketName, []byte(strings.Join(p, ",")), v)
	}
	// write to candidate
	ci.m.Lock()
	defer ci.m.Unlock()
	cand, ok := ci.candidates[cname]
	if !ok {
		return fmt.Errorf("no such candidate %q in cache %q", ci.cfg.Name, cname)
	}

	err := cand.updates.Add(p, v)
	if err != nil {
		return err
	}
	// remove the added path from deletes in case it was deleted before
	cand.m.Lock()
	defer cand.m.Unlock()
	delete(cand.deletes, strings.Join(p, ","))
	return nil
}

func (ci *cacheInstance[T]) readValue(ctx context.Context, cname string, store Store, p []string) ([]*Entry[T], error) {
	var f *candidate[T]
	var ok bool
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		f, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
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

func (ci *cacheInstance[T]) readValueCh(ctx context.Context, cname string, store Store, p []string) (chan *Entry[T], error) {
	var f *candidate[T]
	var ok bool
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		f, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
		}
	}
	rsCh := make(chan *Entry[T])
	go func() {
		defer close(rsCh)
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
	}()
	return rsCh, nil
}

func (ci *cacheInstance[T]) readFromStore(ctx context.Context, prefix []byte) ([]*Entry[T], error) {
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, "", prefix)
	if err != nil {
		return nil, err
	}
	es := make([]*Entry[T], 0)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				return es, nil
			}
			pv := ci.bFn()
			err = proto.Unmarshal(v.V, pv)
			if err != nil {
				return nil, err
			}
			entry := &Entry[T]{
				P: strings.Split(string(v.K), ","),
				V: pv,
			}
			es = append(es, entry)
		}
	}
}

func (ci *cacheInstance[T]) readFromStoreCh(ctx context.Context, prefix []byte, kvCh chan *Entry[T]) error {
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, "", prefix)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				return nil
			}
			pv := ci.bFn()
			err = proto.Unmarshal(v.V, pv)
			if err != nil {
				return err
			}
			entry := &Entry[T]{
				P: strings.Split(string(v.K), ","),
				V: pv,
			}
			kvCh <- entry
		}
	}
}

func (ci *cacheInstance[T]) deleteValue(ctx context.Context, cname string, store Store, p []string) error {
	if cname == "" {
		bucket := "config"
		if store == StoreState {
			bucket = "state"
		}
		err := ci.store.DeleteValue(ctx, ci.cfg.Name, bucket, []byte(strings.Join(p, ",")))
		if err != nil {
			return err
		}
		ci.config.Delete(p)
		return nil
	}

	f, err := ci.getCandidate(cname)
	if err != nil {
		return err
	}
	f.updates.Delete(p)

	f.m.Lock()
	defer f.m.Unlock()
	f.deletes[strings.Join(p, ",")] = struct{}{}
	return nil
}

func (ci *cacheInstance[T]) loadAll(ctx context.Context, name, bucket string) (int64, error) {
	kvCh, err := ci.store.GetAll(ctx, name, bucket)
	if err != nil {
		return 0, err
	}
	count := int64(0)
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case kv, ok := <-kvCh:
			if !ok {
				return count, nil
			}
			pv := ci.bFn()
			err = proto.Unmarshal(kv.V, pv)
			if err != nil {
				return 0, err
			}
			log.Debugf("cache=%q, bucket=%q, key=%q, val=%v", name, bucket, string(kv.K), pv)
			switch bucket {
			case configBucketName:
				err = ci.config.Add(strings.Split(string(kv.K), ","), pv)
			case stateBucketName:
				err = ci.state.Add(strings.Split(string(kv.K), ","), pv)
			}
			if err != nil {
				return 0, err
			}
			count++
		}
	}
}

func (ci *cacheInstance[T]) close() {
	if ci.store != nil {
		ci.store.Close()
	}
}

func (ci *cacheInstance[T]) getCandidate(name string) (*candidate[T], error) {
	ci.m.Lock()
	defer ci.m.Unlock()
	f, ok := ci.candidates[name]
	if !ok {
		return nil, fmt.Errorf("candidate %q does not exist in cache %q", name, ci.cfg.Name)
	}
	return f, nil
}

func (ci *cacheInstance[T]) delete(ctx context.Context, name string) error {
	return ci.store.DeleteCache(ctx, name)
}

func (ci *cacheInstance[T]) deleteCandidate(ctx context.Context, cname string) error {
	ci.m.Lock()
	defer ci.m.Unlock()
	delete(ci.candidates, cname)
	return nil
}

func (ci *cacheInstance[T]) clone(ctx context.Context, cname string) (*cacheInstance[T], error) {
	cCfg, err := ci.config.Clone()
	if err != nil {
		return nil, err
	}
	cState, err := ci.state.Clone()
	if err != nil {
		return nil, err
	}
	clone, err := createCacheInstance(ctx,
		&CacheInstanceConfig{
			Name:      cname,
			StoreType: ci.cfg.StoreType,
			Ephemeral: ci.cfg.Ephemeral,
			Cached:    ci.cfg.Cached,
			Dir:       ci.cfg.Dir,
		}, ci.bFn)
	if err != nil {
		return nil, err
	}
	clone.config = cCfg
	clone.state = cState

	err = ci.store.Clone(ctx, ci.cfg.Name, cname)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

func (ci *cacheInstance[T]) createCandidate(ctx context.Context, cname string) error {
	ci.m.Lock()
	defer ci.m.Unlock()
	_, ok := ci.candidates[cname]
	if ok {
		return fmt.Errorf("candidate %q in cache %q already exists", cname, ci.cfg.Name)
	}

	ci.candidates[cname] = newCandidate[T]()
	return nil
}

func (ci *cacheInstance[T]) discard(candidate string) error {
	cand, err := ci.getCandidate(candidate)
	if err != nil {
		return err
	}
	cand.m.Lock()
	defer cand.m.Unlock()
	cand.deletes = make(map[string]struct{})
	cand.updates = &ctree.Tree{}
	return nil
}

func (ci *cacheInstance[T]) diff(candidate string) ([][]string, []*Entry[T], error) {
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
	return nil, nil, fmt.Errorf("candidate %q does not exist for cache %q", candidate, ci.cfg.Name)
}
