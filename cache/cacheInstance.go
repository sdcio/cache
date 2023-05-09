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

func createCacheInstance[T proto.Message](ctx context.Context, cfg *CacheInstanceConfig, bfn func() T, storage store.Store[T]) (*cacheInstance[T], error) {
	ci := newCacheInstance(cfg, bfn, storage)
	storeConfig := map[string]any{
		"cached": cfg.Cached,
	}
	err := ci.store.CreateCache(ctx, cfg.Name, storeConfig, defaultBuckets...)
	return ci, err
}

func newCacheInstance[T proto.Message](cfg *CacheInstanceConfig, bfn func() T, storage store.Store[T]) *cacheInstance[T] {
	return &cacheInstance[T]{
		cfg:        cfg,
		config:     &ctree.Tree{},
		state:      &ctree.Tree{},
		m:          &sync.RWMutex{},
		candidates: map[string]*candidate[T]{},
		bFn:        bfn,
		store:      storage,
	}
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
		}, ci.bFn,
		ci.store,
	)
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

func (ci *cacheInstance[T]) stats(ctx context.Context) (*InstanceStats, error) {
	ss, err := ci.store.Stats(ctx, ci.cfg.Name)
	if err != nil {
		return nil, err
	}

	is := &InstanceStats{
		Name:     ci.cfg.Name,
		KeyCount: ss.KeysPerBucket,
	}
	return is, nil
}
