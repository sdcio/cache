package cache

import (
	"context"
	"fmt"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/cache/pkg/ctree"
	"github.com/iptecharch/cache/pkg/store"
)

const (
	configBucketName   = "config"
	stateBucketName    = "state"
	intendedBucketName = "intended"

	// max int32
	defaultWritePriority = 0x7FFFFFFF
)

var delimStr = ","
var delimBytes = []byte(delimStr)

var defaultBuckets = []string{configBucketName, stateBucketName}

type cacheInstance struct {
	cfg *CacheInstanceConfig

	m          *sync.RWMutex
	candidates map[string]*candidate
	store      store.Store
}

type CacheInstanceConfig struct {
	Name      string
	StoreType string
	Dir       string
}

func createCacheInstance(ctx context.Context, cfg *CacheInstanceConfig, storage store.Store) (*cacheInstance, error) {
	ci := newCacheInstance(cfg, storage)
	storeConfig := map[string]any{} // metadata about the cache instance
	err := ci.store.CreateCache(ctx, cfg.Name, storeConfig, defaultBuckets...)
	return ci, err
}

func newCacheInstance(cfg *CacheInstanceConfig, storage store.Store) *cacheInstance {
	return &cacheInstance{
		cfg:        cfg,
		m:          new(sync.RWMutex),
		candidates: make(map[string]*candidate),
		store:      storage,
	}
}

type candidate struct {
	owner    string
	priority int32
	updates  *ctree.Tree

	m       *sync.RWMutex
	deletes map[string]struct{}
}

func newCandidate(owner string, prio int32) *candidate {
	return &candidate{
		owner:    owner,
		priority: prio,
		updates:  &ctree.Tree{},
		m:        new(sync.RWMutex),
		deletes:  make(map[string]struct{}),
	}
}

func (ci *cacheInstance) initFromStore(ctx context.Context, wg *sync.WaitGroup) error {
	err := ci.store.LoadCache(ctx, ci.cfg.Name)
	if err != nil {
		return err
	}
	mcfg, err := ci.store.GetMeta(ctx, ci.cfg.Name)
	if err != nil {
		return err
	}
	log.Infof("got cache %q with config: %+v\n", ci.cfg.Name, mcfg)

	// var ok bool
	// ci.cfg.Cached, ok = mcfg["cached"].(bool)
	// if !ok {
	// 	return fmt.Errorf("cache %q not loaded: invalid config: %#v", ci.cfg.Name, mcfg)
	// }

	// // if it's supposed to be cached,
	// // load KVs into the tree
	// if ci.cfg.Cached {
	// 	wg.Add(2)
	// 	go func() {
	// 		defer wg.Done()
	// 		n, err := ci.loadAll(ctx, ci.cfg.Name, "config")
	// 		if err != nil {
	// 			log.Errorf("Failed to load cache %s config bucket", ci.cfg.Name)
	// 			return
	// 		}
	// 		log.Infof("cache=%s, bucket=%s: loaded %d KV", ci.cfg.Name, "config", n)
	// 	}()
	// 	go func() {
	// 		defer wg.Done()
	// 		n, err := ci.loadAll(ctx, ci.cfg.Name, "state")
	// 		if err != nil {
	// 			log.Errorf("Failed to load cache %s state bucket", ci.cfg.Name)
	// 			return
	// 		}
	// 		log.Debugf("cache=%s, bucket=%s: loaded %d KV", ci.cfg.Name, "state", n)
	// 	}()
	// }
	return nil
}

// func (ci *cacheInstance) loadAll(ctx context.Context, name, bucket string) (int64, error) {
// 	kvCh, err := ci.store.GetAll(ctx, name, bucket)
// 	if err != nil {
// 		return 0, err
// 	}
// 	count := int64(0)
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			return 0, ctx.Err()
// 		case kv, ok := <-kvCh:
// 			if !ok {
// 				return count, nil
// 			}
// 			pv := ci.bFn()
// 			err = proto.Unmarshal(kv.V, pv)
// 			if err != nil {
// 				return 0, err
// 			}
// 			log.Debugf("cache=%q, bucket=%q, key=%q, val=%v", name, bucket, string(kv.K), pv)
// 			switch bucket {
// 			case configBucketName:
// 				// err = ci.config.Add(strings.Split(string(kv.K), ","), pv)
// 			case stateBucketName:
// 				// err = ci.state.Add(strings.Split(string(kv.K), ","), pv)
// 			}
// 			if err != nil {
// 				return 0, err
// 			}
// 			count++
// 		}
// 	}
// }

func (ci *cacheInstance) close() {
	if ci.store != nil {
		ci.store.Close()
	}
}

func (ci *cacheInstance) getCandidate(name string) (*candidate, error) {
	ci.m.Lock()
	defer ci.m.Unlock()
	f, ok := ci.candidates[name]
	if !ok {
		return nil, fmt.Errorf("candidate %q does not exist in cache %q", name, ci.cfg.Name)
	}
	return f, nil
}

func (ci *cacheInstance) delete(ctx context.Context, name string) error {
	return ci.store.DeleteCache(ctx, name)
}

func (ci *cacheInstance) deleteCandidate(ctx context.Context, cname string) error {
	ci.m.Lock()
	defer ci.m.Unlock()
	delete(ci.candidates, cname)
	return nil
}

func (ci *cacheInstance) clone(ctx context.Context, cname string) (*cacheInstance, error) {
	clone, err := createCacheInstance(ctx,
		&CacheInstanceConfig{
			Name:      cname,
			StoreType: ci.cfg.StoreType,
			// Ephemeral: ci.cfg.Ephemeral,
			// Cached:    ci.cfg.Cached,
			Dir: ci.cfg.Dir,
		},
		ci.store,
	)
	if err != nil {
		return nil, err
	}

	err = ci.store.Clone(ctx, ci.cfg.Name, cname)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

func (ci *cacheInstance) createCandidate(ctx context.Context, cname, owner string, p int32) error {
	ci.m.Lock()
	defer ci.m.Unlock()
	_, ok := ci.candidates[cname]
	if ok {
		return fmt.Errorf("candidate %q in cache %q already exists", cname, ci.cfg.Name)
	}

	ci.candidates[cname] = newCandidate(owner, p)
	return nil
}

func (ci *cacheInstance) discard(candidate string) error {
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

func (ci *cacheInstance) diff(ctx context.Context, candidate string) ([][]string, []*Entry, error) {
	ci.m.RLock()
	defer ci.m.RUnlock()
	cand, ok := ci.candidates[candidate]
	if !ok {
		return nil, nil, fmt.Errorf("candidate %q does not exist for cache %q", candidate, ci.cfg.Name)
	}
	dels := make([][]string, 0, len(cand.deletes))
	es := make([]*Entry, 0)
	// deletes
	for p := range cand.deletes {
		vch, err := ci.store.GetPrefix(ctx, ci.cfg.Name, intendedBucketName, []byte(p), nil)
		if err != nil {
			return nil, nil, err
		}
	CH_LOOP:
		for {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case v, ok := <-vch:
				if !ok {
					break CH_LOOP
				}
				e, err := kvToEntry(v, intendedBucketName)
				if err != nil {
					return nil, nil, err
				}
				// if the candidate has a higher
				// priority than the current
				// KV in the intended store,
				// then the delete applies.
				// TODO: do we send delete southbound ?
				if e.Priority < cand.priority {
					dels = append(dels, strings.Split(p, delimStr))
				}
			}
		}
	}
	// updates
	err := cand.updates.Query([]string{},
		func(path []string, _ *ctree.Leaf, val interface{}) error {
			vt := val.([]byte)
			vch, err := ci.store.GetPrefix(ctx, ci.cfg.Name, intendedBucketName, []byte(strings.Join(path, delimStr)), nil)
			if err != nil {
				return err
			}
		CH_LOOP:
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case v, ok := <-vch:
					if !ok {
						break CH_LOOP
					}
					e, err := kvToEntry(v, intendedBucketName)
					if err != nil {
						return err
					}
					if e.Priority < cand.priority {
						e.V = vt // override the value
						es = append(es, e)
					}
				}
			}
			return nil
		})
	if err != nil {
		return nil, nil, err
	}
	return dels, es, nil
}

func (ci *cacheInstance) stats(ctx context.Context) (*InstanceStats, error) {
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
