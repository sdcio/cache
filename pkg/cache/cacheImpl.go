package cache

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/cache/pkg/config"
	"github.com/iptecharch/cache/pkg/store"
)

type cache struct {
	cfg *config.CacheConfig

	m      *sync.RWMutex
	caches map[string]*cacheInstance
	store  store.Store
}

func (c *cache) Init(ctx context.Context) error {
	log.Info("initializing cache")
	if _, err := os.Stat(c.cfg.Dir); os.IsNotExist(err) {
		// caches directory does not exist, create it
		log.Debugf("caches directory %q does not exist, create it", c.cfg.Dir)
		err = os.MkdirAll(c.cfg.Dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	log.Debugf("creating a store type %s under dir %q", c.cfg.StoreType, c.cfg.Dir)

	var err error
	c.store, err = store.New(c.cfg.StoreType, c.cfg.Dir)
	if err != nil {
		return err
	}
	log.Info("loading caches...")
	caches, err := c.store.ListCaches(ctx)
	if err != nil {
		return err
	}

	c.m.Lock()
	defer c.m.Unlock()
	for _, cacheName := range caches {
		ccfg := &CacheInstanceConfig{
			Name:      cacheName,
			StoreType: c.cfg.StoreType,
			Dir:       c.cfg.Dir,
		}
		ci := newCacheInstance(ccfg, c.store)
		ci.pruneIndex, err = c.store.GetPruneIndex(ctx, cacheName)
		if err != nil {
			return err
		}
		log.Infof("cache %s pruneIndex=%d", cacheName, ci.pruneIndex)
		c.caches[cacheName] = ci
	}
	log.Infof("loaded %d caches", len(caches))
	return nil
}

func (c *cache) Candidates(ctx context.Context, name string) ([]*CandidateDetails, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	ci.m.RLock()
	defer ci.m.RUnlock()
	rs := make([]*CandidateDetails, 0, len(ci.candidates))
	for n, cand := range ci.candidates {
		rs = append(rs, &CandidateDetails{
			CacheName:     name,
			CandidateName: n,
			Owner:         cand.owner,
			Priority:      cand.priority,
		})
	}
	sort.Slice(rs, func(i, j int) bool {
		return rs[i].CandidateName < rs[j].CandidateName
	})
	return rs, nil
}

func (c *cache) List(ctx context.Context) []string {
	c.m.RLock()
	defer c.m.RUnlock()
	ls := make([]string, 0, len(c.caches))
	for n := range c.caches {
		ls = append(ls, n)
	}
	return ls
}

func (c *cache) Create(ctx context.Context, cfg *CacheInstanceConfig) error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.cfg.MaxCaches > 0 && len(c.caches) == c.cfg.MaxCaches {
		return fmt.Errorf("failed to create cache %q: max caches reached: %d", cfg.Name, c.cfg.MaxCaches)
	}
	if _, ok := c.caches[cfg.Name]; ok {
		return fmt.Errorf("cache %q already exists", cfg.Name)
	}

	ci, err := createCacheInstance(ctx, cfg, c.store)
	if err != nil {
		return err
	}
	c.caches[cfg.Name] = ci
	return nil
}

func (c *cache) GetDetails(ctx context.Context, name string) (*CacheInstanceConfig, error) {
	c.m.RLock()
	defer c.m.RUnlock()
	ci, ok := c.caches[name]
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	return ci.cfg, nil
}

func (c *cache) Delete(ctx context.Context, name string) error {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	log.Debugf("found cache instance %s", name)
	// delete candidate
	if cname != "" {
		log.Debugf("deleting candidate %s from cache %s", cname, name)
		return ci.deleteCandidate(ctx, cname)
	}
	// delete cache
	log.Debugf("deleting cache %s", name)
	err := ci.delete(ctx, name)
	if err != nil {
		return err
	}
	c.m.Lock()
	delete(c.caches, name)
	c.m.Unlock()
	return nil
}

func (c *cache) Exists(ctx context.Context, name string) bool {
	_, ok := c.getCacheInstance(ctx, name)
	return ok
}

func (c *cache) Clone(ctx context.Context, name, cname string) (string, error) {
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

func (c *cache) CreateCandidate(ctx context.Context, name, cname, owner string, priority int32) (string, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return "", fmt.Errorf("cache %q does not exist", name)
	}
	err := ci.createCandidate(ctx, cname, owner, priority)
	return cname, err
}

func (c *cache) GetCandidate(ctx context.Context, name, cname string) (*CandidateDetails, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	cand, err := ci.getCandidate(cname)
	if err != nil {
		return nil, err
	}

	return &CandidateDetails{
		CacheName:     name,
		CandidateName: cname,
		Owner:         cand.owner,
		Priority:      cand.priority,
	}, err
}

func (c *cache) ReadValue(ctx context.Context, name string, ro *Opts) (chan *Entry, error) {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	return ci.readValueCh(ctx, cname, ro)
}

func (c *cache) ReadValuePeriodic(ctx context.Context, name string, ro *Opts, period time.Duration) (chan *Entry, error) {
	rsCh := make(chan *Entry)
	ticker := time.NewTicker(period)

	go func() {
		defer ticker.Stop()
		defer close(rsCh)
		ch, err := c.ReadValue(ctx, name, ro)
		if err != nil {
			log.Errorf("failed to read value: %v", err)
			return
		}
		for e := range ch {
			rsCh <- e
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ch, err := c.ReadValue(ctx, name, ro)
				if err != nil {
					log.Errorf("failed to read value: %v", err)
					return
				}
				for e := range ch {
					rsCh <- e
				}
			}
		}
	}()

	return rsCh, nil
}

func (c *cache) WriteValue(ctx context.Context, name string, wo *Opts, v []byte) error {
	var cname string
	name, cname = splitCacheName(name)
	if cname != "" {
		switch wo.Store {
		case StoreConfig:
		case StoreState:
			return fmt.Errorf("state store does not have candidates")
		case StoreIntended:
			return fmt.Errorf("intended store does not have candidates")
		}
	}
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	return ci.writeValue(ctx, cname, wo, v)
}

// func (c *cache) WriteBytesValue(ctx context.Context, name string, wo *Opts, vb []byte) error {
// 	var cname string
// 	name, cname = splitCacheName(name)
// 	switch wo.Store {
// 	case StoreConfig:
// 	case StoreState:
// 		if cname != "" {
// 			return fmt.Errorf("state store does not have candidates")
// 		}
// 	case StoreIntended:
// 		if cname != "" {
// 			return fmt.Errorf("intended store does not have candidates")
// 		}
// 	}
// 	ci, ok := c.getCacheInstance(ctx, name)
// 	if !ok {
// 		return fmt.Errorf("cache %q does not exist", name)
// 	}
// 	return ci.writeBytesValue(ctx, cname, wo, vb)
// }

func (c *cache) DeleteValue(ctx context.Context, name string, wo *Opts) error {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}

	return ci.deleteValue(ctx, cname, wo)
}

func (c *cache) DeletePrefix(ctx context.Context, name string, wo *Opts) error {
	var cname string
	name, cname = splitCacheName(name)

	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	return ci.deletePrefix(ctx, cname, wo)
}

func (c *cache) Diff(ctx context.Context, name, candidate string) ([][]string, []*Entry, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, nil, fmt.Errorf("cache %q does not exist", name)
	}
	return ci.diff(ctx, candidate)
}

func (c *cache) Discard(ctx context.Context, name, candidate string) error {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	ci.discard(candidate)
	return nil
}

func (c *cache) NumInstances() int {
	c.m.RLock()
	defer c.m.RUnlock()
	return len(c.caches)
}

func (c *cache) Commit(ctx context.Context, name, candidate string) error {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	return ci.commit(ctx, candidate)
}

func (c *cache) Stats(ctx context.Context, name string, withKeysCount bool) (*StatsResponse, error) {
	count := c.NumInstances()
	rsp := &StatsResponse{
		NumInstances: count,
	}
	if !withKeysCount {
		return rsp, nil
	}
	if name == "" {
		rsp.InstanceStats = make(map[string]*InstanceStats, count)
		c.m.RLock()
		defer c.m.RUnlock()
		wg := new(sync.WaitGroup)
		wg.Add(count)
		m := new(sync.Mutex)
		for _, ci := range c.caches {
			go func(ci *cacheInstance) {
				defer wg.Done()
				ss, err := ci.stats(ctx)
				if err != nil {
					log.Errorf("failed to get stats from cache instance %s: %v", ci.cfg.Name, err)
					return
				}
				m.Lock()
				rsp.InstanceStats[ci.cfg.Name] = ss
				m.Unlock()
			}(ci)
		}
		wg.Wait()
		return rsp, nil
	}
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("unknown cache instance %s", name)
	}
	rsp.InstanceStats = make(map[string]*InstanceStats, 1)
	ss, err := ci.stats(ctx)
	if err != nil {
		return nil, err
	}
	rsp.InstanceStats[ci.cfg.Name] = ss
	return rsp, nil
}

func (c *cache) Close() error {
	c.m.RLock()
	defer c.m.RUnlock()
	for _, ci := range c.caches {
		ci.close()
	}
	return nil
}

func (c *cache) Clear(ctx context.Context, name string) error {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	return ci.clear(ctx)
}

func (c *cache) Watch(ctx context.Context, name string, store Store, prefixes [][]string) (chan *Entry, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return nil, fmt.Errorf("cache %q does not exist", name)
	}
	bucket := configBucketName
	switch store {
	case StoreConfig:
	case StoreState:
		bucket = stateBucketName
	case StoreIntended:
		bucket = intendedBucketName
	}
	bPrefixes := make([][]byte, 0, len(prefixes))
	for _, pr := range prefixes {
		bPrefixes = append(bPrefixes, []byte(strings.Join(pr, ",")))
	}
	kvc, err := ci.store.Watch(ctx, name, bucket, bPrefixes)
	if err != nil {
		return nil, err
	}
	eCh := make(chan *Entry)
	go func() {
		defer close(eCh)
		for {
			select {
			case <-ctx.Done():
				return
			case kv, ok := <-kvc:
				if !ok {
					return
				}
				e, err := kvToEntry(kv, bucket)
				if err != nil {
					log.Errorf("store %s/%s failed to convert KV to cache entry: %v", name, bucket, err)
					return
				}
				eCh <- e
			}
		}
	}()
	return eCh, nil
}

func (c *cache) CreatePruneID(ctx context.Context, name string) (string, error) {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return "", fmt.Errorf("cache %q does not exist", name)
	}
	ci.pm.Lock()
	defer ci.pm.Unlock()
	return ci.createPruneID(ctx)
}

func (c *cache) ApplyPrune(ctx context.Context, name, id string) error {
	ci, ok := c.getCacheInstance(ctx, name)
	if !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	ci.pm.Lock()
	defer ci.pm.Unlock()
	return ci.applyPrune(ctx, id)
}

func (c *cache) getCacheInstance(ctx context.Context, name string) (*cacheInstance, bool) {
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
