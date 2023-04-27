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

func newCacheInstance[T proto.Message](ctx context.Context, cfg *CacheInstanceConfig, bfn func() T) (*cacheInstance[T], error) {
	ci := initCacheInstance(cfg, bfn)
	storeConfig := map[string]any{
		"cached": cfg.Cached,
	}
	err := ci.store.CreateCache(ctx, cfg.Name, storeConfig, defaultBuckets...)
	return ci, err
}

func initCacheInstance[T proto.Message](cfg *CacheInstanceConfig, bfn func() T) *cacheInstance[T] {
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
