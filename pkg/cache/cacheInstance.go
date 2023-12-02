package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/cache/pkg/ctree"
	"github.com/iptecharch/cache/pkg/store"
)

const (
	configBucketName   = "config"
	stateBucketName    = "state"
	intendedBucketName = "intended"
	metadataBucketName = "metadata"

	// max int32
	defaultWritePriority = 0x7FFFFFFF
)

var delimStr = ","
var delimBytes = []byte(delimStr)

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

	if err := ci.store.CreateCache(ctx, cfg.Name); err != nil {
		return nil, err
	}
	return ci, nil
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
		return nil, nil, fmt.Errorf("cache %q: candidate %q does not exist", ci.cfg.Name, candidate)
	}

	lCandDel := len(cand.deletes)

	// results
	dels := make([][]string, 0, lCandDel)
	es := make([]*Entry, 0)

	// deletes
	candPb := make([]byte, 4)
	binary.BigEndian.PutUint32(candPb, uint32(cand.priority))
	for p := range cand.deletes {
		pb := []byte(p)
		lpb := len(pb)

		// get 2 keys/values i.e 2 highest priorities
		kvs, err := ci.store.GetN(ctx, ci.cfg.Name, intendedBucketName, 2,
			func(k []byte) bool {
				lk := len(k)
				// must include priority and TS
				if lk < 4+8 {
					return false
				}
				// check for prefix
				if !bytes.HasPrefix(k[4:lk-8], pb) {
					return false
				}
				// make sure this is an exact prefix,
				// next byte should be a ","
				return lk > 4+lpb && bytes.HasPrefix(k[4+lpb:], []byte(","))
			})
		if err != nil {
			return nil, nil, err
		}

		lkvs := len(kvs)
		switch lkvs {
		case 0:
		case 1:
			// got a single value from the intended store
			highP := int32(binary.BigEndian.Uint32(kvs[0].K[:4]))
			switch {
			case highP == cand.priority:
				dels = append(dels, strings.Split(p, delimStr))
			}
		case 2:
			sort.Slice(kvs, func(i, j int) bool {
				return bytes.Compare(kvs[i].K[:4], kvs[j].K[:4]) < 0
			})

			for _, kv := range kvs {
				log.Debugf("%x: %s: %s\n", kv.K[:4], kv.K[4:len(kv.K)-8], kv.V)
			}
			highP := int32(binary.BigEndian.Uint32(kvs[0].K[:4]))
			switch {
			case highP == cand.priority:
				// deleting the highest priority value,
				// so set the next priority value.
				e, err := kvToEntry(kvs[1], intendedBucketName)
				if err != nil {
					return nil, nil, err
				}
				es = append(es, e)
			case highP < cand.priority:
				// deleting a value, not the highest
			case highP > cand.priority:
				// ???
			}
		}
	}
	// updates
	ts := uint64(time.Now().UnixNano())
	err := cand.updates.Query([]string{},
		func(path []string, _ *ctree.Leaf, val interface{}) error {
			log.Infof("query path %v", path)
			vt := val.([]byte)
			bPath := []byte(strings.Join(path, delimStr))
			e, err := ci.readValueFromIntendedStoreHighPrioCh(ctx, bPath)
			log.Infof("highest priority for path %v: %+v: %v", path, e, err)
			if err != nil {
				if errors.Is(err, store.ErrKeyNotFound) {
					es = append(es, &Entry{
						Timestamp: ts,
						Owner:     cand.owner,
						Priority:  cand.priority,
						P:         path,
						V:         vt,
					})
					return nil
				}
				return err
			}
			switch {
			case e.Priority >= cand.priority:
				e := &Entry{
					Timestamp: ts,
					Owner:     cand.owner,
					Priority:  cand.priority,
					P:         path,
					V:         vt,
				}
				es = append(es, e)
			case e.Priority < cand.priority:
				es = append(es, e)
			}
			return nil
		})
	if err != nil {
		return nil, nil, err
	}
	log.Infof("cache %q: candidate %q diff result\n", ci.cfg.Name, candidate)
	log.Infof("cache %q: deletes: %v\n", ci.cfg.Name, dels)
	for i, e := range es {
		log.Infof("cache %q: updates: %d: %s\n", ci.cfg.Name, i, e)
	}
	return dels, es, nil
}

func (ci *cacheInstance) commit(ctx context.Context, candidate string) error {
	ci.m.RLock()
	defer ci.m.RUnlock()
	cand, ok := ci.candidates[candidate]
	if !ok {
		return fmt.Errorf("candidate %q does not exist for cache %q", candidate, ci.cfg.Name)
	}
	var err error
	// write updates to intended store
	ts := make([]byte, 8)
	prio := make([]byte, 4)
	binary.BigEndian.PutUint64(ts, uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(prio, uint32(cand.priority))
	// prepare transaction
	txn := &store.TxnOpts{
		Updates: make([]*store.KV, 0),
		Deletes: make([]*store.DelOpts, 0, len(cand.deletes)),
	}
	// handle deletes
	for p := range cand.deletes {
		p += "," + cand.owner
		bPath := []byte(p)
		kp := make([]byte, 0, 4+len(bPath))
		kp = append(kp, prio...)
		kp = append(kp, bPath...)
		txn.Deletes = append(txn.Deletes, &store.DelOpts{
			K:   kp,
			Fns: []store.SelectFn{selectPrefixMinusTS(kp)},
		})
	}
	// handle updates
	err = cand.updates.Query([]string{},
		func(path []string, _ *ctree.Leaf, val interface{}) error {
			vt := val.([]byte)
			path = append(path, cand.owner)
			bPath := []byte(strings.Join(path, delimStr))
			k := make([]byte, 0, 4+len(bPath)+8)
			k = append(k, prio...)
			k = append(k, bPath...)
			pr := make([]byte, len(k))
			copy(pr, k)
			txn.Deletes = append(txn.Deletes, &store.DelOpts{
				K: pr,
				Fns: []store.SelectFn{
					selectPrefixMinusTS(pr),
					notTS(ts),
				},
			})
			k = append(k, ts...)
			txn.Updates = append(txn.Updates, &store.KV{K: k, V: vt})
			return nil
		},
	)
	if err != nil {
		return err
	}
	// apply transaction
	return ci.store.Txn(ctx, ci.cfg.Name, intendedBucketName, txn)
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

func (ci *cacheInstance) clear(ctx context.Context) error {
	ci.m.Lock()
	defer ci.m.Unlock()
	ci.candidates = make(map[string]*candidate)
	return ci.store.Clear(ctx, ci.cfg.Name)
}

func selectPrefixMinusTS(k []byte) store.SelectFn {
	lkp := len(k)
	return func(k []byte) bool {
		lk := len(k)
		return lk-8 == lkp
	}
}

func notTS(ts []byte) store.SelectFn {
	return func(k []byte) bool {
		return !bytes.Equal(k[len(k)-8:], ts)
	}
}
