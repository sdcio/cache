package cache

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/cache/pkg/store"
)

func (ci *cacheInstance) writeValue(ctx context.Context, cname string, wo *Opts, b []byte) error {
	switch wo.Store {
	case StoreConfig:
		return ci.writeValueConfig(ctx, cname, wo, b)
	case StoreState:
		return ci.writeValueState(ctx, wo, b)
	case StoreIntended:
		return ci.writeValueIntended(ctx, wo, b)
	}
	return fmt.Errorf("unknown store: %v", wo.Store)
}

func (ci *cacheInstance) writeValueConfig(ctx context.Context, cname string, wo *Opts, v []byte) error {
	if cname == "" {
		k := []byte(strings.Join(wo.Path, delimStr))
		log.Debugf("writing to %q: bucket=%s, k=%s, v=%v", ci.cfg.Name, configBucketName, k, v)
		return ci.store.WriteValue(ctx, ci.cfg.Name, configBucketName, k, v)
	}
	// write to candidate
	return ci.writeValueConfigCandidate(ctx, cname, wo, v)
}

func (ci *cacheInstance) writeValueState(ctx context.Context, wo *Opts, v []byte) error {
	k := []byte(strings.Join(wo.Path, delimStr))
	log.Debugf("writing to %q: bucket=%s, k=%s, v=%v", ci.cfg.Name, stateBucketName, k, v)
	return ci.store.WriteValue(ctx, ci.cfg.Name, stateBucketName, k, v)
}

func (ci *cacheInstance) writeValueIntended(ctx context.Context, wo *Opts, v []byte) error {
	if wo.Priority <= 0 {
		wo.Priority = defaultWritePriority
	}
	k := make([]byte, 4)
	binary.BigEndian.PutUint32(k, uint32(wo.Priority))
	opath := make([]string, 0, len(wo.Path)+1)
	opath = append(opath, wo.Path...)
	// append owner
	if wo.Owner != "" {
		opath = append(opath, wo.Owner)
	}
	k = append(k, []byte(strings.Join(opath, delimStr))...)
	// check if the key exists with a different ts and delete
	lkey := len(k)
	ck := make([]byte, lkey)
	copy(ck, k)
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, intendedBucketName, ck, nil,
		func(k []byte) bool {
			return len(k)-lkey == 8
		})
	if err != nil {
		return err
	}
	kvs := make([]*store.KV, 0, 1)
CH_LOOP:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				break CH_LOOP
			}
			kvs = append(kvs, v)
		}
	}

	// append timestamp
	now := time.Now().UnixNano()
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(now))
	k = append(k, ts...)
	log.Debugf("writing to %q: bucket=%s, k=%x, v=%v", ci.cfg.Name, intendedBucketName, k, v)
	err = ci.store.WriteValue(ctx, ci.cfg.Name, intendedBucketName, k, v)
	if err != nil {
		return err
	}
	// delete other values with same prio, owner, and path but diff ts
	for _, kv := range kvs {
		fmt.Println("deleting ", string(kv.K))
		err = ci.store.DeleteValue(ctx, ci.cfg.Name, intendedBucketName, kv.K)
		if err != nil {
			log.Errorf("cache=%s, failed to delete key %x: %v", ci.cfg.Name, kv.K, err)
		}
	}
	return nil
}

func (ci *cacheInstance) writeValueConfigCandidate(ctx context.Context, cname string, wo *Opts, v []byte) error {
	// write to candidate
	ci.m.Lock()
	defer ci.m.Unlock()
	cand, ok := ci.candidates[cname]
	if !ok {
		return fmt.Errorf("no such candidate %q in cache %q", ci.cfg.Name, cname)
	}

	err := cand.updates.Add(wo.Path, v)
	if err != nil {
		return err
	}
	// remove the added path from deletes in case it was deleted before
	cand.m.Lock()
	defer cand.m.Unlock()
	delete(cand.deletes, strings.Join(wo.Path, delimStr))
	return nil
}

func (ci *cacheInstance) deleteValue(ctx context.Context, cname string, wo *Opts) error {
	switch wo.Store {
	case StoreConfig:
		return ci.deleteValueConfig(ctx, cname, wo)
	case StoreState:
		return ci.deleteValueState(ctx, wo)
	case StoreIntended:
		return ci.deleteValueIntended(ctx, wo)
	}
	return fmt.Errorf("unknown store: %v", wo.Store)
}

func (ci *cacheInstance) deleteValueConfig(ctx context.Context, cname string, wo *Opts) error {
	if cname == "" {
		return ci.store.DeleteValue(ctx, ci.cfg.Name, configBucketName, []byte(strings.Join(wo.Path, delimStr)))
	}
	f, err := ci.getCandidate(cname)
	if err != nil {
		return err
	}
	f.updates.Delete(wo.Path)

	f.m.Lock()
	defer f.m.Unlock()
	f.deletes[strings.Join(wo.Path, delimStr)] = struct{}{}
	return nil
}

func (ci *cacheInstance) deleteValueState(ctx context.Context, wo *Opts) error {
	return ci.store.DeleteValue(ctx, ci.cfg.Name, stateBucketName, []byte(strings.Join(wo.Path, delimStr)))
}

func (ci *cacheInstance) deleteValueIntended(ctx context.Context, wo *Opts) error {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(wo.Priority))
	opath := make([]string, 0, 1+len(wo.Path))
	opath = append(opath, wo.Path...)
	if wo.Owner != "" {
		opath = append(opath, wo.Owner)
	}
	key = append(key, []byte(strings.Join(opath, delimStr))...)
	lkey := len(key)
	return ci.store.DeletePrefix(ctx, ci.cfg.Name, intendedBucketName, key,
		func(k []byte) bool {
			return lkey-len(k) == 8
		})
}
