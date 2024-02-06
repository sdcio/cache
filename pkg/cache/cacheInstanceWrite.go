// Copyright 2024 Nokia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	case StoreIntents:
		return ci.writeValueIntents(ctx, wo, b)
	}
	return fmt.Errorf("unknown store: %v", wo.Store)
}

func (ci *cacheInstance) writeValueConfig(ctx context.Context, cname string, wo *Opts, v []byte) error {
	if cname == "" {
		k := []byte(strings.Join(wo.Path[0], delimStr))
		log.Debugf("writing to %q: bucket=%s, k=%s, v=%v", ci.cfg.Name, configBucketName, k, v)
		ci.prune.pm.RLock()
		defer ci.prune.pm.RUnlock()
		return ci.store.WriteValue(ctx, ci.cfg.Name, configBucketName, k, v, ci.prune.pruneIndex)
	}
	// write to candidate
	return ci.writeValueConfigCandidate(ctx, cname, wo, v)
}

func (ci *cacheInstance) writeValueState(ctx context.Context, wo *Opts, v []byte) error {
	k := []byte(strings.Join(wo.Path[0], delimStr))
	log.Debugf("writing to %q: bucket=%s, k=%s, v=%v", ci.cfg.Name, stateBucketName, k, v)
	ci.prune.pm.RLock()
	defer ci.prune.pm.RUnlock()
	return ci.store.WriteValue(ctx, ci.cfg.Name, stateBucketName, k, v, ci.prune.pruneIndex)
}

func (ci *cacheInstance) writeValueIntended(ctx context.Context, wo *Opts, v []byte) error {
	now := time.Now().UnixNano()
	for _, p := range wo.Path {
		k := buildIntendedStoreWriteKey(p, wo.Priority, wo.Owner)
		// check if the key exists with a different ts and delete it
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
	GET_LOOP:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-vCh:
				if !ok {
					break GET_LOOP
				}
				kvs = append(kvs, v)
			}
		}
		// append timestamp
		ts := make([]byte, 8)
		binary.BigEndian.PutUint64(ts, uint64(now))
		k = append(k, ts...)
		log.Debugf("writing to %q: bucket=%s, k=%s, v=%v", ci.cfg.Name, intendedBucketName, k, v)
		err = ci.store.WriteValue(ctx, ci.cfg.Name, intendedBucketName, k, v, 0)
		if err != nil {
			return err
		}
		// delete other values with same prio, owner, and path but diff ts
		for _, kv := range kvs {
			err = ci.store.DeleteValue(ctx, ci.cfg.Name, intendedBucketName, kv.K)
			if err != nil {
				log.Errorf("cache=%s, failed to delete key %x: %v", ci.cfg.Name, kv.K, err)
			}
		}
	}
	return nil
}

func (ci *cacheInstance) writeValueIntents(ctx context.Context, wo *Opts, v []byte) error {
	k := []byte(strings.Join(wo.Path[0], delimStr))
	log.Debugf("writing to %q: bucket=%s, k=%s, v=%v", ci.cfg.Name, intentsBucketName, k, v)
	return ci.store.WriteValue(ctx, ci.cfg.Name, intentsBucketName, k, v, 0)
}

func (ci *cacheInstance) writeValueConfigCandidate(ctx context.Context, cname string, wo *Opts, v []byte) error {
	// write to candidate
	ci.m.Lock()
	defer ci.m.Unlock()
	cand, ok := ci.candidates[cname]
	if !ok {
		return fmt.Errorf("no such candidate %q in cache %q", ci.cfg.Name, cname)
	}

	err := cand.updates.Add(wo.Path[0], v)
	if err != nil {
		return err
	}
	// remove the added path from deletes in case it was deleted before
	cand.m.Lock()
	defer cand.m.Unlock()
	delete(cand.deletes, strings.Join(wo.Path[0], delimStr))
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
	case StoreIntents:
		return ci.deleteValueIntents(ctx, wo)
	}
	return fmt.Errorf("unknown store: %v", wo.Store)
}

func (ci *cacheInstance) deletePrefix(ctx context.Context, cname string, wo *Opts) error {
	switch wo.Store {
	case StoreConfig:
		return ci.deletePrefixConfig(ctx, cname, wo)
	case StoreState:
		return ci.deletePrefixState(ctx, wo)
	case StoreIntended:
		return ci.deletePrefixIntended(ctx, wo)
	case StoreIntents:
		return ci.deletePrefixIntents(ctx, wo)
	}
	return fmt.Errorf("unknown store: %v", wo.Store)
}

func (ci *cacheInstance) deleteValueConfig(ctx context.Context, cname string, wo *Opts) error {
	if cname == "" {
		return ci.store.DeleteValue(ctx, ci.cfg.Name, configBucketName, []byte(strings.Join(wo.Path[0], delimStr)))
	}
	cand, err := ci.getCandidate(cname)
	if err != nil {
		return err
	}
	cand.updates.Delete(wo.Path[0])

	cand.m.Lock()
	defer cand.m.Unlock()
	cand.deletes[strings.Join(wo.Path[0], delimStr)] = struct{}{}
	return nil
}

func (ci *cacheInstance) deleteValueState(ctx context.Context, wo *Opts) error {
	return ci.store.DeleteValue(ctx, ci.cfg.Name, stateBucketName, []byte(strings.Join(wo.Path[0], delimStr)))
}

func (ci *cacheInstance) deleteValueIntended(ctx context.Context, wo *Opts) error {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(wo.Priority))
	opath := make([]string, 0, 1+len(wo.Path))
	opath = append(opath, wo.Path[0]...)
	if wo.Owner != "" {
		opath = append(opath, wo.Owner)
	}
	key = append(key, []byte(strings.Join(opath, delimStr))...)
	lkey := len(key)
	return ci.store.DeletePrefix(ctx, ci.cfg.Name, intendedBucketName, key,
		func(k []byte) bool {
			return len(k)-lkey == 8
		})
}

func (ci *cacheInstance) deleteValueIntents(ctx context.Context, wo *Opts) error {
	return ci.store.DeleteValue(ctx, ci.cfg.Name, intentsBucketName, []byte(strings.Join(wo.Path[0], delimStr)))
}

func (ci *cacheInstance) deletePrefixConfig(ctx context.Context, cname string, wo *Opts) error {
	if cname == "" {
		return ci.store.DeletePrefix(ctx, ci.cfg.Name, configBucketName, []byte(strings.Join(wo.Path[0], delimStr)))
	}
	cand, err := ci.getCandidate(cname)
	if err != nil {
		return err
	}
	cand.updates.Delete(wo.Path[0])

	cand.m.Lock()
	defer cand.m.Unlock()
	cand.deletes[strings.Join(wo.Path[0], delimStr)] = struct{}{}
	return nil
}

func (ci *cacheInstance) deletePrefixState(ctx context.Context, wo *Opts) error {
	return ci.store.DeletePrefix(ctx, ci.cfg.Name, stateBucketName, []byte(strings.Join(wo.Path[0], delimStr)))
}

func (ci *cacheInstance) deletePrefixIntended(ctx context.Context, wo *Opts) error {
	key := buildIntendedStoreWriteKey(wo.Path[0], wo.Priority, wo.Owner)
	lkey := len(key)
	return ci.store.DeletePrefix(ctx, ci.cfg.Name, intendedBucketName, key,
		func(k []byte) bool {
			return len(k)-lkey >= 8

		})
}

func (ci *cacheInstance) deletePrefixIntents(ctx context.Context, wo *Opts) error {
	return ci.store.DeletePrefix(ctx, ci.cfg.Name, intentsBucketName, []byte(strings.Join(wo.Path[0], delimStr)))
}

// Intended store key format
// this key is parsed right to left.
// having the path at the beginning allows for fast path queries.
// +--------------------------------------------+--------------+-------------+-----------------+--------------+
// | Variable Length Path Elements   			| 4 Bytes      | Variable    | 2 Bytes         | 8 Bytes      |
// | (Comma Separated and ending with a comma)	| Priority     | Owner       | Owner Length    | Timestamp    |
// +--------------------------------------------+--------------+-------------+-----------------+--------------+
func buildIntendedStoreWriteKey(path []string, priority int32, owner string) []byte {
	joinedPath := strings.Join(path, delimStr) + delimStr
	pathLength := len(joinedPath)
	ownerLength := len(owner)

	if priority == 0 {
		priority = defaultWritePriority
	}

	k := make([]byte, pathLength+4+2+ownerLength)
	copy(k, joinedPath)
	binary.BigEndian.PutUint32(k[pathLength:pathLength+4], uint32(priority))
	copy(k[pathLength+4:pathLength+4+ownerLength], owner)
	binary.BigEndian.PutUint16(k[pathLength+4+ownerLength:], uint16(ownerLength))
	return k
}
