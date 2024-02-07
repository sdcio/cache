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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/sdcio/cache/pkg/ctree"
	"github.com/sdcio/cache/pkg/store"
)

// readValueCh is the main reading method on cache instance
// it reads paths ro.Path from the store.
// It also handles reading values from a candidate in case of a config store.
func (ci *cacheInstance) readValueCh(ctx context.Context, cname string, ro *Opts) (chan *Entry, error) {
	rsCh := make(chan *Entry)
	var err error
	switch ro.Store {
	case StoreConfig:
		var cand *candidate
		var ok bool
		if cname != "" {
			ci.m.RLock()
			defer ci.m.RUnlock()
			cand, ok = ci.candidates[cname]
			if !ok {
				return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
			}
		}
		go func() {
			err = ci.readFromConfigStore(ctx, cand, ro, rsCh)
			if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	case StoreState:
		go func() {
			err = ci.readFromStateStore(ctx, ro, rsCh)
			if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	case StoreIntended:
		go func() {
			err = ci.readFromIntendedStore(ctx, ro, rsCh)
			if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	case StoreIntents:
		go func() {
			err = ci.readFromIntentsStore(ctx, ro, rsCh)
			if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	default:
		return nil, fmt.Errorf("unknown store type %d", ro.Store)
	}

	return rsCh, nil
}

func (ci *cacheInstance) readFromConfigStore(ctx context.Context, cand *candidate, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)

	var err error
	pathsFoundInCandidate := make(map[string]struct{})

	// read from candidate first
	if cand != nil {
		for _, p := range ro.Path {
			err = cand.updates.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					vt := val.([]byte)
					e := &Entry{
						P: path,
						V: vt,
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case rsCh <- e:
					}
					pathsFoundInCandidate[strings.Join(path, delimStr)] = struct{}{}
					return nil
				})
			if err != nil {
				log.Errorf("failed to run query: %v", err)
				return err
			}
		}
	}
	// convert paths to keys ([]byte)
	keys := make([][]byte, 0, len(ro.Path))
	for _, p := range ro.Path {
		keys = append(keys, []byte(strings.Join(p, delimStr)))
	}

	// read from config(main)
	return ci.readPrefixFromConfigStoreCh(ctx, keys, cand, pathsFoundInCandidate, rsCh)
}

func (ci *cacheInstance) readFromStateStore(ctx context.Context, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)

	keys := make([][]byte, 0, len(ro.Path))
	for _, p := range ro.Path {
		keys = append(keys, []byte(strings.Join(p, delimStr)))
	}

	return ci.readPrefixFromStateStoreCh(ctx, keys, rsCh)
}

func (ci *cacheInstance) readFromIntendedStore(ctx context.Context, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)

	log.Debugf("reading from intended store: %+v", ro)
	// prepare keys
	keys := make([][]byte, 0, len(ro.Path))
	for _, p := range ro.Path {
		keys = append(keys, buildIntendedStoreReadeKey(p, ro.Priority, ro.Owner))
	}
	sfn := []store.SelectFn{}
	switch {
	case ro.Priority > 0: // read specific priority
	case ro.Priority == 0: // read highest priority(ies) (priority count per path)
		sfn = append(sfn, highestPrioritiesSelectFn(int32(ro.PriorityCount)))
	case ro.Priority < 0: // read all priorities for all paths
	}
	//
	vCh, err := ci.store.GetBatch(ctx, ci.cfg.Name, intendedBucketName, keys, sfn...)
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
			lk := len(v.K)
			if lk < 12 {
				log.Errorf("GetBatch returned key is too short (<12): %x", v.K)
				continue
			}

			log.Debugf("reading path=%s | priority=%x | ts=%x\n",
				string(v.K[:lk-12]), v.K[lk-12:lk-8], v.K[lk-8:])
			e, err := kvToEntry(v, intendedBucketName)
			if err != nil {
				return err
			}

			select {
			case rsCh <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readFromIntentsStore(ctx context.Context, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)
	for _, p := range ro.Path {
		err := ci.readFromIntentsStoreCh(ctx, []byte(strings.Join(p, delimStr)), ro.KeysOnly, rsCh)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ci *cacheInstance) readPrefixFromConfigStoreCh(ctx context.Context, keys [][]byte,
	cand *candidate,
	found map[string]struct{}, kvCh chan *Entry) error {
	bucket := configBucketName
	vCh, err := ci.store.GetBatch(ctx, ci.cfg.Name, bucket, keys)
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
			// path
			ep := string(v.K)
			if cand != nil {
				// check if the path was found
				// in the candidate so we don't
				// return it again
				if _, ok := found[ep]; ok {
					continue
				}
				// check if the path has been deleted
				// from the candidate
				cand.m.RLock()
				_, ok = cand.deletes[ep]
				cand.m.RUnlock()
				if ok {
					continue
				}
			}

			e, _ := kvToEntry(v, bucket)

			select {
			case kvCh <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readPrefixFromStateStoreCh(ctx context.Context, keys [][]byte, kvCh chan *Entry) error {
	var bucket = "state"
	vCh, err := ci.store.GetBatch(ctx, ci.cfg.Name, bucket, keys)
	if err != nil {
		return err
	}
	count := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				return nil
			}
			e, _ := kvToEntry(v, bucket)

			select {
			case kvCh <- e:
				count++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readValueFromIntendedStoreHighPrioCh(ctx context.Context, p []byte) (*Entry, error) {
	var bucket = "intended"
	kvs, err := ci.store.GetN(ctx, ci.cfg.Name, bucket, 1,
		func(k []byte) bool {
			lk := len(k)
			// must include priority, owner length, owner and  TS
			if lk < 4+1+2+8 {
				return false
			}
			// check for prefix
			return bytes.HasPrefix(k[:lk-10], p)
		})
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}
	e, err := kvToEntry(kvs[0], bucket)
	return e, err
}

func (ci *cacheInstance) readFromIntentsStoreCh(ctx context.Context, key []byte, keysOnly bool, kvCh chan *Entry) error {
	if keysOnly {
		return ci.readFromIntentsStoreKeysOnlyCh(ctx, kvCh)
	}
	var bucket = "intents"
	v, err := ci.store.GetValue(ctx, ci.cfg.Name, bucket, key)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case kvCh <- &Entry{
		P: []string{string(key)},
		V: v,
	}:
	}
	return nil
}

func (ci *cacheInstance) readFromIntentsStoreKeysOnlyCh(ctx context.Context, kvCh chan *Entry) error {
	var bucket = "intents"
	sCh, err := ci.store.GetAll(ctx, ci.cfg.Name, bucket, true)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case kv, ok := <-sCh:
			if !ok {
				return nil
			}
			kvCh <- &Entry{
				P: []string{string(kv.K)},
			}
		}
	}
}

func kvToEntry(v *store.KV, bucket string) (*Entry, error) {
	switch bucket {
	case "config", "state":
		return &Entry{
			P: strings.Split(string(v.K), delimStr),
			V: v.V,
		}, nil
	case "intended":
		return parseIntendedStoreKey(v.K, v.V)
	}
	return nil, fmt.Errorf("unknown bucket name %q", bucket)
}

// Intended store key format
// +--------------------------------------------+--------------+-------------+-----------------+--------------+
// | Variable Length Path Elements   			| 4 Bytes      | Variable    | 2 Bytes         | 8 Bytes      |
// | (Comma Separated and ending with a comma)	| Priority     | Owner       | Owner Length    | Timestamp    |
// +--------------------------------------------+--------------+-------------+-----------------+--------------+
func buildIntendedStoreReadeKey(path []string, priority int32, owner string) []byte {
	joinedPath := strings.Join(path, delimStr) + delimStr
	pathLength := len(joinedPath)

	if priority <= 0 {
		return []byte(joinedPath)
	}

	ownerLength := len(owner)
	totalLength := pathLength + 4

	if owner != "" {
		totalLength += 2 + ownerLength
	}

	k := make([]byte, totalLength)

	copy(k, joinedPath)

	offset := pathLength
	binary.BigEndian.PutUint32(k[offset:], uint32(priority))
	offset += 4

	if owner != "" {
		copy(k[offset:offset+ownerLength], owner)
		offset += ownerLength

		binary.BigEndian.PutUint16(k[offset:], uint16(ownerLength))
	}

	return k
}

// this function assumes GetBatch goes through the priorities in ascending order
// for a given path, which is true as long as I don't mess up the key format again.
func highestPrioritiesSelectFn(priorityCount int32) store.SelectFn {
	// This mutex and map are used
	// to keep track of the number of
	// encountered values per key (i.e path) per priority.
	m := new(sync.Mutex)
	// we could use map[string]map[int32]struct{} instead of map[string]map[int32]int32.
	// In a future optimization the last int32 value can be used to limit
	// the number of intents per priority.
	readPaths := make(map[string]map[int32]int32)

	return func(k []byte) bool {
		e, err := parseIntendedStoreKey(k, nil)
		if err != nil {
			return false
		}
		path := strings.Join(e.P, delimStr)

		// lock readPaths map
		m.Lock()
		defer m.Unlock()

		// unknown path: add it and return true
		if _, ok := readPaths[path]; !ok {
			readPaths[path] = map[int32]int32{e.Priority: 1}
			return true
		}
		// known path, unknown priority:
		if _, ok := readPaths[path][e.Priority]; !ok {
			// check priority count
			if len(readPaths[path]) >= int(priorityCount) {
				return false // we have enough priorities
			}
			// add it and return true
			readPaths[path][e.Priority] = 1
			return true
		}
		// known path, known priority
		readPaths[path][e.Priority]++
		return true
	}
}

// Intended store key format
// +--------------------------------------------+--------------+-------------+-----------------+--------------+
// | Variable Length Path Elements   			| 4 Bytes      | Variable    | 2 Bytes         | 8 Bytes      |
// | (Comma Separated and ending with a comma)	| Priority     | Owner       | Owner Length    | Timestamp    |
// +--------------------------------------------+--------------+-------------+-----------------+--------------+

func parseIntendedStoreKey(k, v []byte) (*Entry, error) {
	lkey := len(k)
	// min: 1 byte path, 4 bytes priority, 1 byte owner, 2 bytes owner length, 8 bytes timestamp
	if lkey < 16 {
		return nil, fmt.Errorf("intended store key too short(<16): %x", k)
	}

	// extract timestamp, owner length, owner, and priority
	ts := k[lkey-8:]
	lOwner := binary.BigEndian.Uint16(k[lkey-10 : lkey-8])
	owner := string(k[lkey-10-int(lOwner) : lkey-10])
	pr := k[lkey-10-int(lOwner)-4 : lkey-10-int(lOwner)]

	// split the path elements
	pathEnd := lkey - 10 - int(lOwner) - 4
	keyItems := bytes.Split(k[:pathEnd], delimBytes)

	e := &Entry{
		Timestamp: binary.BigEndian.Uint64(ts),
		Owner:     owner,
		Priority:  int32(binary.BigEndian.Uint32(pr)),
		P:         make([]string, len(keyItems)-1),
		V:         v,
	}

	// copy the path elements into the entry
	for i, ki := range keyItems[:len(keyItems)-1] {
		e.P[i] = string(ki)
	}

	return e, nil
}
