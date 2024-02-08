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

package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/z"
	log "github.com/sirupsen/logrus"
)

const (
	dbName = "cached"
)

type badgerDB struct {
	path         string
	cfn          context.CancelFunc
	m            *sync.RWMutex
	cacheIndexes map[string]uint16
	cacheNames   map[uint16]string
	db           *badger.DB
}

func newBadgerDBStore(p string) Store {
	if p == "" {
		p = dbName
	}
	s := &badgerDB{
		path:         p,
		m:            &sync.RWMutex{},
		cacheIndexes: map[string]uint16{},
		cacheNames:   map[uint16]string{},
	}
	dbCtx, cancel := context.WithCancel(context.Background())
	s.cfn = cancel

	var err error
CREATE_DB:
	s.db, err = s.openDB(dbCtx)
	if err != nil {
		log.Errorf("failed to create DB: %v", err)
		time.Sleep(time.Second)
		goto CREATE_DB
	}
	log.Infof("DB created/read")
	log.Infof("loading existing caches")

	s.m.Lock()
	defer s.m.Unlock()
LOAD_CACHES:
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// opts.PrefetchValues = false
		opts.PrefetchSize = 0xFFFF
		prefix := []byte{metaPrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().KeyCopy(nil)
			if len(k) < 3 {
				log.Warnf("found short key(<3) in caches store: %x", k)
				continue
			}
			log.Debugf("loading meta key: %x | %x | %s", k[0], k[1:3], string(k[3:]))
			cacheIndex := binary.BigEndian.Uint16(k[1:3])
			cacheName := string(k[3:])
			s.cacheIndexes[cacheName] = cacheIndex
			s.cacheNames[cacheIndex] = cacheName
		}
		return nil
	})
	if err != nil {
		log.Errorf("failed to load indexes: %v", err)
		time.Sleep(time.Second)
		goto LOAD_CACHES
	}
	log.Infof("existing caches loaded")
	return s
}

func (s *badgerDB) CreateCache(ctx context.Context, name string) error {
	s.m.Lock()
	defer s.m.Unlock()

	if len(s.cacheNames) == 0xFFFF {
		return fmt.Errorf("max caches (65536) reached")
	}

	if _, ok := s.cacheIndexes[name]; ok {
		return fmt.Errorf("cache %q already exists", name)
	}

	// find next free index
	index := s.nextAvailableKey()
	// write index/name
	err := s.writeCacheIndex(ctx, index, name, 0)
	if err != nil {
		return err
	}
	s.cacheIndexes[name] = index
	s.cacheNames[index] = name
	return nil
}

func (s *badgerDB) ListCaches(ctx context.Context) ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	caches := make([]string, 0, len(s.cacheIndexes))
	for k := range s.cacheIndexes {
		caches = append(caches, k)
	}
	sort.Strings(caches)
	return caches, nil
}

func (s *badgerDB) DeleteCache(ctx context.Context, name string) error {
	s.m.Lock()
	defer s.m.Unlock()

	if index, ok := s.cacheIndexes[name]; ok {
		err := s.clear(ctx, name)
		if err != nil {
			return err
		}
		err = s.deleteCacheIndex(ctx, index)
		if err != nil {
			return err
		}

		delete(s.cacheIndexes, name)
		delete(s.cacheNames, index)

		cib := cacheIndexKey(index)
		prefixes := make([][]byte, 0, 4)
		for _, s := range []byte{configPrefix, statePrefix, intendedPrefix, intentsPrefix} {
			prefixes = append(prefixes, []byte{s, cib[0], cib[1]})
		}
		return s.db.DropPrefix(prefixes...)
	}
	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) Clone(ctx context.Context, name, cname string) error {
	s.m.Lock()
	defer s.m.Unlock()

	if len(s.cacheNames) == 0xFFFF {
		return fmt.Errorf("max caches (65536) reached")
	}
	if _, ok := s.cacheIndexes[name]; !ok {
		return fmt.Errorf("cache %q does not exist", name)
	}
	if _, ok := s.cacheIndexes[cname]; ok {
		return fmt.Errorf("cache %q already exists", name)
	}
	// find next free index
	index := s.nextAvailableKey()
	s.cacheIndexes[name] = index
	s.cacheNames[index] = name
	s.db.Update(func(txn *badger.Txn) error {
		return nil
	})
	return nil
}

// data

func (s *badgerDB) WriteValue(ctx context.Context, name, bucket string, k []byte, v []byte, m byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		fk := buildFullKey(bucket, index, k)
		return s.db.Update(func(txn *badger.Txn) error {
			e := badger.NewEntry(fk, v).WithMeta(m)
			log.Debugf("writing kv %x | %x | %s with meta %x", fk[0], fk[1:3], fk[3:], m)
			return txn.SetEntry(e)
		})
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}
	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		var v []byte
		fk := buildFullKey(bucket, index, k)
		err := s.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(fk)
			if err != nil {
				// key not found error
				if errors.Is(err, badger.ErrKeyNotFound) {
					return ErrKeyNotFound
				}
				return err
			}
			v = make([]byte, 0, item.ValueSize())
			v, _ = item.ValueCopy(v)
			return nil
		})
		return v, err
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		fk := buildFullKey(bucket, index, k)
		return s.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(fk)
		})
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) DeletePrefix(ctx context.Context, name, bucket string, k []byte, fn ...SelectFn) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		return s.db.Update(s.deletePrefixSingleFn(bucket, index, k, fn...))
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) GetAll(ctx context.Context, name, bucket string, keysOnly bool, fn ...SelectFn) (chan *KV, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		kvCh := make(chan *KV)
		if keysOnly {
			go s.getAllKeysOnly(ctx, bucket, index, kvCh, fn...)
			return kvCh, nil
		}
		go s.getAll(ctx, bucket, index, kvCh, fn...)
		return kvCh, nil
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte, fn ...SelectFn) (chan *KV, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		kvCh := make(chan *KV, 100) // TODO:
		fk := buildFullKey(bucket, index, prefix)
		go func() {
			defer close(kvCh)
			err := s.db.View(func(txn *badger.Txn) error {
				withPattern := len(pattern) > 0
				var re *regexp.Regexp
				var err error
				if withPattern {
					re, err = regexp.Compile(string(pattern))
					if err != nil {
						return err
					}
				}
				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()
				prefixLen := len(fk)
			OUTER:
				for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
					item := it.Item()
					k := item.Key()
					if withPattern && !re.Match(k[3:]) {
						continue
					}
					lk := len(k)
					if lk > prefixLen && !bytes.HasPrefix(k[prefixLen:], sepBytes) { // is an exact prefix
						continue
					}
					for _, sfn := range fn {
						if !sfn(k[3:]) {
							continue OUTER
						}
					}
					err := item.Value(func(v []byte) error {
						return kvToChan(ctx, k[3:], v, kvCh)
					})
					if err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				log.Errorf("failed to read values based on prefix from cache %s: %v", name, err)
			}
		}()
		return kvCh, nil
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) GetN(ctx context.Context, name, bucket string, n uint64, fn ...SelectFn) ([]*KV, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		fk := buildFullKey(bucket, index, nil)
		rkvs := make([]*KV, 0, n)
		count := uint64(0)

		err := s.db.View(func(tx *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			it := tx.NewIterator(opts)
			defer it.Close()
		OUTER:
			for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
				item := it.Item()
				k := item.KeyCopy(nil)
				for _, sfn := range fn {
					if !sfn(k[3:]) {
						continue OUTER
					}
				}

				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				count++
				rkvs = append(rkvs, &KV{
					K: k[3:],
					V: v,
				})
				if count >= n { // done
					return nil
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read N values from cache %s: %v", name, err)
		}
		return rkvs, nil
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) GetBatch(ctx context.Context, name, bucket string, keys [][]byte, fn ...SelectFn) (chan *KV, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		kvCh := make(chan *KV)
		go s.getBatch(ctx, bucket, index, keys, kvCh, fn...)
		return kvCh, nil
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) Txn(ctx context.Context, name, bucket string, txnOpts *TxnOpts) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		var err error
	RETRY:
		err = s.db.Update(func(txn *badger.Txn) error {
			var err error
			for _, del := range txnOpts.Deletes {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					err = s.deletePrefixSingleFn(bucket, index, del.K, del.Fns...)(txn)
					if err != nil {
						return err
					}
				}
			}
			for _, kv := range txnOpts.Updates {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					fk := buildFullKey(bucket, index, kv.K)
					err = txn.Set(fk, kv.V)
					if err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			// ugly: https://github.com/dgraph-io/badger/issues/742
			if strings.HasPrefix(err.Error(), "Transaction Conflict.") {
				goto RETRY
			}
			return err
		}
		return nil
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) Watch(ctx context.Context, name, bucket string, prefixes [][]byte) (chan *KV, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		kvc := make(chan *KV)
		matches := make([]pb.Match, 0, len(prefixes))
		for i := range prefixes {
			fk := buildFullKey(bucket, index, prefixes[i])
			matches = append(matches, pb.Match{Prefix: fk})
		}
		if len(prefixes) == 0 {
			matches = []pb.Match{{}} // empty match
		}
		go func() {
			defer close(kvc)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			err := s.db.Subscribe(ctx,
				func(kv *badger.KVList) error {
					for _, kvitem := range kv.GetKv() {
						if bytes.Equal(badgerTxnKey, kvitem.GetKey()) {
							continue
						}
						select {
						case <-ctx.Done():
							return ctx.Err()
						case kvc <- &KV{
							K: kvitem.GetKey()[3:],
							V: kvitem.GetValue(),
						}:
						}

					}
					return nil
				}, matches)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Errorf("db subscribe err: %v", err)
			}
		}()
		return kvc, nil
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) Close() error {
	s.cfn()
	s.db.Close()
	return nil
}

func (s *badgerDB) Clear(ctx context.Context, name string) error {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.clear(ctx, name)
}

func (s *badgerDB) Prune(ctx context.Context, name, bucket string, pruneIndex uint8) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		return s.db.Update(
			func(txn *badger.Txn) error {
				fk := buildFullKey(bucket, index, nil)
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()

				keysToDelete := make([][]byte, 0, deletePrefixBatchSize)

				for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
					item := it.Item()
					if item.UserMeta() == pruneIndex {
						continue
					}
					//fmt.Printf("meta=%d, %d | %x | %x | %s DELETING\n", item.UserMeta(), meta, fkey[0], fkey[1:3], fkey[3:])
					key := item.KeyCopy(nil)
					keysToDelete = append(keysToDelete, key)
					if len(keysToDelete) >= deletePrefixBatchSize {
						err := s.deleteKeys(keysToDelete)
						if err != nil {
							return err
						}
						keysToDelete = make([][]byte, 0)
					}
				}
				err := s.deleteKeys(keysToDelete)
				if err != nil {
					return err
				}
				return nil
			},
		)
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) SetPruneIndex(ctx context.Context, name string, pruneIndex uint8) error {
	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		return s.db.Update(func(txn *badger.Txn) error {
			mk := make([]byte, 0, 3)
			mk = append(mk, metaPrefix)
			mk = append(mk, cacheIndexKey(index)...)
			bn := []byte(name)
			mk = append(mk, bn...)
			return txn.Set(mk, []byte{pruneIndex})
		})
	}
	return fmt.Errorf("unknown cache name %s", name)
}

func (s *badgerDB) GetPruneIndex(ctx context.Context, name string) (uint8, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		var pidx uint8
		err := s.db.View(func(txn *badger.Txn) error {
			mk := make([]byte, 0, 3)
			mk = append(mk, metaPrefix)
			mk = append(mk, cacheIndexKey(index)...)
			bn := []byte(name)
			mk = append(mk, bn...)
			item, err := txn.Get(mk)
			if err != nil {
				return err
			}
			if item.ValueSize() != 1 {
				return fmt.Errorf("incorrect pruneIndex value for cache %s", name)
			}
			bv, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			pidx = bv[0]
			return nil
		})
		if err != nil {
			return 0, err
		}
		return pidx, nil
	}
	return 0, fmt.Errorf("unknown cache name %s", name)
}

// helpers

func (s *badgerDB) openDB(ctx context.Context) (*badger.DB, error) {
	opts := badger.DefaultOptions(s.path).
		WithLoggingLevel(badger.WARNING).
		WithCompression(options.None).
		WithBlockCacheSize(0)

	bdb, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			again:
				log.Debugf("running GC for %s", s.path)
				err = bdb.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
				log.Debugf("GC for %s ended with err: %v", s.path, err)
			}
		}
	}()
	return bdb, nil
}

func (s *badgerDB) deleteKeys(keys [][]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *badgerDB) nextAvailableKey() uint16 {
	for i := uint16(0); i < ^uint16(0); i++ { // ^uint16(0) gives the maximum value of uint16
		if _, exists := s.cacheNames[i]; !exists {
			return i
		}
	}
	return ^uint16(0)
}

func (s *badgerDB) writeCacheIndex(ctx context.Context, index uint16, name string, pruneIndex uint8) error {
	return s.db.Update(func(txn *badger.Txn) error {
		mk := make([]byte, 0, 3)
		mk = append(mk, metaPrefix)
		mk = append(mk, cacheIndexKey(index)...)
		bn := []byte(name)
		mk = append(mk, bn...)
		return txn.Set(mk, []byte{pruneIndex})
	})
}

func (s *badgerDB) clear(ctx context.Context, name string) error {
	if index, ok := s.cacheIndexes[name]; ok {
		return s.db.Update(func(txn *badger.Txn) error {
			fk := make([]byte, 1, 3)
			fk[0] = metaPrefix
			fk = append(fk, cacheIndexKey(index)...)

			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()
			keysToDelete := make([][]byte, 0, deletePrefixBatchSize)
			for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
				item := it.Item()
				key := item.KeyCopy(nil)
				keysToDelete = append(keysToDelete, key)
				if len(keysToDelete) >= deletePrefixBatchSize {
					err := s.deleteKeys(keysToDelete)
					if err != nil {
						return err
					}
					keysToDelete = make([][]byte, 0)
				}
			}
			err := s.deleteKeys(keysToDelete)
			if err != nil {
				return err
			}
			return nil
		})
	}
	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerDB) deleteCacheIndex(ctx context.Context, index uint16) error {
	return s.db.Update(func(txn *badger.Txn) error {
		mk := make([]byte, 0, 3)
		mk = append(mk, metaPrefix)
		mk = append(mk, cacheIndexKey(index)...)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		keysToDelete := make([][]byte, 0, deletePrefixBatchSize)
		for it.Seek(mk); it.ValidForPrefix(mk); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			keysToDelete = append(keysToDelete, key)
		}
		err := s.deleteKeys(keysToDelete)
		if err != nil {
			return err
		}
		return nil
	})
}

func (s *badgerDB) deletePrefixSingleFn(bucket string, index uint16, k []byte, fn ...SelectFn) func(txn *badger.Txn) error {
	return func(txn *badger.Txn) error {
		fk := buildFullKey(bucket, index, k)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		keysToDelete := make([][]byte, 0, deletePrefixBatchSize)
	OUTER:
		for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			// fmt.Printf("iter key: %x | %x | %x\n", key[:3], key[3:len(key)-12], key[len(key)-12:len(key)-8])
			for _, sfn := range fn {
				if !sfn(key[3:]) {
					continue OUTER // NOT GOTO !!!!!
				}
			}
			// fmt.Printf("iter key passed sfn")
			keysToDelete = append(keysToDelete, key)
			if len(keysToDelete) >= deletePrefixBatchSize {
				err := s.deleteKeys(keysToDelete)
				if err != nil {
					return err
				}
				keysToDelete = make([][]byte, 0)
			}
		}
		err := s.deleteKeys(keysToDelete)
		if err != nil {
			return err
		}
		return nil
	}
}

func (s *badgerDB) getAll(ctx context.Context, bucket string, index uint16, kvCh chan *KV, fn ...SelectFn) {
	defer close(kvCh)
	fk := buildFullKey(bucket, index, nil)
	stream := s.db.NewStream()
	stream.Prefix = fk
	// TODO: revisit with selectFn
	// stream.ChooseKey = func(item *badger.Item) bool {
	// 	return bytes.HasPrefix(item.Key(), fk)
	// }
	stream.Send = func(buf *z.Buffer) error {
		kvs, err := badger.BufferToKVList(buf)
		if err != nil {
			return err
		}
	OUTER:
		for _, kv := range kvs.GetKv() {
			for _, sfn := range fn {
				if !sfn(kv.GetKey()[3:]) {
					continue OUTER
				}
			}
			err = kvToChan(ctx, kv.GetKey()[3:], kv.GetValue(), kvCh)
			if err != nil {
				return err
			}
		}
		return nil
	}
	err := stream.Orchestrate(ctx)
	if err != nil {
		log.Errorf("stream orchestrate error: %v", err)
	}
}

func (s *badgerDB) getAllKeysOnly(ctx context.Context, bucket string, index uint16, kvCh chan *KV, fn ...SelectFn) {
	defer close(kvCh)
	fk := buildFullKey(bucket, index, nil)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.PrefetchSize = 0xFFFF
		it := txn.NewIterator(opts)
		defer it.Close()

	OUTER:
		for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
			key := it.Item().Key()
			for _, sfn := range fn {
				if !sfn(key[3:]) {
					continue OUTER
				}
			}
			err := kvToChan(ctx, key[3:], nil, kvCh)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Errorf("failed getAllKeysOnly: %v", err)
	}
}

func (s *badgerDB) getBatch(ctx context.Context, bucket string, index uint16, keys [][]byte, kvCh chan *KV, fn ...SelectFn) {
	defer close(kvCh)

	var err error
	switch bucket {
	case "config", "state":
		err = s.db.View(s.getBatchFromConfigStateStore(ctx, keys, bucket, index, kvCh, fn...))
		if err != nil {
			log.Errorf("failed read batch: %v", err)
		}
		return
	case "intended":
		err = s.db.View(s.getBatchFromIntendedStore(ctx, keys, bucket, index, kvCh, fn...))
		if err != nil {
			log.Errorf("failed read batch: %v", err)
		}
		return
	case "intents":
		err = s.db.View(func(tx *badger.Txn) error {
		NEXT_KEY:
			for _, k := range keys {
				for _, sfn := range fn {
					if !sfn(k) {
						continue NEXT_KEY
					}
				}
				fk := buildFullKey(bucket, index, k)

				item, err := tx.Get(fk)
				if err != nil {
					return err
				}
				v, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				err = kvToChan(ctx, k, v, kvCh)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			log.Errorf("failed read batch: %v", err)
		}
	}
}

// config/state stores batch fn
func (s *badgerDB) getBatchFromConfigStateStore(ctx context.Context, keys [][]byte, bucket string, index uint16, kvCh chan *KV, fn ...SelectFn) func(tx *badger.Txn) error {
	return func(tx *badger.Txn) error {
		for _, k := range keys {
			err := getBatchFromConfigStateSingleKey(ctx, tx, k, bucket, index, kvCh, fn...)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func getBatchFromConfigStateSingleKey(ctx context.Context, tx *badger.Txn, k []byte, bucket string, index uint16, kvCh chan *KV, fn ...SelectFn) error {
	prefix, pattern, err := keyToPrefixPattern(k)
	if err != nil {
		return err
	}
	withPattern := len(pattern) > 0
	var patternMatch *regexp.Regexp
	patternMatch, err = regexp.Compile(string(pattern))
	if err != nil {
		return err
	}

	fk := buildFullKey(bucket, index, prefix)
	lfk := len(fk)
	opts := badger.DefaultIteratorOptions
	it := tx.NewIterator(opts)
	defer it.Close()

	var v []byte
OUTER:
	for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
		item := it.Item()
		key := item.Key()

		if withPattern {
			if !patternMatch.Match(key[lfk:]) {
				continue OUTER
			}
		}
		for _, sfn := range fn {
			if !sfn(key[3:]) {
				continue OUTER
			}
		}
		v, err = item.ValueCopy(v)
		if err != nil {
			return err
		}
		err = kvToChan(ctx, key[3:], v, kvCh)
		if err != nil {
			return err
		}
	}
	return nil
}

// intended store batch fn
func (s *badgerDB) getBatchFromIntendedStore(ctx context.Context, keys [][]byte, bucket string, index uint16, kvCh chan *KV, fn ...SelectFn) func(tx *badger.Txn) error {
	return func(tx *badger.Txn) error {
		for _, k := range keys {
			err := getBatchIntendedSingleKey(ctx, tx, k, bucket, index, kvCh, fn...)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func getBatchIntendedSingleKey(ctx context.Context, tx *badger.Txn, k []byte, bucket string, index uint16, kvCh chan *KV, fn ...SelectFn) error {
	fk := buildFullKey(bucket, index, k)

	opts := badger.DefaultIteratorOptions
	it := tx.NewIterator(opts)
	defer it.Close()

	var v []byte
	var err error
	for it.Seek(fk); it.ValidForPrefix(fk); it.Next() {
		item := it.Item()
		key := item.Key()

		for _, sfn := range fn {
			if !sfn(key[3:]) {
				return nil
			}
		}
		v, err = item.ValueCopy(v)
		if err != nil {
			return err
		}
		err = kvToChan(ctx, key[3:], v, kvCh)
		if err != nil {
			return err
		}
	}
	return nil
}

// helps

func keyToPrefixPattern(k []byte) ([]byte, []byte, error) {
	k = replaceStarBetweenDelim(k)

	re, err := regexp.Compile(string(k))
	if err != nil {
		return nil, nil, err
	}
	prefix, all := re.LiteralPrefix()
	if all {
		return []byte(prefix), nil, nil
	}
	pattern := bytes.TrimPrefix(k, []byte(prefix))
	bprefix := bytes.TrimSuffix(k, pattern)
	return bprefix, pattern, nil
}

func replaceStarBetweenDelim(input []byte) []byte {
	// check if the key contains a '*'
	numStars := bytes.Count(input, []byte("*"))
	if numStars == 0 {
		return input
	}

	var result = make([]byte, 0, len(input)+numStars)

	for _, b := range input {
		if b == '*' {
			result = append(result, '.', '*')
			continue
		}
		result = append(result, b)
	}

	return result
}

func buildFullKey(bucket string, index uint16, k []byte) []byte {
	fk := make([]byte, 0, 1+2+len(k))
	fk = append(fk, bucketPrefix(bucket))    // 1
	fk = append(fk, cacheIndexKey(index)...) // 2
	fk = append(fk, k...)
	return fk
}

func cacheIndexKey(i uint16) []byte {
	return []byte{byte(i >> 8), byte(i & 0xFF)}
}

func bucketPrefix(bucket string) uint8 {
	switch bucket {
	case "config":
		return configPrefix
	case "state":
		return statePrefix
	case "intended":
		return intendedPrefix
	case "intents":
		return intentsPrefix
	}
	return 0
}
