package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/z"
	log "github.com/sirupsen/logrus"
)

const (
	dbName             = "cached"
	metaSinglePrefix   = uint8(0)
	cachesSinglePrefix = uint8(1)
)

type badgerSingleDBStore struct {
	path         string
	cfn          context.CancelFunc
	m            *sync.RWMutex
	cacheIndexes map[string]uint16
	cacheNames   map[uint16]string
	db           *badger.DB
}

func newBadgerSingleDBStore(p string) Store {
	if p == "" {
		p = dbName
	}
	s := &badgerSingleDBStore{
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
		prefix := []byte{metaSinglePrefix}
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			k := it.Item().KeyCopy(nil)
			if len(k) < 3 {
				log.Warnf("found short key(<3) in caches store: %x", k)
				continue
			}
			log.Debugf("loading meta key: %x | %x | %s\n", k[0], k[1:3], string(k[3:]))
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

func (s *badgerSingleDBStore) CreateCache(ctx context.Context, name string) error {
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
	err := s.writeCacheIndex(ctx, index, name)
	if err != nil {
		return err
	}
	s.cacheIndexes[name] = index
	s.cacheNames[index] = name
	return nil
}

func (s *badgerSingleDBStore) ListCaches(ctx context.Context) ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	caches := make([]string, 0, len(s.cacheIndexes))
	for k := range s.cacheIndexes {
		caches = append(caches, k)
	}
	sort.Strings(caches)
	return caches, nil
}

func (s *badgerSingleDBStore) DeleteCache(ctx context.Context, name string) error {
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
		return nil
	}
	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerSingleDBStore) Clone(ctx context.Context, name, cname string) error {
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

func (s *badgerSingleDBStore) WriteValue(ctx context.Context, name, bucket string, k []byte, v []byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		fk := buildFullKey(bucket, index, k)
		return s.db.Update(func(txn *badger.Txn) error {
			return txn.Set(fk, v)
		})
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerSingleDBStore) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
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
				// map not found error
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

func (s *badgerSingleDBStore) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		fk := buildFullKey(bucket, index, k)
		fmt.Printf("deleting %x | %x | %s\n", fk[0], fk[1:3], fk[3:])
		return s.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(fk)
		})
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerSingleDBStore) DeletePrefix(ctx context.Context, name, bucket string, k []byte, fn ...SelectFn) error {
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

func (s *badgerSingleDBStore) GetAll(ctx context.Context, name, bucket string, fn ...SelectFn) (chan *KV, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		kvCh := make(chan *KV)
		go func() {
			defer close(kvCh)
			fk := buildFullKey(bucket, index, nil)
			stream := s.db.NewStream()
			stream.Prefix = fk
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

		}()
		return kvCh, nil
	}
	return nil, fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerSingleDBStore) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte, fn ...SelectFn) (chan *KV, error) {
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

func (s *badgerSingleDBStore) GetN(ctx context.Context, name, bucket string, n uint64, fn ...SelectFn) ([]*KV, error) {
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

func (s *badgerSingleDBStore) Txn(ctx context.Context, name, bucket string, txnOpts *TxnOpts) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}

	s.m.RLock()
	defer s.m.RUnlock()

	if index, ok := s.cacheIndexes[name]; ok {
		return s.db.Update(func(txn *badger.Txn) error {
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
	}

	return fmt.Errorf("cache %q does not exist", name)
}

func (s *badgerSingleDBStore) Watch(ctx context.Context, name, bucket string, prefixes [][]byte) (chan *KV, error) {
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

func (s *badgerSingleDBStore) Close() error {
	s.cfn()
	s.db.Close()
	return nil
}

func (s *badgerSingleDBStore) Stats(ctx context.Context, name string) (*StoreStats, error) {
	return nil, nil
}

func (s *badgerSingleDBStore) Clear(ctx context.Context, name string) error {
	s.m.RLock()
	defer s.m.RUnlock()

	return s.clear(ctx, name)
}

// helpers

func (s *badgerSingleDBStore) openDB(ctx context.Context) (*badger.DB, error) {
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

func (s *badgerSingleDBStore) deleteKeys(keys [][]byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *badgerSingleDBStore) nextAvailableKey() uint16 {
	for i := uint16(0); i < ^uint16(0); i++ { // ^uint16(0) gives the maximum value of uint16
		if _, exists := s.cacheNames[i]; !exists {
			return i
		}
	}
	return ^uint16(0)
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
	}
	return 0
}

func buildFullKey(bucket string, index uint16, k []byte) []byte {
	fk := make([]byte, 0, 1+2+1+len(k))
	fk = append(fk, bucketPrefix(bucket))    // 1
	fk = append(fk, cacheIndexKey(index)...) // 2
	fk = append(fk, k...)
	return fk
}

func (s *badgerSingleDBStore) deletePrefixSingleFn(bucket string, index uint16, k []byte, fn ...SelectFn) func(txn *badger.Txn) error {
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
			for _, sfn := range fn {
				if !sfn(key[1:]) {
					continue OUTER // NOT GOTO !!!!!
				}
			}
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

func (s *badgerSingleDBStore) clear(ctx context.Context, name string) error {
	if index, ok := s.cacheIndexes[name]; ok {
		return s.db.Update(func(txn *badger.Txn) error {
			fk := make([]byte, 1, 3)
			fk[0] = metaSinglePrefix
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

func (s *badgerSingleDBStore) deleteCacheIndex(ctx context.Context, index uint16) error {
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

func (s *badgerSingleDBStore) writeCacheIndex(ctx context.Context, index uint16, name string) error {
	return s.db.Update(func(txn *badger.Txn) error {
		mk := make([]byte, 0, 3)
		mk = append(mk, metaPrefix)
		mk = append(mk, cacheIndexKey(index)...)
		bn := []byte(name)
		mk = append(mk, bn...)
		return txn.Set(mk, bn)
	})
}
