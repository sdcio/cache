package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/ristretto/z"
	log "github.com/sirupsen/logrus"
)

var (
	metaPrefix     uint8 = 0
	configPrefix   uint8 = 1
	statePrefix    uint8 = 2
	intendedPrefix uint8 = 3
)

const (
	deletePrefixBatchSize = 10
)

type badgerDBStore struct {
	path string
	m    *sync.RWMutex
	dbs  map[string]*bdb
}

type bdb struct {
	db  *badger.DB
	cfn context.CancelFunc
}

func newBadgerDBStore(p string) Store {
	return &badgerDBStore{
		path: p,
		m:    new(sync.RWMutex),
		dbs:  map[string]*bdb{},
	}
}

func (s *badgerDBStore) CreateCache(ctx context.Context, name string, meta map[string]any, bucket ...string) error {
	bdb := &bdb{}
	dbCtx, cancel := context.WithCancel(context.Background())
	bdb.cfn = cancel

	var err error
	bdb.db, err = s.openDB(dbCtx, s.dbDirName(name))
	if err != nil {
		return err
	}
	// create buckets
	err = bdb.db.Update(func(tx *badger.Txn) error {
		// create cache config
		for _, kv := range metaToKV(meta) {
			// append meta prefix
			kv.K = append(kv.K, 0)
			copy(kv.K[1:], kv.K)
			kv.K[0] = metaPrefix
			err = tx.Set(kv.K, kv.V)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.m.Lock()
	defer s.m.Unlock()
	s.dbs[name] = bdb
	return nil
}

func (s *badgerDBStore) ListCaches(ctx context.Context) ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	caches := make([]string, 0, len(s.dbs))
	for n := range s.dbs {
		caches = append(caches, n)
	}
	sort.Strings(caches)
	return caches, nil
}

func (s *badgerDBStore) DeleteCache(ctx context.Context, name string) error {
	s.m.Lock()
	defer s.m.Unlock()
	db, ok := s.dbs[name]
	if ok {
		err := db.db.Close()
		if err != nil {
			log.Errorf("failed to close db: %v", err)
		}
		db.cfn()
	}
	delete(s.dbs, name)

	// delete file if it exists
	dbDirName := s.dbDirName(name)
	_, err := os.Stat(dbDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return os.RemoveAll(dbDirName)
}

func (s *badgerDBStore) Clone(ctx context.Context, name, cname string) error {
	cfg, err := s.GetMeta(ctx, name)
	if err != nil {
		return err
	}
	err = s.CreateCache(ctx, cname, cfg)
	if err != nil {
		return err
	}
	vCh, err := s.GetAll(ctx, name, "")
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
			err = s.WriteValue(ctx, cname, "", v.K, v.V)
			if err != nil {
				return err
			}
		}
	}
}

func (s *badgerDBStore) GetMeta(ctx context.Context, name string) (map[string]any, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}
	cfg := make(map[string]any)
	err := db.db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte{metaPrefix}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if string(item.Key()[1:]) == "cached" {
				item.Value(func(val []byte) error {
					cfg["cached"] = string(val) == "true"
					return nil
				})
			}
		}
		return nil
	})
	return cfg, err
}

func (s *badgerDBStore) LoadCache(ctx context.Context, name string) error {
	dbDirName := s.dbDirName(name)

	bdb := &bdb{}
	dbCtx, cancel := context.WithCancel(ctx)
	bdb.cfn = cancel

	var err error
	bdb.db, err = s.openDB(dbCtx, dbDirName)
	if err != nil {
		return err
	}
	s.m.Lock()
	s.dbs[name] = bdb
	s.m.Unlock()
	return nil
}

func (s *badgerDBStore) WriteValue(ctx context.Context, name, bucket string, k []byte, v []byte) error {
	k = append(k, 0)
	copy(k[1:], k)
	switch bucket {
	case "config":
		k[0] = configPrefix
	case "state":
		k[0] = statePrefix
	case "intended":
		k[0] = intendedPrefix
	default:
		return fmt.Errorf("unknown bucket name %q", bucket)
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return fmt.Errorf("unknown cache name %s", name)
	}

	err := db.db.Update(
		func(tx *badger.Txn) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return tx.Set(k, v)
			}
		})
	return err
}

func (s *badgerDBStore) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
	if bucket == "" {
		return nil, errors.New("a bucket name must be specified")
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	var v []byte
	err := db.db.View(func(tx *badger.Txn) error {
		k = append(k, 0)
		copy(k[1:], k)
		switch bucket {
		case "config":
			k[0] = configPrefix
		case "state":
			k[0] = statePrefix
		case "intended":
			k[0] = intendedPrefix
		}
		item, err := tx.Get(k)
		if err != nil {
			return err
		}
		v = make([]byte, 0, item.ValueSize())
		v, _ = item.ValueCopy(v)
		return nil
	})
	return v, err
}

func (s *badgerDBStore) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return fmt.Errorf("unknown cache name %s", name)
	}

	err := db.db.Update(func(tx *badger.Txn) error {
		k = append(k, 0)
		copy(k[1:], k)
		switch bucket {
		case "config":
			k[0] = configPrefix
		case "state":
			k[0] = statePrefix
		case "intended":
			k[0] = intendedPrefix
		}
		return tx.Delete(k)
	})
	return err
}

func (s *badgerDBStore) DeletePrefix(ctx context.Context, name, bucket string, k []byte, fn ...SelectFn) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return fmt.Errorf("unknown cache name %s", name)
	}

	err := db.db.Update(func(tx *badger.Txn) error {
		k = append(k, 0)
		copy(k[1:], k)
		switch bucket {
		case "config":
			k[0] = configPrefix
		case "state":
			k[0] = statePrefix
		case "intended":
			k[0] = intendedPrefix
		}
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := tx.NewIterator(opts)
		defer it.Close()
		keysToDelete := make([][]byte, 0)
	OUTER:
		for it.Seek(k); it.ValidForPrefix(k); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			for _, sfn := range fn {
				if !sfn(key[1:]) {
					continue OUTER // NOT GOTO !!!!!
				}
			}
			keysToDelete = append(keysToDelete, key)
			if len(keysToDelete) >= deletePrefixBatchSize {
				err := db.deleteKeys(keysToDelete)
				if err != nil {
					return err
				}
				keysToDelete = make([][]byte, 0)
			}
		}
		err := db.deleteKeys(keysToDelete)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}

func (s *badgerDBStore) GetAll(ctx context.Context, name, bucket string, fn ...SelectFn) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV)
	go func() {
		defer close(kvCh)
		stream := db.db.NewStream()
		stream.ChooseKey = func(item *badger.Item) bool {
			if bucket == "" {
				return item.Key()[0] != metaPrefix && item.Key()[0] != intendedPrefix
			}
			switch bucket {
			case "config":
				return item.Key()[0] == configPrefix
			case "state":
				return item.Key()[0] == statePrefix
			case "intended":
				return item.Key()[0] == intendedPrefix
			default:
				return false
			}
		}
		stream.Send = func(buf *z.Buffer) error {
			kvs, err := badger.BufferToKVList(buf)
			if err != nil {
				return err
			}
		OUTER:
			for _, kv := range kvs.GetKv() {
				for _, sfn := range fn {
					if !sfn(kv.GetKey()[1:]) {
						continue OUTER
					}
				}
				err = kvToChan(ctx, kv.GetKey()[1:], kv.GetValue(), kvCh)
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

func (s *badgerDBStore) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte, fn ...SelectFn) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV, 100) //TODO:
	go func() {
		defer close(kvCh)
		err := db.db.View(getPrefixFn(ctx, name, bucket, prefix, pattern, kvCh, fn...))
		if err != nil {
			log.Errorf("failed to read values based on prefix from cache %s: %v", name, err)
		}
	}()

	return kvCh, nil
}

func (s *badgerDBStore) Close() error {
	s.m.RLock()
	defer s.m.RUnlock()
	for _, db := range s.dbs {
		db.cfn()
		db.db.Close()
	}
	return nil
}

func (s *badgerDBStore) Stats(ctx context.Context, name string) (*StoreStats, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	count := len(s.dbs)
	ss := &StoreStats{
		NumCache:      count,
		KeysPerBucket: make(map[string]int64),
	}
	configKeyCount := int64(0)
	stateKeyCount := int64(0)
	intendedCount := int64(0)
	err := db.db.View(func(tx *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		iterOpts.PrefetchValues = false

		it := tx.NewIterator(iterOpts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				item := it.Item()
				key := item.Key()
				switch key[0] {
				case configPrefix:
					configKeyCount++
				case statePrefix:
					stateKeyCount++
				case intendedPrefix:
					intendedCount++
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to count cache %q keys: %v", name, err)
	}
	ss.KeysPerBucket["config"] = configKeyCount
	ss.KeysPerBucket["state"] = stateKeyCount
	ss.KeysPerBucket["intended"] = intendedCount
	return ss, nil
}

func (*badgerDBStore) openDB(ctx context.Context, name string) (*badger.DB, error) {
	opts := badger.DefaultOptions(name).
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
				log.Debugf("running GC for %s", name)
				err = bdb.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
				log.Debugf("GC for %s ended with err: %v", name, err)
			}
		}
	}()
	return bdb, nil
}

// badgerGetAllFn builds a function that can be passed to db.View()
// it reads all the KV from an index and writes the KV pair to the channel KvCh
// func badgerGetAllFn(ctx context.Context, indexName, bucket string, kvCh chan *KV) func(tx *badger.Txn) error {
// 	return func(tx *badger.Txn) error {
// 		it := tx.NewIterator(badger.DefaultIteratorOptions)
// 		defer it.Close()
// 		var k = make([]byte, 1)
// 		switch bucket {
// 		case "config":
// 			k[0] = configPrefix
// 		case "state":
// 			k[0] = statePrefix
// 		default:
// 			// get both config and state
// 			// iterate over keys
// 			for it.Rewind(); it.Valid(); it.Next() {
// 				select {
// 				case <-ctx.Done():
// 					return ctx.Err()
// 				default:
// 					item := it.Item()
// 					key := item.Key()
// 					if key[0] == 0 {
// 						continue // skip cache config
// 					}
// 					err := item.Value(func(v []byte) error {
// 						kvToChan(ctx, key[1:], v, kvCh)
// 						return nil
// 					})
// 					if err != nil {
// 						return err
// 					}
// 				}
// 			}
// 			return nil
// 		}
// 		// prefix scan
// 		for it.Seek(k); it.ValidForPrefix(k); it.Next() {
// 			item := it.Item()
// 			k := item.Key()
// 			if k[0] == 0 {
// 				continue // skip cache config
// 			}
// 			err := item.Value(func(v []byte) error {
// 				kvToChan(ctx, k[1:], v, kvCh)
// 				return nil
// 			})
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		return nil
// 	}
// }

// getPrefixFn
func getPrefixFn(ctx context.Context, indexName, bucket string, prefix, pattern []byte, kvCh chan *KV, fn ...SelectFn) func(tx *badger.Txn) error {
	return func(tx *badger.Txn) error {
		withPattern := len(pattern) > 0
		var re *regexp.Regexp
		var err error
		if withPattern {
			re, err = regexp.Compile(string(pattern))
			if err != nil {
				return err
			}
		}
		for _, pr := range getPrefixes(bucket, prefix) {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefixLen := len(pr)
		OUTER:
			for it.Seek(pr); it.ValidForPrefix(pr); it.Next() {
				item := it.Item()
				k := item.Key()
				if withPattern && !re.Match(k[1:]) {
					continue
				}
				if len(k) > prefixLen &&
					!bytes.HasPrefix(k[prefixLen:], []byte(",")) {
					continue
				}
				for _, sfn := range fn {
					if !sfn(k[1:]) {
						continue OUTER
					}
				}
				err := item.Value(func(v []byte) error {
					return kvToChan(ctx, k[1:], v, kvCh)
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func (s *badgerDBStore) dbDirName(cacheName string) string {
	return filepath.Join(s.path, cacheName)
}

func getPrefixes(bucket string, prefix []byte) [][]byte {
	prefix = append(prefix, 0)
	copy(prefix[1:], prefix)
	prefixes := make([][]byte, 0, 2)
	switch bucket {
	case "config":
		prefix[0] = configPrefix
		prefixes = append(prefixes, prefix)
	case "state":
		prefix[0] = statePrefix
		prefixes = append(prefixes, prefix)
	case "intended":
		prefix[0] = intendedPrefix
		prefixes = append(prefixes, prefix)
	default:
		lprefix := len(prefix)
		ck := make([]byte, lprefix)
		ck[0] = configPrefix
		copy(ck[1:], prefix[1:])

		sk := make([]byte, lprefix)
		sk[0] = statePrefix
		copy(sk[1:], prefix[1:])

		prefixes = [][]byte{ck, sk}
	}
	return prefixes
}

func (db *bdb) deleteKeys(keys [][]byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			fmt.Printf("deleting key: %x\n", key)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}