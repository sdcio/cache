package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var (
	cacheConfigPrefix uint8 = 0
	configPrefix      uint8 = 1
	statePrefix       uint8 = 2
)

type badgerDBStore[T proto.Message] struct {
	path string
	m    *sync.RWMutex
	dbs  map[string]*bdb
}

type bdb struct {
	db  *badger.DB
	cfn context.CancelFunc
}

func newBadgerDBStore[T proto.Message](p string) Store[T] {
	return &badgerDBStore[T]{
		path: p,
		m:    new(sync.RWMutex),
		dbs:  map[string]*bdb{},
	}
}

func (s *badgerDBStore[T]) CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error {
	dbFileName := s.dbFileName(name)
	fileDir := filepath.Dir(dbFileName)
	// create file dir if it doesn't exist
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		err = os.MkdirAll(fileDir, 0700)
		if err != nil {
			return err
		}
	}
	bdb := &bdb{}
	dbCtx, cancel := context.WithCancel(context.Background())
	bdb.cfn = cancel

	var err error
	bdb.db, err = s.openDB(dbCtx, dbFileName)
	if err != nil {
		return err
	}

	// create buckets
	err = bdb.db.Update(func(tx *badger.Txn) error {
		// create cache config
		for _, kv := range cfgToKV(cfg) {
			// append cache config prefix
			kv.K = append(kv.K, 0)
			copy(kv.K[1:], kv.K)
			kv.K[0] = cacheConfigPrefix
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

func (s *badgerDBStore[T]) ListCaches(ctx context.Context) ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	caches := make([]string, 0, len(s.dbs))
	for n := range s.dbs {
		caches = append(caches, n)
	}
	sort.Strings(caches)
	return caches, nil
}

func (s *badgerDBStore[T]) DeleteCache(ctx context.Context, name string) error {
	s.m.Lock()
	defer s.m.Lock()
	db, ok := s.dbs[name]
	if ok {
		db.db.Close()
		db.cfn()
	}
	// delete file if it exists
	dbFileName := s.dbFileName(name)
	if _, err := os.Stat(dbFileName); os.IsNotExist(err) {
		return nil
	}
	return os.Remove(dbFileName)
}

func (s *badgerDBStore[T]) Clone(ctx context.Context, name, cname string) error {
	cfg, err := s.GetCacheConfig(ctx, name)
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
			err = s.WriteBytesValue(ctx, cname, "", v.K, v.V)
			if err != nil {
				return err
			}
		}
	}
}

func (s *badgerDBStore[T]) GetCacheConfig(ctx context.Context, name string) (map[string]any, error) {
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

		prefix := []byte{cacheConfigPrefix}
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

func (s *badgerDBStore[T]) SyncCache(ctx context.Context, name string) error { return nil }

func (s *badgerDBStore[T]) LoadCache(ctx context.Context, name string) error {
	dbFileName := s.dbFileName(name)

	bdb := &bdb{}
	dbCtx, cancel := context.WithCancel(context.Background())
	bdb.cfn = cancel

	var err error
	bdb.db, err = s.openDB(dbCtx, dbFileName)
	if err != nil {
		return err
	}
	s.m.Lock()
	s.dbs[name] = bdb
	s.m.Unlock()
	return nil
}

func (s *badgerDBStore[T]) WriteValue(ctx context.Context, name, bucket string, k []byte, v T) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}
	vb, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return s.WriteBytesValue(ctx, name, bucket, k, vb)
}

func (s *badgerDBStore[T]) WriteBytesValue(ctx context.Context, name, bucket string, k []byte, v []byte) error {
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
		}
		return tx.Set(k, v)
	})
	return err
}

func (s *badgerDBStore[T]) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
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

func (s *badgerDBStore[T]) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
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
		}
		return tx.Delete(k)
	})
	return err
}

func (s *badgerDBStore[T]) GetAll(ctx context.Context, name, bucket string) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV)
	go func() {
		defer close(kvCh)
		err := db.db.View(badgerGetAllFn(ctx, name, bucket, kvCh))
		if err != nil {
			log.Errorf("failed to read all values from cache/bucket %s/%s: %v", name, bucket, err)
		}
	}()

	return kvCh, nil
}

func (s *badgerDBStore[T]) GetPrefix(ctx context.Context, name, bucket string, prefix []byte) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV)
	go func() {
		defer close(kvCh)
		err := db.db.View(badgerGetPrefixFn(ctx, name, bucket, prefix, kvCh))
		if err != nil {
			log.Errorf("failed to read values based on prefix from cache %s: %v", name, err)
		}
	}()

	return kvCh, nil
}

func (s *badgerDBStore[T]) Close() error {
	s.m.RLock()
	defer s.m.RUnlock()
	for _, db := range s.dbs {
		db.cfn()
		db.db.Close()
	}
	return nil
}

func (s *badgerDBStore[T]) openDB(ctx context.Context, name string) (*badger.DB, error) {
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
				err := bdb.RunValueLogGC(0.7)
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
func badgerGetAllFn(ctx context.Context, indexName, bucket string, kvCh chan *KV) func(tx *badger.Txn) error {
	return func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		var k = make([]byte, 1)
		switch bucket {
		case "config":
			k[0] = configPrefix
		case "state":
			k[0] = statePrefix
		default:
			// get both config and state
			// iterate over keys
			for it.Rewind(); it.Valid(); it.Next() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:

					item := it.Item()
					key := item.Key()
					if key[0] == 0 {
						continue // skip cache config
					}
					err := item.Value(func(v []byte) error {
						kvToChan(key[1:], v, kvCh)
						return nil
					})
					if err != nil {
						return err
					}
				}
			}
			return nil
		}
		// prefix scan
		for it.Seek(k); it.ValidForPrefix(k); it.Next() {
			item := it.Item()
			k := item.Key()
			if k[0] == 0 {
				continue // skip cache config
			}
			err := item.Value(func(v []byte) error {
				kvToChan(k[1:], v, kvCh)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func badgerGetPrefixFn(ctx context.Context, indexName, bucket string, prefix []byte, kvCh chan *KV) func(tx *badger.Txn) error {
	return func(tx *badger.Txn) error {
		k := []byte(prefix)
		k = append(k, 0)
		copy(k[1:], k)
		prefixes := make([][]byte, 0, 2)
		switch bucket {
		case "config":
			k[0] = configPrefix
			prefixes = append(prefixes, k)
		case "state":
			k[0] = statePrefix
			prefixes = append(prefixes, k)
		default:
			prefixes = append(prefixes, append([]byte{configPrefix}, k[1:]...))
			prefixes = append(prefixes, append([]byte{statePrefix}, k[1:]...))
		}
		for _, prefix := range prefixes {
			it := tx.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					item := it.Item()
					k := item.Key()
					err := item.Value(func(v []byte) error {
						kvToChan(k[1:], v, kvCh)
						return nil
					})
					if err != nil {
						return err
					}
				}
			}
		}
		return nil
	}
}

func (s *badgerDBStore[T]) dbFileName(cacheName string) string {
	cachePath := filepath.Join(s.path, cacheName)
	return cachePath + "/" + cacheName + ".db"
}
