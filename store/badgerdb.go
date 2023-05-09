package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/dgraph-io/ristretto/z"
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
	db         *badger.DB
	cfn        context.CancelFunc
	writeCount atomic.Uint64
}

func newBadgerDBStore[T proto.Message](p string) Store[T] {
	return &badgerDBStore[T]{
		path: p,
		m:    new(sync.RWMutex),
		dbs:  map[string]*bdb{},
	}
}

func (s *badgerDBStore[T]) CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error {
	bdb := &bdb{
		writeCount: atomic.Uint64{},
	}
	dbCtx, cancel := context.WithCancel(context.Background())
	bdb.cfn = cancel

	var err error
	bdb.db, err = s.openDB(dbCtx, s.dbDirName(name))
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				log.Infof("%s write count %d", name, bdb.writeCount.Load())
			}
		}
	}()
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
	db, ok := s.dbs[name]
	if ok {
		err := db.db.Close()
		if err != nil {
			log.Errorf("failed to close db: %v", err)
		}
		db.cfn()
	}
	delete(s.dbs, name)
	s.m.Unlock()
	// delete file if it exists
	dbDirName := s.dbDirName(name)
	_, err := os.Stat(dbDirName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return os.RemoveAll(dbDirName)
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

func (s *badgerDBStore[T]) LoadCache(ctx context.Context, name string) error {
	dbDirName := s.dbDirName(name)

	bdb := &bdb{}
	dbCtx, cancel := context.WithCancel(context.Background())
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
	k = append(k, 0)
	copy(k[1:], k)
	switch bucket {
	case "config":
		k[0] = configPrefix
	case "state":
		k[0] = statePrefix
	default:
		return fmt.Errorf("unknown bucket name %q", bucket)
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return fmt.Errorf("unknown cache name %s", name)
	}

	err := db.db.Update(func(tx *badger.Txn) error {
		defer db.writeCount.Add(1)
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return tx.Set(k, v)
		}
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
		stream := db.db.NewStream()
		// stream.NumGo = 4 // TODO:
		stream.ChooseKey = func(item *badger.Item) bool {
			if bucket == "" {
				return item.Key()[0] != cacheConfigPrefix
			}
			switch bucket {
			case "config":
				return item.Key()[0] == configPrefix
			case "state":
				return item.Key()[0] == statePrefix
			default:
				return false
			}
		}
		stream.Send = func(buf *z.Buffer) error {
			kvs, err := badger.BufferToKVList(buf)
			if err != nil {
				return err
			}
			for _, kv := range kvs.GetKv() {
				kvToChan(ctx, kv.GetKey()[1:], kv.GetKey(), kvCh)
			}
			return nil
		}
	}()

	return kvCh, nil
}

func (s *badgerDBStore[T]) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV, 1000) //TODO:
	go func() {
		defer close(kvCh)
		err := db.db.View(badgerGetPrefixFn(ctx, name, bucket, prefix, pattern, kvCh))
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

func (s *badgerDBStore[T]) Stats(ctx context.Context, name string) (*StoreStats, error) {
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
	return ss, nil
}

func (s *badgerDBStore[T]) openDB(ctx context.Context, name string) (*badger.DB, error) {
	opts := badger.DefaultOptions(name).
		WithLoggingLevel(badger.WARNING).
		WithCompression(options.None)
		// .
		// WithBlockCacheSize(0)

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
				log.Infof("running GC for %s", name)
				err = bdb.RunValueLogGC(0.7)
				if err == nil {
					goto again
				}
				log.Infof("GC for %s ended with err: %v", name, err)
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
						kvToChan(ctx, key[1:], v, kvCh)
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
				kvToChan(ctx, k[1:], v, kvCh)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// badgerGetPrefixFn
func badgerGetPrefixFn(ctx context.Context, indexName, bucket string, prefix, pattern []byte, kvCh chan *KV) func(tx *badger.Txn) error {
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
			for it.Seek(pr); it.ValidForPrefix(pr); it.Next() {
				item := it.Item()
				k := item.Key()
				if withPattern && !re.Match(k[1:]) {
					continue
				}

				err := item.Value(func(v []byte) error {
					kvToChan(ctx, k[1:], v, kvCh)
					return nil
				})
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func (s *badgerDBStore[T]) dbDirName(cacheName string) string {
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
