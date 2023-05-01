package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

type bboltStore[T proto.Message] struct {
	path string
	m    *sync.RWMutex
	dbs  map[string]*bolt.DB
}

func newBBoltDBStore[T proto.Message](p string) Store[T] {
	return &bboltStore[T]{
		path: p,
		m:    new(sync.RWMutex),
		dbs:  map[string]*bolt.DB{},
	}
}

func (s *bboltStore[T]) CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error {
	dbFileName := s.dbFileName(name)
	fileDir := filepath.Dir(dbFileName)
	// create file dir if it doesn't exist
	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		err = os.MkdirAll(fileDir, 0700)
		if err != nil {
			return err
		}
	}

	bdb, err := s.openDB(dbFileName)
	if err != nil {
		return err
	}

	// create buckets
	err = bdb.Update(func(tx *bolt.Tx) error {
		// create cache_config bucket
		cb, err := tx.CreateBucket([]byte("__" + name))
		if err != nil {
			return err
		}
		for _, kv := range cfgToKV(cfg) {
			err = cb.Put(kv.K, kv.V)
			if err != nil {
				return err
			}
		}

		for _, bk := range bucket {
			_, err = tx.CreateBucket([]byte(bk))
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

func (s *bboltStore[T]) ListCaches(ctx context.Context) ([]string, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	caches := make([]string, 0, len(s.dbs))
	for n := range s.dbs {
		caches = append(caches, n)
	}
	sort.Strings(caches)
	return caches, nil
}

func (s *bboltStore[T]) DeleteCache(ctx context.Context, name string) error {
	s.m.Lock()
	defer s.m.Lock()
	db, ok := s.dbs[name]
	if ok {
		db.Close()
	}
	// delete file if it exists
	dbFileName := s.dbFileName(name)
	if _, err := os.Stat(dbFileName); os.IsNotExist(err) {
		return nil
	}
	return os.Remove(dbFileName)
}

func (s *bboltStore[T]) GetCacheConfig(ctx context.Context, name string) (map[string]any, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}
	cfg := make(map[string]any)
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("__" + name))
		if b == nil {
			return fmt.Errorf("bucket __%s does not exist", name)
		}
		err := b.ForEach(func(k, v []byte) error {
			// TODO: find a better schema
			if string(k) == "cached" {
				cfg[string(k)] = string(v) == "true"
			}
			return nil
		})
		return err
	})
	return cfg, err
}

func (s *bboltStore[T]) Clone(ctx context.Context, name, cname string) error {
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

func (s *bboltStore[T]) LoadCache(ctx context.Context, name string) error {
	dbFileName := s.dbFileName(name)
	db, err := s.openDB(dbFileName)
	if err != nil {
		return err
	}
	s.m.Lock()
	s.dbs[name] = db
	s.m.Unlock()
	return nil
}

func (s *bboltStore[T]) WriteValue(ctx context.Context, name, bucket string, k []byte, v T) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}
	vb, err := proto.Marshal(v)
	if err != nil {
		return err
	}
	return s.WriteBytesValue(ctx, name, bucket, k, vb)
}

func (s *bboltStore[T]) WriteBytesValue(ctx context.Context, name, bucket string, k []byte, v []byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return fmt.Errorf("unknown cache name %s", name)
	}
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s not found", bucket)
		}
		return b.Put(k, v)
	})
	return err
}

func (s *bboltStore[T]) GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error) {
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
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}
		bv := b.Get(k)
		v = make([]byte, 0, len(bv))
		copy(v, bv)
		return nil
	})
	return v, err
}

func (s *bboltStore[T]) DeleteValue(ctx context.Context, name, bucket string, k []byte) error {
	if bucket == "" {
		return errors.New("a bucket name must be specified")
	}
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return fmt.Errorf("unknown cache name %s", name)
	}

	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return fmt.Errorf("bucket %s does not exist", bucket)
		}
		return b.Delete(k)
	})
	return err
}

func (s *bboltStore[T]) GetAll(ctx context.Context, name, bucket string) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV)
	go func() {
		defer close(kvCh)
		err := db.View(boltDBGetAllFn(name, bucket, kvCh))
		if err != nil {
			log.Errorf("failed to read all values from cache/bucket %s/%s: %v", name, bucket, err)
		}
	}()

	return kvCh, nil
}

func (s *bboltStore[T]) GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte) (chan *KV, error) {
	s.m.RLock()
	defer s.m.RUnlock()

	db, ok := s.dbs[name]
	if !ok {
		return nil, fmt.Errorf("unknown cache name %s", name)
	}

	kvCh := make(chan *KV)
	go func() {
		defer close(kvCh)
		err := db.View(boltDBGetPrefixFn(name, bucket, prefix, kvCh))
		if err != nil {
			log.Errorf("failed to read values based on prefix from cache %s: %v", name, err)
		}
	}()

	return kvCh, nil
}

func (s *bboltStore[T]) Close() error {
	s.m.RLock()
	defer s.m.RUnlock()
	for _, db := range s.dbs {
		db.Close()
	}
	return nil
}

func (s *bboltStore[T]) Stats(ctx context.Context, name string) (*StoreStats, error) { return nil, nil }

func (s *bboltStore[T]) openDB(name string) (*bolt.DB, error) {
	bdb, err := bolt.Open(name, 0644, &bolt.Options{
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}
	return bdb, nil
}

// TODO: needs a context for cancellation

// boltDBGetAllFn builds a function that can be passed to db.View()
// it reads all the KV from an index and writes the KV pair to the channel KvCh
func boltDBGetAllFn(indexName, bucket string, kvCh chan *KV) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		// specific bucket
		if bucket != "" {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return fmt.Errorf("bucket %s does not exist", bucket)
			}
			err := b.ForEach(func(k, v []byte) error {
				kvToChan(k, v, kvCh)
				return nil
			})
			return err
		}
		// no bucket specified
		// loop through buckets
		err := tx.ForEach(
			func(name []byte, b *bolt.Bucket) error {
				// skip bucket if the name does not match
				if strings.HasPrefix(string(name), "__") {
					return nil
				}
				err := b.ForEach(
					func(k, v []byte) error {
						kvToChan(k, v, kvCh)
						return nil
					})
				return err
			})
		return err
	}
}

// TODO: needs a context for cancellation
func boltDBGetPrefixFn(indexName, bucket string, prefix []byte, kvCh chan *KV) func(tx *bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		// specific bucket
		if bucket != "" {
			b := tx.Bucket([]byte(bucket))
			if b == nil {
				return fmt.Errorf("bucket %s does not exist", bucket)
			}

			c := b.Cursor()
			for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
				kvToChan(k, v, kvCh)
			}
			return nil
		}
		// no bucket specified
		// loop through buckets
		err := tx.ForEach(
			func(name []byte, b *bolt.Bucket) error {
				// skip bucket if it's the store config bucket
				if strings.HasPrefix(string(name), "__") {
					return nil
				}
				c := b.Cursor()
				for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
					kvToChan(k, v, kvCh)
				}
				return nil
			})
		return err
	}
}

func kvToChan(k, v []byte, kvCh chan *KV) {
	kb := make([]byte, len(k))
	vb := make([]byte, len(v))
	copy(kb, k)
	copy(vb, v)
	kvCh <- &KV{
		K: kb,
		V: vb,
	}
}

func (s *bboltStore[T]) dbFileName(cacheName string) string {
	cachePath := filepath.Join(s.path, cacheName)
	return cachePath + "/" + cacheName + ".db"
}

func cfgToKV(cfg map[string]any) []*KV {
	rs := make([]*KV, 0, len(cfg))
	for k, v := range cfg {
		kv := &KV{
			K: []byte(k),
		}
		switch v := v.(type) {
		case string:
			kv.V = []byte(v)
		case bool:
			kv.V = []byte(fmt.Sprintf("%t", v))
		case uint8:
			kv.V = []byte{v}
		case uint16:
			kv.V = make([]byte, 2)
			binary.BigEndian.PutUint16(kv.V, v)
		case uint32:
			kv.V = make([]byte, 4)
			binary.BigEndian.PutUint32(kv.V, v)
		case uint64:
			kv.V = make([]byte, 8)
			binary.BigEndian.PutUint64(kv.V, v)
		}
		rs = append(rs, kv)
	}
	return rs
}
