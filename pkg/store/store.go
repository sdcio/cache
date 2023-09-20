package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
)

type Store interface {
	CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error
	ListCaches(ctx context.Context) ([]string, error)
	DeleteCache(ctx context.Context, name string) error

	Clone(ctx context.Context, name, cname string) error
	GetMeta(ctx context.Context, name string) (map[string]any, error)
	LoadCache(ctx context.Context, name string) error

	WriteValue(ctx context.Context, name, bucket string, k []byte, v []byte) error

	DeleteValue(ctx context.Context, name, bucket string, k []byte) error
	DeletePrefix(ctx context.Context, name, bucket string, k []byte, fn ...SelectFn) error

	GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error)
	GetAll(ctx context.Context, name, bucket string, fn ...SelectFn) (chan *KV, error)
	GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte, fn ...SelectFn) (chan *KV, error)

	Close() error
	Stats(ctx context.Context, name string) (*StoreStats, error)
}

type SelectFn func(k []byte) bool

type KV struct {
	K, V []byte
}

func WithPrefix(prefix []byte) SelectFn {
	return func(k []byte) bool {
		return bytes.HasPrefix(prefix, k)
	}
}

const (
	storeTypeBadgerDB = "badgerdb"
)

func New(typ, p string) Store {
	switch typ {
	case storeTypeBadgerDB:
		return newBadgerDBStore(p)
	default:
		return newNoopStore()
	}
}

type StoreStats struct {
	NumCache      int
	KeysPerBucket map[string]int64
}

func metaToKV(meta map[string]any) []*KV {
	rs := make([]*KV, 0, len(meta))
	for k, v := range meta {
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

func kvToChan(ctx context.Context, k, v []byte, kvCh chan *KV) error {
	kb := make([]byte, len(k))
	vb := make([]byte, len(v))
	copy(kb, k)
	copy(vb, v)
	kv := &KV{
		K: kb,
		V: vb,
	}
	select {
	case kvCh <- kv:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
