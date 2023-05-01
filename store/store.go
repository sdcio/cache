package store

import (
	"context"

	"google.golang.org/protobuf/proto"
)

type Store[T proto.Message] interface {
	CreateCache(ctx context.Context, name string, cfg map[string]any, bucket ...string) error
	ListCaches(ctx context.Context) ([]string, error)
	DeleteCache(ctx context.Context, name string) error
	Clone(ctx context.Context, name, cname string) error
	GetCacheConfig(ctx context.Context, name string) (map[string]any, error)
	// create cache from persistent storage, does not load KVs
	LoadCache(ctx context.Context, name string) error
	WriteValue(ctx context.Context, name, bucket string, k []byte, v T) error
	WriteBytesValue(ctx context.Context, name, bucket string, k []byte, v []byte) error
	GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error)
	DeleteValue(ctx context.Context, name, bucket string, k []byte) error
	GetAll(ctx context.Context, name, bucket string) (chan *KV, error)
	GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte) (chan *KV, error)
	Close() error
	Stats(ctx context.Context, name string) (*StoreStats, error)
}

type KV struct {
	K, V []byte
}

const (
	storeTypeBoltDB   = "boltdb"
	storeTypeBadgerDB = "badgerdb"
)

func New[T proto.Message](typ, p string) Store[T] {
	switch typ {
	case storeTypeBoltDB:
		return newBBoltDBStore[T](p)
	case storeTypeBadgerDB:
		return newBadgerDBStore[T](p)
	default:
		return newNoopStore[T]()
	}
}

type StoreStats struct {
	NumCache      int
	KeysPerBucket map[string]int64
}
