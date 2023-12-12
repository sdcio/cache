package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
)

type Store interface {
	CreateCache(ctx context.Context, name string) error
	ListCaches(ctx context.Context) ([]string, error)
	DeleteCache(ctx context.Context, name string) error

	SetPruneIndex(ctx context.Context, name string, idx uint8) error
	GetPruneIndex(ctx context.Context, name string) (uint8, error)
	Prune(ctx context.Context, name, bucket string, pruneIndex uint8) error

	Clone(ctx context.Context, name, cname string) error

	WriteValue(ctx context.Context, name, bucket string, k []byte, v []byte, m byte) error
	DeleteValue(ctx context.Context, name, bucket string, k []byte) error
	DeletePrefix(ctx context.Context, name, bucket string, k []byte, fn ...SelectFn) error
	Txn(ctx context.Context, name, bucket string, txnOpts *TxnOpts) error

	GetValue(ctx context.Context, name, bucket string, k []byte) ([]byte, error)
	GetAll(ctx context.Context, name, bucket string, keysOnly bool, fn ...SelectFn) (chan *KV, error)
	GetPrefix(ctx context.Context, name, bucket string, prefix, pattern []byte, fn ...SelectFn) (chan *KV, error)
	GetN(ctx context.Context, name, bucket string, n uint64, fn ...SelectFn) ([]*KV, error)
	Watch(ctx context.Context, name, bucket string, prefixes [][]byte) (chan *KV, error)

	Close() error
	Clear(ctx context.Context, name string) error

	Stats(ctx context.Context, name string) (*StoreStats, error)
}

type SelectFn func(k []byte) bool

type KV struct {
	K, V []byte
}

type DelOpts struct {
	K   []byte
	Fns []SelectFn
}

type TxnOpts struct {
	Updates []*KV
	Deletes []*DelOpts
}

func WithPrefix(prefix []byte) SelectFn {
	return func(k []byte) bool {
		return bytes.HasPrefix(prefix, k)
	}
}

const (
	storeTypeBadgerDB       = "badgerdb"
	storeTypeBadgerSingleDB = "badgerdbsingle"
)
const (
	metaPrefix       uint8 = 0
	configPrefix     uint8 = 1
	statePrefix      uint8 = 2
	intendedPrefix   uint8 = 3
	intentMetaPrefix uint8 = 4
)

const (
	deletePrefixBatchSize = 10
)

var sepBytes = []byte(",")

var (
	badgerTxnKey   = []byte("!badger!txn")
	ErrKeyNotFound = errors.New("KeyNotFound")
)

func New(typ, p string) (Store, error) {
	switch typ {
	case storeTypeBadgerDB:
		return newBadgerDBStore(p), nil
	case storeTypeBadgerSingleDB:
		return newBadgerSingleDBStore(p), nil
	default:
		return nil, fmt.Errorf("unknown store type %q", typ)
	}
}

type StoreStats struct {
	NumCache      int
	KeysPerBucket map[string]int64
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
