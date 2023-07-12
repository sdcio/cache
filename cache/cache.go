package cache

import (
	"context"
	"sync"
	"time"

	"github.com/iptecharch/cache/config"
	"google.golang.org/protobuf/proto"
)

type Store int8

const (
	StoreConfig Store = 0
	StoreState  Store = 1
)

type Cache[T proto.Message] interface {
	// Initialize cache instances
	Init(ctx context.Context) error
	// List cache instances
	List(ctx context.Context) []string
	// Create a new cache instance
	Create(ctx context.Context, cfg *CacheInstanceConfig) error
	// GetDetails returns a cache instance details
	GetDetails(ctx context.Context, name string) (*CacheInstanceConfig, error)
	// Delete a cache instance or a candidate in a cache instance.
	// the name should be in the format $cache/$candidate to delete a candidate
	Delete(ctx context.Context, name string) error
	// Exists return true if a cache instance called 'name' exists,
	// false otherwise
	Exists(ctx context.Context, name string) bool
	// Clone a cache instance
	Clone(ctx context.Context, name, cname string) (string, error)
	// Create a candidate for an existing cache instance
	CreateCandidate(ctx context.Context, name, candidate string) (string, error)
	// Candidates returns the list of candidates created for a cache instance
	Candidates(ctx context.Context, name string) ([]string, error)
	// WriteValue writes a new value in a cache instance.
	// the value can be written into 2 different stores, CONFIG or STATE
	WriteValue(ctx context.Context, name string, store Store, p []string, v T) error
	// WriteBytesValue writes a bytes value into the named cache without unmarshaling
	// it into a proto.Message T first
	WriteBytesValue(ctx context.Context, name string, store Store, p []string, vb []byte) error
	// ReadValue reads a value from a cache instance.
	ReadValue(ctx context.Context, name string, store Store, p []string) (chan *Entry[T], error)
	// ReadValuePeriodic reads a value from a cache instance every period
	ReadValuePeriodic(ctx context.Context, name string, store Store, p []string, period time.Duration) (chan *Entry[T], error)
	// DeleteValue deletes a value from a cache instance.
	DeleteValue(ctx context.Context, name string, store Store, p []string) error
	// Diff returns the changes made to a candidate
	Diff(ctx context.Context, name, candidate string) ([][]string, []*Entry[T], error)
	// Discard drops the changes made to a candidate
	Discard(ctx context.Context, name, candidate string) error
	// Close the underlying resources, like the persistent store
	Close() error
	// Stats
	Stats(ctx context.Context, name string, withKeysCount bool) (*StatsResponse, error)
	NumInstances() int
}

type Entry[T proto.Message] struct {
	P []string
	V T
}

type StatsResponse struct {
	NumInstances  int
	InstanceStats map[string]*InstanceStats
}

type InstanceStats struct {
	Name     string
	KeyCount map[string]int64
}

func New[T proto.Message](cfg *config.CacheConfig, bfn func() T) Cache[T] {
	return &cache[T]{
		cfg:    cfg,
		m:      new(sync.RWMutex),
		caches: make(map[string]*cacheInstance[T]),
		bFn:    bfn,
	}
}
