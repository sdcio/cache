package cache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/iptecharch/cache/pkg/config"
)

type Store int8

const (
	StoreConfig   Store = 0
	StoreState    Store = 1
	StoreIntended Store = 2
	StoreMetadata Store = 3
)

type Cache interface {
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
	CreateCandidate(ctx context.Context, name, candidate, owner string, priority int32) (string, error)
	// GetCandidate returns the candidate details; owner and priority
	GetCandidate(ctx context.Context, name, cname string) (*CandidateDetails, error)
	// Candidates returns the list of candidates created for a cache instance
	Candidates(ctx context.Context, name string) ([]*CandidateDetails, error)
	// WriteValue writes a bytes value into the named cache
	WriteValue(ctx context.Context, name string, wo *Opts, vb []byte) error
	// CreatePruneID creates a pruneID that can be used to trigger a prune
	// with ApplyPrune once all updates are pushed
	CreatePruneID(ctx context.Context, name string, force bool) (string, error)
	// ApplyPrune runs a prune on the config and state stores of the cache instance.
	// It deletes all values that where not updated since the pruneID was generated.
	ApplyPrune(ctx context.Context, name, id string) error
	// ReadValue reads a value from a cache instance.
	ReadValue(ctx context.Context, name string, ro *Opts) (chan *Entry, error)
	// ReadValuePeriodic reads a value from a cache instance every period
	ReadValuePeriodic(ctx context.Context, name string, ro *Opts, period time.Duration) (chan *Entry, error)
	// DeleteValue deletes a value from a cache instance.
	DeleteValue(ctx context.Context, name string, wo *Opts) error
	// DeletePrefix deletes any key/value with the given prefix
	DeletePrefix(ctx context.Context, name string, wo *Opts) error
	// Diff returns the changes made to a candidate
	Diff(ctx context.Context, name, candidate string) ([][]string, []*Entry, error)
	// Discard drops the changes made to a candidate
	Discard(ctx context.Context, name, candidate string) error
	// Close the underlying resources, like the persistent store
	Close() error
	// Commit candidate into the intended store
	Commit(ctx context.Context, name, candidate string) error
	// Stats
	Stats(ctx context.Context, name string, withKeysCount bool) (*StatsResponse, error)
	Clear(ctx context.Context, name string) error
	NumInstances() int
	Watch(ctx context.Context, name string, store Store, prefixes [][]string) (chan *Entry, error)
}

type Entry struct {
	Timestamp uint64
	Owner     string
	Priority  int32
	P         []string
	V         []byte
}

func (e *Entry) String() string {
	return fmt.Sprintf("ts=%d, owner=%s, priority=%d, path=%v, value=%s",
		e.Timestamp, e.Owner, e.Priority, e.P, e.V)
}

type CandidateDetails struct {
	CacheName     string
	CandidateName string
	Owner         string
	Priority      int32
}

type Opts struct {
	Store         Store
	Path          []string
	Owner         string
	Priority      int32
	PriorityCount uint64
	KeysOnly      bool // used with the metadata store only
}

type StatsResponse struct {
	NumInstances  int
	InstanceStats map[string]*InstanceStats
}

type InstanceStats struct {
	Name     string
	KeyCount map[string]int64
}

func New(cfg *config.CacheConfig) Cache {
	return &cache{
		cfg:    cfg,
		m:      new(sync.RWMutex),
		caches: make(map[string]*cacheInstance),
	}
}
