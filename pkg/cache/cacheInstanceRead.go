package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/cache/pkg/ctree"
	"github.com/iptecharch/cache/pkg/store"
)

// readValueCh is the main reading method on cache instance
// it reads paths ro.Path from the store.
// It also handles reading values from a candidate in case of a config store.
func (ci *cacheInstance) readValueCh(ctx context.Context, cname string, ro *Opts) (chan *Entry, error) {
	rsCh := make(chan *Entry)
	var err error
	switch ro.Store {
	case StoreConfig:
		var cand *candidate
		var ok bool
		if cname != "" {
			ci.m.RLock()
			defer ci.m.RUnlock()
			cand, ok = ci.candidates[cname]
			if !ok {
				return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
			}
		}
		go func() {
			err = ci.readFromConfigStore(ctx, cand, ro, rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	case StoreState:
		go func() {
			err = ci.readFromStateStore(ctx, ro, rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	case StoreIntended:
		go func() {
			err = ci.readFromIntendedStore(ctx, ro, rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	case StoreMetadata:
		go func() {
			err = ci.readFromMetadataStore(ctx, ro, rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
		}()
	default:
		return nil, fmt.Errorf("unknown store type %d", ro.Store)
	}

	return rsCh, nil
}

func (ci *cacheInstance) readFromConfigStore(ctx context.Context, cand *candidate, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)

	var err error
	pathsFoundInCandidate := make(map[string]struct{})

	// read from candidate first
	if cand != nil {
		for _, p := range ro.Path {
			err = cand.updates.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					vt := val.([]byte)
					e := &Entry{
						P: path,
						V: vt,
					}
					select {
					case <-ctx.Done():
						return ctx.Err()
					case rsCh <- e:
					}
					pathsFoundInCandidate[strings.Join(path, delimStr)] = struct{}{}
					return nil
				})
			if err != nil {
				log.Errorf("failed to run query: %v", err)
				return err
			}
		}
	}
	// convert paths to keys ([]byte)
	keys := make([][]byte, 0, len(ro.Path))
	for _, p := range ro.Path {
		keys = append(keys, []byte(strings.Join(p, delimStr)))
	}

	// read from config(main)
	return ci.readPrefixFromConfigStoreCh(ctx, keys, cand, pathsFoundInCandidate, rsCh)
}

func (ci *cacheInstance) readFromStateStore(ctx context.Context, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)

	keys := make([][]byte, 0, len(ro.Path))
	for _, p := range ro.Path {
		keys = append(keys, []byte(strings.Join(p, delimStr)))
	}

	return ci.readPrefixFromStateStoreCh(ctx, keys, rsCh)
}

func (ci *cacheInstance) readFromIntendedStore(ctx context.Context, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)

	log.Debugf("reading from intended store: %+v", ro)
	// prepare keys
	keys := make([][]byte, 0, len(ro.Path))
	for _, p := range ro.Path {
		keys = append(keys, buildIntendedStoreReadeKey(p, ro.Priority, ro.Owner))
	}
	sfn := []store.SelectFn{}
	switch {
	case ro.Priority > 0: // read specific priority
	case ro.Priority == 0: // read highest priority(ies) (priority count per path)
		sfn = append(sfn, highestPrioritiesSelectFn(int32(ro.PriorityCount)))
	case ro.Priority < 0: // read all priorities for all paths
	}
	//
	vCh, err := ci.store.GetBatch(ctx, ci.cfg.Name, intendedBucketName, keys, sfn...)
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
			lk := len(v.K)
			if lk < 12 {
				log.Errorf("GetBatch returned key is too short (<12): %x", v.K)
				continue
			}

			log.Debugf("reading path=%s | priority=%x | ts=%x\n",
				string(v.K[:lk-12]), v.K[lk-12:lk-8], v.K[lk-8:])
			e, err := kvToEntry(v, intendedBucketName)
			if err != nil {
				return err
			}

			select {
			case rsCh <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readFromMetadataStore(ctx context.Context, ro *Opts, rsCh chan *Entry) error {
	defer close(rsCh)
	for _, p := range ro.Path {
		prefix, _, _ := pathToPrefixPattern(p)
		err := ci.readFromMetadataStoreCh(ctx, []byte(prefix), ro.KeysOnly, rsCh)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ci *cacheInstance) readPrefixFromConfigStoreCh(ctx context.Context, keys [][]byte,
	cand *candidate,
	found map[string]struct{}, kvCh chan *Entry) error {
	bucket := configBucketName
	vCh, err := ci.store.GetBatch(ctx, ci.cfg.Name, bucket, keys)
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
			// path
			ep := string(v.K)
			if cand != nil {
				// check if the path was found
				// in the candidate so we don't
				// return it again
				if _, ok := found[ep]; ok {
					continue
				}
				// check if the path has been deleted
				// from the candidate
				cand.m.RLock()
				_, ok = cand.deletes[ep]
				cand.m.RUnlock()
				if ok {
					continue
				}
			}

			e, _ := kvToEntry(v, bucket)

			select {
			case kvCh <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readPrefixFromStateStoreCh(ctx context.Context, keys [][]byte, kvCh chan *Entry) error {
	var bucket = "state"
	vCh, err := ci.store.GetBatch(ctx, ci.cfg.Name, bucket, keys)
	if err != nil {
		return err
	}
	count := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				return nil
			}
			e, _ := kvToEntry(v, bucket)

			select {
			case kvCh <- e:
				count++
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readValueFromIntendedStoreHighPrioCh(ctx context.Context, p []byte) (*Entry, error) {
	var bucket = "intended"
	kvs, err := ci.store.GetN(ctx, ci.cfg.Name, bucket, 1,
		func(k []byte) bool {
			lk := len(k)
			// must include priority and TS
			if lk < 4+8 {
				return false
			}
			// check for prefix
			return bytes.HasPrefix(k[4:lk-8], p)
		})
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, store.ErrKeyNotFound
	}
	e, err := kvToEntry(kvs[0], bucket)
	return e, err
}

func (ci *cacheInstance) readFromMetadataStoreCh(ctx context.Context, key []byte, keysOnly bool, kvCh chan *Entry) error {
	if keysOnly {
		return ci.readFromMetadataStoreKeysOnlyCh(ctx, key, kvCh)
	}
	var bucket = "metadata"
	v, err := ci.store.GetValue(ctx, ci.cfg.Name, bucket, key)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case kvCh <- &Entry{
		P: []string{string(key)},
		V: v,
	}:
	}
	return nil
}

func (ci *cacheInstance) readFromMetadataStoreKeysOnlyCh(ctx context.Context, key []byte, kvCh chan *Entry) error {
	var bucket = "metadata"
	sCh, err := ci.store.GetAll(ctx, ci.cfg.Name, bucket, true)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case kv, ok := <-sCh:
			if !ok {
				return nil
			}
			kvCh <- &Entry{
				P: []string{string(kv.K)},
			}
		}
	}
}

func pathToPrefixPattern(path []string) (string, string, error) {
	for i := range path {
		if path[i] == "*" {
			path[i] = ".*"
		}
	}
	fp := strings.Join(path, delimStr)
	re, err := regexp.Compile(fp)
	if err != nil {
		return "", "", err
	}
	prefix, all := re.LiteralPrefix()
	if all {
		return prefix, "", nil
	}
	pattern := strings.TrimPrefix(fp, prefix)
	prefix = strings.TrimSuffix(prefix, delimStr)
	return prefix, pattern, nil
}

func kvToEntry(v *store.KV, bucket string) (*Entry, error) {
	switch bucket {
	case "config", "state":
		return &Entry{
			P: strings.Split(string(v.K), delimStr),
			V: v.V,
		}, nil
	case "intended":
		return parseIntendedStoreKey(v.K, v.V)
	}
	return nil, fmt.Errorf("unknown bucket name %q", bucket)
}

func buildIntendedStoreReadeKey(path []string, priority int32, owner string) []byte {
	// ignore owner if the priority is 0
	if priority == 0 {
		owner = ""
	}
	path = append(path, owner)
	k := []byte(strings.Join(path, delimStr))
	if priority <= 0 {
		return k // trying to read all priorities
	}

	priob := make([]byte, 4)
	binary.BigEndian.PutUint32(priob, uint32(priority))

	k = append(k, priob...)
	return k
}

func highestPrioritiesSelectFn(priorityCount int32) store.SelectFn {
	// This mutex and map are used
	// to keep track of the number of
	// encountered values per key (i.e path).
	m := new(sync.Mutex)
	readPaths := make(map[string]int32)

	return func(k []byte) bool {
		e, err := parseIntendedStoreKey(k, nil)
		if err != nil {
			return false
		}
		path := strings.Join(e.P, delimStr)

		// lock readPaths map
		m.Lock()
		defer m.Unlock()
		// if the key was read before
		// check if we reached priorityCount for this key.
		if count, ok := readPaths[path]; ok {
			// check how many priorities where read
			if count >= priorityCount {
				// if the priorityCount was reached, skip it.
				return false
			}
			// else: increment the count and return true
			readPaths[path]++
			return true
		}
		// if the key is encountered for the first time,
		// add the count item to the map and
		// return true
		readPaths[path] = 1
		return true
	}
}

func parseIntendedStoreKey(k, v []byte) (*Entry, error) {
	lkey := len(k)
	if lkey < 4+8 {
		return nil, fmt.Errorf("key too short(<12): %x", k)
	}
	// timestamp is the last 8 bytes
	ts := k[lkey-8:]
	// priority is the 4 bytes before the timestamp
	pr := k[lkey-12 : lkey-8]
	// the key items are the path elements + the owner
	keyItems := bytes.Split(k[:lkey-12], delimBytes)
	numItems := len(keyItems)
	e := &Entry{
		Timestamp: binary.BigEndian.Uint64(ts),
		Owner:     string(keyItems[numItems-1]), // owner is the last path elem
		Priority:  int32(binary.BigEndian.Uint32(pr)),
		P:         make([]string, 0, numItems-1),
		V:         v,
	}
	// add the path elements to the entry to be returned
	for _, ki := range keyItems[:numItems-1] {
		e.P = append(e.P, string(ki))
	}
	return e, nil
}
