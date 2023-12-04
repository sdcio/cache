package cache

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/iptecharch/cache/pkg/ctree"
	"github.com/iptecharch/cache/pkg/store"
)

// readValue is the main reading method on cache instance
// it reads path p from the store.
// It also handles reading values from a candidate
func (ci *cacheInstance) readValueCh(ctx context.Context, cname string, ro *Opts) (chan *Entry, error) {
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
	rsCh := make(chan *Entry)
	go func() {
		defer func() {
			close(rsCh)
			log.Debugf("read from %s/%s to channel done", ci.cfg.Name, cname)
		}()
		var err error
		found := make(map[string]struct{})
		// check if the path exists in the candidate
		if ro.Store == StoreConfig && cname != "" {
			err = cand.updates.Query(ro.Path,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					vt := val.([]byte)
					e := &Entry{
						P: path,
						V: vt,
					}
					rsCh <- e
					found[strings.Join(path, delimStr)] = struct{}{}
					return nil
				})
			if err != nil {
				log.Errorf("failed to run query: %v", err)
			}
		}
		prefix, pattern, _ := pathToPrefixPattern(ro.Path)
		bucket := ""
		switch ro.Store {
		case StoreConfig:
			bucket = configBucketName
			err = ci.readPrefixFromConfigStoreCh(ctx, bucket, []byte(prefix), []byte(pattern), cand, found, rsCh)
		case StoreState:
			bucket = stateBucketName
			err = ci.readPrefixFromStateStoreCh(ctx, []byte(prefix), []byte(pattern), rsCh)
		case StoreIntended:
			bucket = intendedBucketName
			err = ci.readPrefixFromIntendedStoreCh(ctx, ro.Priority, ro.Owner, []byte(prefix), rsCh)
		case StoreMetadata:
			bucket = intendedBucketName
			err = ci.readFromMetadataStoreCh(ctx, []byte(prefix), rsCh)
		}
		if err != nil {
			log.Errorf("failed to run query from store: %v", err)
		}
	}()
	return rsCh, nil
}

func (ci *cacheInstance) readPrefixFromConfigStoreCh(ctx context.Context, bucket string, prefix, pattern []byte,
	cand *candidate,
	found map[string]struct{}, kvCh chan *Entry) error {
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, bucket, prefix, pattern)
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

func (ci *cacheInstance) readPrefixFromStateStoreCh(ctx context.Context, prefix, pattern []byte, kvCh chan *Entry) error {
	var bucket = "state"
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, bucket, prefix, pattern)
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

func (ci *cacheInstance) readPrefixFromIntendedStoreCh(ctx context.Context, prio int32, owner string, prefix []byte, kvCh chan *Entry) error {
	var bucket = "intended"
	var err error
	var vCh chan *store.KV
	ownerB := []byte(owner)
	prefixLen := len(prefix)
	switch {
	case prio < 0:
		// selectFn: selects keys with any priority that include the prefix
		vCh, err = ci.store.GetAll(ctx, ci.cfg.Name, bucket,
			func(k []byte) bool {
				lk := len(k)
				// must include priority(4) and timestamp(8)
				if lk < 4+8 {
					return false
				}
				// check for prefix
				// any owner
				if owner == "" {
					// the key we are scanning has "prefix" as prefix and right after prefix there is a delimiter
					return bytes.HasPrefix(k[4:lk-8], prefix) &&
						bytes.HasPrefix(k[4+prefixLen:], delimBytes)
				}
				return lk > prefixLen && // key is longer than the prefix.
					bytes.HasPrefix(k[4:lk-8], prefix) && // the key after priority has "prefix" as prefix.
					bytes.HasPrefix(k[4+prefixLen:], delimBytes) && // right after prefix there is a delimiter.
					bytes.HasSuffix(k[:lk-8], ownerB) // the key ends with the owner and 8 bytes for a timestamp.
			})
	case prio == 0:
		return ci.readPrefixFromIntendedStoreHighPrioCh(ctx, prefix, ownerB, kvCh)
	default:
		fprefix := make([]byte, 4+len(prefix))
		binary.BigEndian.PutUint32(fprefix, uint32(prio))
		copy(fprefix[4:], prefix)
		vCh, err = ci.store.GetPrefix(ctx, ci.cfg.Name, bucket, fprefix, nil,
			func(k []byte) bool {
				lk := len(k)
				// must include priority and TS
				if lk < 4+8 {
					return false
				}
				if owner == "" {
					return true
				}
				return hasOwner(k, ownerB)
			})
	}
	if err != nil {
		return err
	}
	// specific priority or all of them
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				return nil
			}
			log.Debugf("reading prio=%x path=%s ts=%x\n", v.K[:4], string(v.K[4:len(v.K)-8]), v.K[len(v.K)-8:])
			e, err := kvToEntry(v, bucket)
			if err != nil {
				return err
			}

			select {
			case kvCh <- e:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (ci *cacheInstance) readPrefixFromIntendedStoreHighPrioCh(ctx context.Context, prefix, owner []byte, kvCh chan *Entry) error {
	var bucket = "intended"
	withOwner := len(owner) > 0
	vCh, err := ci.store.GetAll(ctx, ci.cfg.Name, bucket,
		func(k []byte) bool {
			lk := len(k)
			// must include priority and TS
			if lk < 4+8 {
				return false
			}
			// check for prefix
			if !bytes.HasPrefix(k[4:lk-8], prefix) {
				return false
			}
			// check for prefix
			if !withOwner {
				return true
			}
			// check for owner and prefix
			return hasOwner(k, owner)

		})
	if err != nil {
		return err
	}
	highPrioKVs := make(map[string]*Entry)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				for _, e := range highPrioKVs {
					select {
					case kvCh <- e:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			}
			// extract entry attrs
			lkey := len(v.K)
			if lkey < 4+8 {
				return fmt.Errorf("key too short(<12): %x", v.K)
			}
			ts := v.K[lkey-8:]

			keyItems := bytes.Split(v.K[4:lkey-8], delimBytes)
			numItems := len(keyItems)

			e := &Entry{
				Timestamp: binary.BigEndian.Uint64(ts),
				Owner:     string(keyItems[numItems-1]),
				Priority:  int32(binary.BigEndian.Uint32(v.K[:4])),
				P:         make([]string, 0, numItems-1),
				V:         v.V,
			}

			for _, ki := range keyItems[:numItems-1] {
				e.P = append(e.P, string(ki))
			}
			if ee, ok := highPrioKVs[strings.Join(e.P, delimStr)]; ok {
				if e.Priority < ee.Priority {
					highPrioKVs[strings.Join(e.P, delimStr)] = e
					continue
				}
				if e.Priority == ee.Priority {
					if e.Timestamp > ee.Timestamp {
						highPrioKVs[strings.Join(e.P, delimStr)] = e
						continue
					}
				}
				continue
			}
			highPrioKVs[strings.Join(e.P, delimStr)] = e
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

func (ci *cacheInstance) readFromMetadataStoreCh(ctx context.Context, key []byte, kvCh chan *Entry) error {
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
		lkey := len(v.K)
		if lkey < 4+8 {
			return nil, fmt.Errorf("key too short(<12): %x", v.K)
		}
		ts := v.K[lkey-8:]

		keyItems := bytes.Split(v.K[4:lkey-8], delimBytes)
		numItems := len(keyItems)

		e := &Entry{
			Timestamp: binary.BigEndian.Uint64(ts),
			Owner:     string(keyItems[numItems-1]),
			Priority:  int32(binary.BigEndian.Uint32(v.K[:4])),
			P:         make([]string, 0, numItems-1),
			V:         v.V,
		}

		for _, ki := range keyItems[:numItems-1] {
			e.P = append(e.P, string(ki))
		}
		return e, nil
	}
	return nil, fmt.Errorf("unknown bucket name %q", bucket)
}

func hasOwner(k, owner []byte) bool {
	return bytes.HasSuffix(k[:len(k)-8], owner)
}
