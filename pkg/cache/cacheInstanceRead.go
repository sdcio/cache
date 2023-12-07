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
// it reads path ro.Path from the store.
// It also handles reading values from a candidate in case of a config store.
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
		pathsFoundInCandidate := make(map[string]struct{})
		// check if the path exists in the candidate
		if ro.Store == StoreConfig && cname != "" && cand != nil {
			err = cand.updates.Query(ro.Path,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					vt := val.([]byte)
					e := &Entry{
						P: path,
						V: vt,
					}
					rsCh <- e
					pathsFoundInCandidate[strings.Join(path, delimStr)] = struct{}{}
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
			err = ci.readPrefixFromConfigStoreCh(ctx, bucket, []byte(prefix), []byte(pattern), cand, pathsFoundInCandidate, rsCh)
		case StoreState:
			bucket = stateBucketName
			err = ci.readPrefixFromStateStoreCh(ctx, []byte(prefix), []byte(pattern), rsCh)
		case StoreIntended:
			bucket = intendedBucketName
			err = ci.readPrefixFromIntendedStoreCh(ctx, ro.Priority, ro.Owner, ro.MaxPriorities, []byte(prefix), rsCh)
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

func (ci *cacheInstance) readPrefixFromIntendedStoreCh(ctx context.Context, priority int32, owner string, mp uint64, prefix []byte, kvCh chan *Entry) error {
	switch {
	case priority < 0:
		return ci.readPrefixFromIntendedStoreAnyPrioCh(ctx, prefix, []byte(owner), kvCh)
	case priority == 0:
		return ci.readPrefixFromIntendedStoreHighPrioCh(ctx, prefix, []byte(owner), mp, kvCh)
	default: // specific priority
		return ci.readPrefixFromIntendedStoreSpecificPrioCh(ctx, priority, prefix, []byte(owner), kvCh)
	}
}

func (ci *cacheInstance) readPrefixFromIntendedStoreHighPrioCh(ctx context.Context, prefix, owner []byte, priorityCount uint64, kvCh chan *Entry) error {
	var bucket = "intended"
	withOwner := len(owner) > 0
	if priorityCount == 0 {
		priorityCount = 1
	}
	pp := int32(0)

	for iter := uint64(0); iter < priorityCount; iter++ {
		vCh, err := ci.store.GetAll(ctx, ci.cfg.Name, bucket,
			ignorePriorityHigherThanFn(pp),
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
				// check for owner
				if withOwner && !hasOwnerFn(k, owner) {
					return false
				}
				return true
			})
		if err != nil {
			return err
		}
		// this struct keeps track of the highest priority entries
		// per path.
		hpr := &highPriorityReader{
			highPrioEntries: map[string][]*Entry{},
			highestPriority: -1,
		}
	FOR:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case v, ok := <-vCh:
				if !ok {
					log.Debugf("current min priority %d", pp)
					log.Debugf("done with HPR %d", hpr.highestPriority)
					// found nothing
					if hpr.highestPriority < 0 {
						log.Debug("found nothing, stop iterations")
						return nil
					}
					// got the same priority as the previous iteration
					if pp == hpr.highestPriority {
						log.Debug("found the same priority as the previous iteration, stop iterations")
						return nil
					}
					for _, es := range hpr.highPrioEntries {
						for _, e := range es {
							log.Debugf("read entry: %s", e)
							select {
							case kvCh <- e:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
					}
					pp = hpr.highestPriority
					break FOR
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
				hpr.add(e)
			}
		}
	}
	return nil
}

func (ci *cacheInstance) readPrefixFromIntendedStoreAnyPrioCh(ctx context.Context, prefix, owner []byte, kvCh chan *Entry) error {
	var bucket = "intended"
	withOwner := len(owner) > 0
	prefixLen := len(prefix)
	// selectFn: selects keys with any priority that include the prefix
	vCh, err := ci.store.GetAll(ctx, ci.cfg.Name, bucket,
		func(k []byte) bool {
			lk := len(k)
			// must include priority(4) and timestamp(8)
			if lk < 4+8 {
				return false
			}
			// check for prefix
			// the key we are scanning has "prefix" as prefix and right after prefix there is a delimiter
			if !(bytes.HasPrefix(k[4:lk-8], prefix) &&
				bytes.HasPrefix(k[4+prefixLen:], delimBytes)) {
				return false
			}
			// any owner, done
			if !withOwner {
				return true
			}
			return lk > prefixLen && // key is longer than the prefix.
				hasOwnerFn(k, owner)

		})
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
			log.Debugf("reading prio=%x path=%s ts=%x", v.K[:4], string(v.K[4:len(v.K)-8]), v.K[len(v.K)-8:])
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

func (ci *cacheInstance) readPrefixFromIntendedStoreSpecificPrioCh(ctx context.Context, prio int32, prefix, owner []byte, kvCh chan *Entry) error {
	var bucket = "intended"
	withOwner := len(owner) > 0
	fprefix := make([]byte, 4+len(prefix))
	binary.BigEndian.PutUint32(fprefix, uint32(prio))
	copy(fprefix[4:], prefix)
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, bucket, fprefix, nil,
		func(k []byte) bool {
			lk := len(k)
			// must include priority and TS
			if lk < 4+8 {
				return false
			}
			if !withOwner {
				return true
			}
			return hasOwnerFn(k, owner)
		})
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

func hasOwnerFn(k, owner []byte) bool {
	lk := len(k)
	lo := len(owner)
	// the key ends with the owner bytes and 8 bytes for a timestamp.
	// and the bytes right before the owner are the delimiter bytes
	return bytes.HasSuffix(k[:lk-8], owner) &&
		bytes.HasPrefix(k[lk-8-lo-1:], delimBytes)
}

// used to ignore priorities higher than
func ignorePriorityHigherThanFn(p int32) store.SelectFn {
	return func(k []byte) bool {
		kp := binary.BigEndian.Uint32(k[:4])
		return int32(kp) > p
	}
}

type highPriorityReader struct {
	highPrioEntries map[string][]*Entry
	highestPriority int32
}

func (hpr *highPriorityReader) add(e *Entry) {
	switch {
	case hpr.highestPriority < 0:
		// first entry to be added
		hpr.highestPriority = e.Priority
		p := strings.Join(e.P, delimStr)
		hpr.highPrioEntries[p] = []*Entry{e}
	case hpr.highestPriority < e.Priority:
		return
	case hpr.highestPriority > e.Priority:
		// reset highest priority map entries
		// this case should not happen since we read from
		// the cache sequentially
		clear(hpr.highPrioEntries)
		// set new highest priority
		hpr.highestPriority = e.Priority
		// set the new first entry
		hpr.highPrioEntries[strings.Join(e.P, delimStr)] = []*Entry{e}
	case hpr.highestPriority == e.Priority:
		//
		p := strings.Join(e.P, delimStr)
		//
		if _, ok := hpr.highPrioEntries[p]; ok {
			hpr.highPrioEntries[p] = append(hpr.highPrioEntries[p], e)
			return
		}
		hpr.highPrioEntries[p] = []*Entry{e}
	}
}
