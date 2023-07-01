package cache

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/iptecharch/data-server/datastore/ctree"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// readValue is the main reading method on cache instance
// it reads path p from the ctrees or from the store depending
// on the values of `cached` and `ephemeral`.
// It also handles reading values from a candidate
func (ci *cacheInstance[T]) readValueCh(ctx context.Context, cname string, store Store, p []string) (chan *Entry[T], error) {
	var cand *candidate[T]
	var ok bool
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		cand, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
		}
	}
	rsCh := make(chan *Entry[T])
	go func() {
		defer func() {
			close(rsCh)
			log.Debugf("read from %s/%s to channel done", ci.cfg.Name, cname)
		}()
		var err error
		found := make(map[string]struct{})
		// check if the path exists in the candidate
		if cname != "" {
			err = cand.updates.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					vt := val.(T)
					e := &Entry[T]{
						P: path,
						V: vt,
					}
					rsCh <- e
					found[strings.Join(path, ",")] = struct{}{}
					return nil
				})
			if err != nil {
				log.Errorf("failed to run query: %v", err)
			}
		}
		if ci.cfg.Cached || ci.cfg.Ephemeral {
			ch, err := ci.readFromTreesCh(ctx, cname, store, p, cand, found)
			if err != nil {
				log.Errorf("failed to read from trees: %v", err)
				return
			}
			for {
				select {
				case v, ok := <-ch:
					if !ok {
						return
					}
					rsCh <- v
				case <-ctx.Done():
					return
				}
			}
		}
		if !ci.cfg.Cached {
			prefix, pattern, _ := pathToPrefixPattern(p)
			bucket := ""
			switch store {
			case StoreConfig:
				bucket = "config"
			case StoreState:
				bucket = "state"
			}
			err = ci.readPrefixFromStoreCh(ctx, bucket, []byte(prefix), []byte(pattern), cand, found, rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
			return
		}
	}()
	return rsCh, nil
}

// readFromTreesCh is the same as readFromTrees but it writes the results into a channel.
func (ci *cacheInstance[T]) readFromTreesCh(ctx context.Context, cname string, store Store, p []string,
	cand *candidate[T], found map[string]struct{}) (chan *Entry[T], error) {
	rsCh := make(chan *Entry[T])
	trees := make([]*ctree.Tree, 0, 2)
	switch store {
	case StoreConfig:
		trees = append(trees, ci.config)
	case StoreState:
		trees = append(trees, ci.state)
	default:
		trees = append(trees, ci.config)
		trees = append(trees, ci.state)
	}
	go func() {
		defer close(rsCh)
		for _, tr := range trees {
			err := tr.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					if cname != "" {
						// check if the path has been deleted
						cand.m.RLock()
						_, ok := cand.deletes[strings.Join(path, ",")]
						cand.m.RUnlock()
						if ok {
							return nil
						}
						if _, ok := found[strings.Join(path, ",")]; ok {
							// path read from candidate, exit
							return nil
						}
					}
					// send value from main cache
					vt := val.(T)
					e := &Entry[T]{
						P: path,
						V: vt,
					}
					select {
					case rsCh <- e:
					case <-ctx.Done():
						return ctx.Err()
					}
					return nil
				},
			)
			if err != nil {
				log.Errorf("failed to run query from trees: %v", err)
			}
		}
	}()

	return rsCh, nil
}

func (ci *cacheInstance[T]) readPrefixFromStoreCh(ctx context.Context, bucket string, prefix, pattern []byte,
	cand *candidate[T],
	found map[string]struct{}, kvCh chan *Entry[T]) error {
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
				// fmt.Printf("read %d\n", count)
				return nil
			}
			// path
			ep := string(v.K)
			if cand != nil {
				// check if the path was found
				// in the candidate
				if _, ok := found[ep]; ok {
					continue
				}
				// check if the path has been deleted
				// from the candidate
				cand.m.RLock()
				_, ok = cand.deletes[ep]
				cand.m.RUnlock()
				if ok {
					return nil
				}
			}
			pv := ci.bFn()
			err = proto.Unmarshal(v.V, pv)
			if err != nil {
				return err
			}
			entry := &Entry[T]{
				P: strings.Split(ep, ","),
				V: pv,
			}
			select {
			case kvCh <- entry:
				count++
			case <-ctx.Done():
				return ctx.Err()
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
	fp := strings.Join(path, ",")
	re, err := regexp.Compile(fp)
	if err != nil {
		return "", "", err
	}
	prefix, all := re.LiteralPrefix()
	if all {
		return prefix, "", nil
	}
	pattern := strings.TrimPrefix(fp, prefix)
	prefix = strings.TrimSuffix(prefix, ",")
	return prefix, pattern, nil
}
