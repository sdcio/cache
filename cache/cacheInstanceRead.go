package cache

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/iptecharch/schema-server/datastore/ctree"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

// readValue is the main reading method on cache instance
// it reads path p from the ctrees or from the store depending
// on the values of `cached` and `ephemeral`.
// It also handles reading values from a candidate
func (ci *cacheInstance[T]) readValue(ctx context.Context, cname string, store Store, p []string) ([]*Entry[T], error) {
	var f *candidate[T]
	var ok bool
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		f, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
		}
	}
	var err error
	es := make([]*Entry[T], 0)
	found := make(map[string]struct{})
	// check if the path exists in the candidate
	if cname != "" {
		err = f.updates.Query(p,
			func(path []string, _ *ctree.Leaf, val interface{}) error {
				vt := val.(T)
				e := &Entry[T]{
					P: path,
					V: vt,
				}
				es = append(es, e)
				found[strings.Join(path, ",")] = struct{}{}
				return nil
			})
		if err != nil {
			return nil, err
		}
	}

	if ci.cfg.Cached || ci.cfg.Ephemeral {
		// read from main cache
		return ci.readFromTrees(ctx, cname, store, p, f, found)
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
		return ci.readPrefixFromStore(ctx, bucket, []byte(prefix), []byte(pattern))
	}
	return nil, nil
}

// readValueCh is the same as readValue but returns a channel from which
// the entries can be read.
func (ci *cacheInstance[T]) readValueCh(ctx context.Context, cname string, store Store, p []string) (chan *Entry[T], error) {
	var f *candidate[T]
	var ok bool
	if cname != "" {
		ci.m.RLock()
		defer ci.m.RUnlock()
		f, ok = ci.candidates[cname]
		if !ok {
			return nil, fmt.Errorf("no such candidate %q in cache %q", cname, ci.cfg.Name)
		}
	}
	rsCh := make(chan *Entry[T])
	go func() {
		defer close(rsCh)
		var err error
		found := make(map[string]struct{})
		// check if the path exists in the candidate
		if cname != "" {
			err = f.updates.Query(p,
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
			ch, err := ci.readFromTreesCh(ctx, cname, store, p, f, found)
			if err != nil {
				log.Errorf("failed to read from trees: %v", err)
				return
			}
			for {
				select {
				case <-ctx.Done():
					return
				case e, ok := <-ch:
					if !ok {
						return
					}
					rsCh <- e
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
			err = ci.readPrefixFromStoreCh(ctx, bucket, []byte(prefix), []byte(pattern), rsCh)
			if err != nil {
				log.Errorf("failed to run query from store: %v", err)
			}
			return
		}
	}()
	return rsCh, nil
}

// readFromTrees is called when a cacheInstance is cached, it reads path `p` from the cacheInstance trees.
func (ci *cacheInstance[T]) readFromTrees(ctx context.Context, cname string, store Store, p []string, f *candidate[T], found map[string]struct{}) ([]*Entry[T], error) {
	es := make([]*Entry[T], 0)
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
	for _, tr := range trees {
		err := tr.Query(p,
			func(path []string, _ *ctree.Leaf, val interface{}) error {
				if cname != "" {
					// check if the path has been deleted
					f.m.RLock()
					_, ok := f.deletes[strings.Join(path, ",")]
					f.m.RUnlock()
					if ok {
						return nil
					}
					if _, ok := found[strings.Join(path, ",")]; ok {
						// path read from candidate, exit
						return nil
					}
				}
				// append value from main cache
				vt := val.(T)
				e := &Entry[T]{
					P: path,
					V: vt,
				}
				es = append(es, e)
				return nil
			},
		)
		if err != nil {
			return nil, err
		}
	}
	return es, nil
}

// readFromTreesCh is the same as readFromTrees but it writes the results into a channel.
func (ci *cacheInstance[T]) readFromTreesCh(ctx context.Context, cname string, store Store, p []string, f *candidate[T], found map[string]struct{}) (chan *Entry[T], error) {
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
	rsCh := make(chan *Entry[T])
	go func() {
		defer close(rsCh)
		for _, tr := range trees {
			err := tr.Query(p,
				func(path []string, _ *ctree.Leaf, val interface{}) error {
					if cname != "" {
						// check if the path has been deleted
						f.m.RLock()
						_, ok := f.deletes[strings.Join(path, ",")]
						f.m.RUnlock()
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
					rsCh <- e
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

func (ci *cacheInstance[T]) readPrefixFromStore(ctx context.Context, bucket string, prefix, pattern []byte) ([]*Entry[T], error) {
	vCh, err := ci.store.GetPrefix(ctx, ci.cfg.Name, bucket, prefix, pattern)
	if err != nil {
		return nil, err
	}
	es := make([]*Entry[T], 0)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case v, ok := <-vCh:
			if !ok {
				return es, nil
			}
			pv := ci.bFn()
			err = proto.Unmarshal(v.V, pv)
			if err != nil {
				return nil, err
			}
			entry := &Entry[T]{
				P: strings.Split(string(v.K), ","),
				V: pv,
			}
			es = append(es, entry)
		}
	}
}

func (ci *cacheInstance[T]) readPrefixFromStoreCh(ctx context.Context, bucket string, prefix, pattern []byte, kvCh chan *Entry[T]) error {
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
			pv := ci.bFn()
			err = proto.Unmarshal(v.V, pv)
			if err != nil {
				return err
			}
			entry := &Entry[T]{
				P: strings.Split(string(v.K), ","),
				V: pv,
			}
			kvCh <- entry
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
