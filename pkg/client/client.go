// Copyright 2024 Nokia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/sdcio/cache/pkg/cache"
	"github.com/sdcio/cache/proto/cachepb"
)

const (
	defaultTimeout = 5 * time.Second
)

type Client struct {
	cfg *ClientConfig

	cc     *grpc.ClientConn
	client cachepb.CacheClient
}

type ClientConfig struct {
	// the cache server address
	Address string
	// number of read streams that can be opened concurrently
	// defaults to 1
	MaxReadStream int64
	// number of read streams that can be opened concurrently
	// defaults to 64
	MaxWatchStream int64
	// gRPC dial and unary RPCs timeout
	// defaults to 5s
	Timeout time.Duration
}

type ClientOpts struct {
	Store         cache.Store
	Owner         string
	Priority      int32
	PriorityCount uint64
	KeysOnly      bool
}

func New(ctx context.Context, ccfg *ClientConfig) (*Client, error) {
	if ccfg.Address == "" {
		return nil, fmt.Errorf("missing server address")
	}
	if ccfg.MaxReadStream <= 0 {
		ccfg.MaxReadStream = 1
	}
	if ccfg.MaxWatchStream <= 0 {
		ccfg.MaxWatchStream = 64
	}
	if ccfg.Timeout <= 0 {
		ccfg.Timeout = defaultTimeout
	}

	ctx, cancel := context.WithTimeout(ctx, ccfg.Timeout)
	defer cancel()

	cc, err := grpc.DialContext(ctx, ccfg.Address,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:    ccfg,
		client: cachepb.NewCacheClient(cc),
	}, nil
}

func (c *Client) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

// List cache instances
func (c *Client) List(ctx context.Context, opts ...grpc.CallOption) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	rsp, err := c.client.List(ctx, &cachepb.ListRequest{}, opts...)
	if err != nil {
		return nil, err
	}
	return rsp.GetCache(), nil
}

// Get a single cache details
func (c *Client) Get(ctx context.Context, name string, opts ...grpc.CallOption) (*cachepb.GetResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	rsp, err := c.client.Get(ctx, &cachepb.GetRequest{
		Name: name,
	}, opts...)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

// Create a new cache instance
func (c *Client) Create(ctx context.Context, name string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	_, err := c.client.Create(ctx,
		&cachepb.CreateRequest{
			Name: name,
		},
		opts...)
	return err
}

// Delete a cache instance
func (c *Client) Delete(ctx context.Context, name string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	_, err := c.client.Delete(ctx,
		&cachepb.DeleteRequest{
			Name: name},
		opts...)
	return err
}

func (c *Client) Exists(ctx context.Context, name string, opts ...grpc.CallOption) (bool, error) {
	rsp, err := c.client.Exists(ctx, &cachepb.ExistsRequest{
		Name: name,
	}, opts...)
	if err != nil {
		return false, err
	}
	return rsp.GetExists(), nil
}

// Create a Candidate
func (c *Client) CreateCandidate(ctx context.Context, name, candidate, owner string, p int32, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	_, err := c.client.CreateCandidate(ctx,
		&cachepb.CreateCandidateRequest{
			Name:      name,
			Candidate: candidate,
			Owner:     owner,
			Priority:  p,
		},
		opts...)
	return err
}

// Clone a cache
func (c *Client) Clone(ctx context.Context, name, clone string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	_, err := c.client.Clone(ctx,
		&cachepb.CloneRequest{
			Name:  name,
			Cname: clone,
		},
		opts...)
	return err
}

// modify a cache instance
func (c *Client) Modify(ctx context.Context, name string, wo *ClientOpts, dels [][]string, upds []*cachepb.Update, opts ...grpc.CallOption) error {
	stream, err := c.client.Modify(ctx, opts...)
	if err != nil {
		return err
	}
	var pbStore cachepb.Store
	switch wo.Store {
	case cache.StoreConfig:
		pbStore = cachepb.Store_CONFIG
	case cache.StoreState:
		pbStore = cachepb.Store_STATE
	case cache.StoreIntended:
		pbStore = cachepb.Store_INTENDED
	case cache.StoreIntents:
		pbStore = cachepb.Store_INTENTS
	}
	for _, del := range dels {
		err = stream.Send(&cachepb.ModifyRequest{
			Request: &cachepb.ModifyRequest_Delete{
				Delete: &cachepb.DeleteValueRequest{
					Name:     name,
					Path:     del,
					Store:    pbStore,
					Owner:    wo.Owner,
					Priority: wo.Priority,
				},
			},
		})
		if err != nil {
			return err
		}
	}

	for _, upd := range upds {
		err = stream.Send(
			&cachepb.ModifyRequest{
				Request: &cachepb.ModifyRequest_Write{
					Write: &cachepb.WriteValueRequest{
						Name:     name,
						Path:     upd.GetPath(),
						Value:    upd.GetValue(),
						Store:    pbStore,
						Owner:    wo.Owner,
						Priority: wo.Priority,
					},
				},
			})
		if err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	if strings.Contains(err.Error(), "EOF") {
		return nil
	}
	return err
}

// Read value(s) from a cache instance
func (c *Client) Read(ctx context.Context, name string, ro *ClientOpts, paths [][]string, period time.Duration, opts ...grpc.CallOption) chan *cachepb.ReadResponse {
	updCh := make(chan *cachepb.ReadResponse)

	go c.read(ctx, name, ro, paths, period, updCh, opts...)

	return updCh
}

// ReadKeys Read all the keys of the given store
func (c *Client) ReadKeys(ctx context.Context, name string, store cachepb.Store) (chan *cachepb.ReadResponse, error) {
	keyCh := make(chan *cachepb.ReadResponse)
	rkc, err := c.client.ReadKeys(ctx, &cachepb.ReadRequest{
		Name:     name,
		Store:    store,
		KeysOnly: true,
	})
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				rsp, err := rkc.Recv()
				if err != nil {
					if strings.Contains(err.Error(), "EOF") {
						break
					}
					log.Error("fail rcv", err)
					return
				}
				keyCh <- rsp
			}
		}
	}()
	return keyCh, nil
}

// GetChanges made to a candidate
func (c *Client) GetChanges(ctx context.Context, name, candidate string, opts ...grpc.CallOption) ([]*cachepb.GetChangesResponse, error) {
	stream, err := c.client.GetChanges(ctx, &cachepb.GetChangesRequest{
		Name:      name,
		Candidate: candidate,
	})
	if err != nil {
		return nil, err
	}
	result := make([]*cachepb.GetChangesResponse, 0)
	for {
		rsp, err := stream.Recv()
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				return result, nil
			}
			return nil, err
		}
		result = append(result, rsp)
	}
}

func (c *Client) Commit(ctx context.Context, name, candidate string, opts ...grpc.CallOption) error {
	_, err := c.client.Commit(ctx, &cachepb.CommitRequest{
		Name:      name,
		Candidate: candidate,
	}, opts...)
	return err
}

// Discard changes made to a candidate
func (c *Client) Discard(ctx context.Context, name, candidate string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	_, err := c.client.Discard(ctx, &cachepb.DiscardRequest{
		Name:      name,
		Candidate: candidate,
	}, opts...)
	return err
}

func (c *Client) Watch(ctx context.Context, name string, store cache.Store, prefixes [][]string, opts ...grpc.CallOption) (chan *cachepb.WatchResponse, error) {
	var cStore cachepb.Store
	switch store {
	case cache.StoreConfig:
		cStore = cachepb.Store_CONFIG
	case cache.StoreState:
		cStore = cachepb.Store_STATE
	case cache.StoreIntended:
		cStore = cachepb.Store_INTENDED
	}
	paths := make([]*cachepb.Path, 0, len(prefixes))
	for _, pr := range prefixes {
		paths = append(paths, &cachepb.Path{Elem: pr})
	}
	stream, err := c.client.Watch(ctx, &cachepb.WatchRequest{
		Name:  name,
		Store: cStore,
		Path:  paths,
	}, opts...)
	if err != nil {
		return nil, err
	}
	rspCh := make(chan *cachepb.WatchResponse)
	go func() {
		defer close(rspCh)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				rsp, err := stream.Recv()
				if err != nil {
					if strings.Contains(err.Error(), "EOF") {
						break
					}
					log.Error("fail rcv", err)
					return
				}
				rspCh <- rsp
			}
		}
	}()
	return rspCh, nil
}

func (c *Client) Prune(ctx context.Context, name string, id string, force bool, opts ...grpc.CallOption) (*cachepb.PruneResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	return c.client.Prune(ctx, &cachepb.PruneRequest{
		Name:  name,
		Id:    id,
		Force: force,
	}, opts...)
}

// helpers
func (c *Client) read(ctx context.Context, name string, ro *ClientOpts, paths [][]string, period time.Duration, updCh chan *cachepb.ReadResponse, opts ...grpc.CallOption) {
	defer close(updCh)
	var cStore cachepb.Store
	switch ro.Store {
	case cache.StoreConfig:
		cStore = cachepb.Store_CONFIG
	case cache.StoreState:
		cStore = cachepb.Store_STATE
	case cache.StoreIntended:
		cStore = cachepb.Store_INTENDED
	case cache.StoreIntents:
		cStore = cachepb.Store_INTENTS
	}
	cachePaths := make([]*cachepb.Path, 0, len(paths))
	for _, p := range paths {
		cachePaths = append(cachePaths, &cachepb.Path{Elem: p})
	}
	req := &cachepb.ReadRequest{
		Name:          name,
		Path:          cachePaths,
		Store:         cStore,
		Period:        uint64(period),
		Owner:         ro.Owner,
		Priority:      ro.Priority,
		PriorityCount: ro.PriorityCount,
		KeysOnly:      ro.KeysOnly,
	}
	stream, err := c.client.Read(ctx, req, opts...)
	if err != nil {
		log.Errorf("failed to create read stream: %v", err)
		return
	}
	for {
		rsp, err := stream.Recv()
		if err != nil {
			return
			// return nil, err
		}
		updCh <- rsp
	}
}
