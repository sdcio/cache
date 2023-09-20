package client

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iptecharch/cache/pkg/cache"
	"github.com/iptecharch/cache/proto/cachepb"
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
	// gRPC dial and unary RPCs timeout
	// defaults to 5s
	Timeout time.Duration
}

type ClientOpts struct {
	Store    cache.Store
	Owner    string
	Priority int32
}

func New(ctx context.Context, ccfg *ClientConfig) (*Client, error) {
	if ccfg.Address == "" {
		return nil, fmt.Errorf("missing server address")
	}
	if ccfg.MaxReadStream <= 0 {
		ccfg.MaxReadStream = 1
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
	go func() {
		defer close(updCh)
		wg := new(sync.WaitGroup)
		sem := semaphore.NewWeighted(c.cfg.MaxReadStream)
		var cStore cachepb.Store
		switch ro.Store {
		case cache.StoreConfig:
			cStore = cachepb.Store_CONFIG
		case cache.StoreState:
			cStore = cachepb.Store_STATE
		case cache.StoreIntended:
			cStore = cachepb.Store_INTENDED
		}
		for _, p := range paths {
			wg.Add(1)
			go func(p []string) {
				defer wg.Done()
				err := sem.Acquire(ctx, 1)
				if err != nil {
					return
				}
				defer sem.Release(1)
				req := &cachepb.ReadRequest{
					Name:     name,
					Path:     p,
					Store:    cStore,
					Period:   uint64(period),
					Owner:    ro.Owner,
					Priority: ro.Priority,
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
			}(p)
		}
		wg.Wait()
	}()
	return updCh
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

func (c *Client) Stats(ctx context.Context, name string, withKeyCount bool, opts ...grpc.CallOption) (*cachepb.StatsResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	return c.client.Stats(ctx, &cachepb.StatsRequest{
		Name:      name,
		KeysCount: withKeyCount,
	}, opts...)
}
