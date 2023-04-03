package client

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/iptecharch/cache/proto/cachepb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	cfg    *ClientConfig
	client cachepb.CacheClient
}
type ClientConfig struct {
	Address       string
	MaxReadStream int64
}

func New(ccfg *ClientConfig) (*Client, error) {
	if ccfg.MaxReadStream <= 0 {
		ccfg.MaxReadStream = 1
	}
	cc, err := grpc.Dial(ccfg.Address,
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

// List cache instances
func (c *Client) List(ctx context.Context, opts ...grpc.CallOption) ([]string, error) {
	rsp, err := c.client.List(ctx, &cachepb.ListRequest{}, opts...)
	if err != nil {
		return nil, err
	}
	return rsp.GetCache(), nil
}

// Get a single cache details
func (c *Client) Get(ctx context.Context, name string, opts ...grpc.CallOption) (*cachepb.GetResponse, error) {
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
	_, err := c.client.Create(ctx,
		&cachepb.CreateRequest{
			Name: name},
		opts...)
	return err
}

// Delete a cache instance
func (c *Client) Delete(ctx context.Context, name string, opts ...grpc.CallOption) error {
	_, err := c.client.Delete(ctx,
		&cachepb.DeleteRequest{
			Name: name},
		opts...)
	return err
}

// Create a Candidate
func (c *Client) CreateCandidate(ctx context.Context, name, candidate string, opts ...grpc.CallOption) error {
	_, err := c.client.CreateCandidate(ctx,
		&cachepb.CreateCandidateRequest{
			Name:      name,
			Candidate: candidate,
		},
		opts...)
	return err
}

// Clone a cache
func (c *Client) Clone(ctx context.Context, name, clone string, opts ...grpc.CallOption) error {
	_, err := c.client.Clone(ctx,
		&cachepb.CloneRequest{
			Name:  name,
			Cname: clone,
		},
		opts...)
	return err
}

// modify a cache instance
func (c *Client) Modify(ctx context.Context, name string, store cachepb.Store, dels [][]string, upds []*cachepb.Update, opts ...grpc.CallOption) error {
	stream, err := c.client.Modify(ctx, opts...)
	if err != nil {
		return err
	}
	errs := make([]error, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			rsp, err := stream.Recv()
			if err != nil {
				if strings.Contains(err.Error(), "EOF") {
					return
				}
				errs = append(errs, err)
				return
			}
			switch rsp := rsp.Response.(type) {
			case *cachepb.ModifyResponse_Write:
				if rsp.Write.Error != "" {
					errs = append(errs, fmt.Errorf("%v", rsp.Write.Error))
				}
			case *cachepb.ModifyResponse_Delete:
				if rsp.Delete.Error != "" {
					errs = append(errs, fmt.Errorf("%v", rsp.Delete.Error))
				}
			}
		}
	}()

	for _, del := range dels {
		err = stream.Send(&cachepb.ModifyRequest{
			Request: &cachepb.ModifyRequest_Delete{
				Delete: &cachepb.DeleteValueRequest{
					Name:  name,
					Path:  del,
					Store: store,
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
						Name:  name,
						Path:  upd.GetPath(),
						Value: upd.GetValue(),
						Store: store,
					},
				},
			})
		if err != nil {
			return err
		}
	}
	stream.CloseSend()
	<-done
	if len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// Read value(s) from a cache instance
func (c *Client) Read(ctx context.Context, name string, store cachepb.Store, paths [][]string, opts ...grpc.CallOption) chan *cachepb.Update {
	updCh := make(chan *cachepb.Update)
	go func() {
		defer close(updCh)
		wg := new(sync.WaitGroup)
		sem := semaphore.NewWeighted(c.cfg.MaxReadStream)
		for _, p := range paths {
			err := sem.Acquire(ctx, 1)
			if err != nil {
				return
			}
			stream, err := c.client.Read(ctx,
				&cachepb.ReadRequest{
					Name:  name,
					Path:  p,
					Store: store,
				}, opts...)
			if err != nil {
				log.Errorf("failed to create read stream: %v", err)
				return
				// return nil, err
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer sem.Release(1)
				for {
					rsp, err := stream.Recv()
					if err != nil {
						return
						// return nil, err
					}
					updCh <- &cachepb.Update{
						Path:  rsp.GetPath(),
						Value: rsp.GetValue(),
					}
				}
			}()
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
	_, err := c.client.Discard(ctx, &cachepb.DiscardRequest{
		Name:      name,
		Candidate: candidate,
	}, opts...)
	return err
}
