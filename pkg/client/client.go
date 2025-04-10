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
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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
	Owner string
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

	cc, err := grpc.NewClient(ccfg.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))

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

// InstancesList List all cache instances
func (c *Client) InstancesList(ctx context.Context, opts ...grpc.CallOption) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	rsp, err := c.client.InstancesList(ctx, &cachepb.InstancesListRequest{}, opts...)
	if err != nil {
		return nil, err
	}
	return rsp.GetCacheInstances(), nil
}

// InstanceCreate Create the an instance with the given name
func (c *Client) InstanceCreate(ctx context.Context, name string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	_, err := c.client.InstanceCreate(ctx, &cachepb.InstanceCreateRequest{CacheInstanceName: name}, opts...)
	return err
}

// InstanceDelete Delete the given Cache instance
func (c *Client) InstanceDelete(ctx context.Context, name string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	_, err := c.client.InstanceDelete(ctx, &cachepb.InstanceDeleteRequest{CacheInstanceName: name}, opts...)
	return err
}

// InstanceIntentsList List all the intents for the given cache instance
func (c *Client) InstanceIntentsList(ctx context.Context, cacheInstanceName string, opts ...grpc.CallOption) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	rsp, err := c.client.InstanceIntentsList(ctx, &cachepb.InstanceIntentsListRequest{CacheInstanceName: cacheInstanceName}, opts...)
	if err != nil {
		return nil, err
	}
	return rsp.GetIntents(), nil
}

// InstanceIntentModify Create or modify the intent for the given instance
func (c *Client) InstanceIntentModify(ctx context.Context, cacheInstanceName string, intentName string, intentData []byte, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	_, err := c.client.InstanceIntentModify(ctx, &cachepb.InstanceIntentModifyRequest{CacheInstanceName: cacheInstanceName, IntentName: intentName, Intent: intentData}, opts...)
	return err
}

// InstanceIntentDelete Delete the specified intent of the given cache instance
func (c *Client) InstanceIntentDelete(ctx context.Context, cacheInstanceName string, intentName string, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	_, err := c.client.InstanceIntentDelete(ctx, &cachepb.InstanceIntentDeleteRequest{CacheInstanceName: cacheInstanceName, IntentName: intentName}, opts...)
	return err
}

// InstanceIntentGet Get the specified intent of the given cache instance
func (c *Client) InstanceIntentGet(ctx context.Context, cacheInstanceName string, intentName string, opts ...grpc.CallOption) (*Intent, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	rsp, err := c.client.InstanceIntentGet(ctx, &cachepb.InstanceIntentGetRequest{CacheInstanceName: cacheInstanceName, IntentName: intentName}, opts...)
	return NewIntent(intentName, rsp.GetIntent()), err
}

// InstanceIntentExists Check for the existance of the specified intent as part of the given cache instance
func (c *Client) InstanceIntentExists(ctx context.Context, cacheInstanceName string, intentName string, opts ...grpc.CallOption) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	rsp, err := c.client.InstanceIntentExists(ctx, &cachepb.InstanceIntentExistsRequest{CacheInstanceName: cacheInstanceName, IntentName: intentName}, opts...)
	return rsp.GetExists(), err
}

// InstanceIntentsGetAllChan Get all intents of the given cache instance. Allows to specify specific intents via name that should be excluded by the server.
func (c *Client) InstanceIntentsGetAllChan(ctx context.Context, cacheInstanceName string, excludeIntents []string, opts ...grpc.CallOption) (<-chan *Intent, <-chan error, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	strCl, err := c.client.InstanceIntentsGetAll(ctx, &cachepb.InstanceIntentGetAllRequest{CacheInstanceName: cacheInstanceName, ExcludeIntents: excludeIntents}, opts...)
	if err != nil {
		return nil, nil, err
	}

	outCh := make(chan *Intent)
	errCh := make(chan error)

	go func() {
		defer close(outCh)
		for {

			select {
			case <-ctx.Done():
				return
			default:
				req, err := strCl.Recv()
				if err == io.EOF {
					// all data received
					return
				}
				if err != nil {
					errCh <- err
					return
				}
				outCh <- NewIntent(req.GetIntentName(), req.GetIntent())
			}
		}
	}()

	return outCh, errCh, nil
}

func (c *Client) InstanceIntentsGetAll(ctx context.Context, cacheInstanceName string, excludeIntents []string, opts ...grpc.CallOption) ([]*Intent, error) {
	intentChan, errChan, err := c.InstanceIntentsGetAllChan(ctx, cacheInstanceName, excludeIntents, opts...)
	if err != nil {
		return nil, err
	}

	results := []*Intent{}

	for {
		select {
		case intent, ok := <-intentChan:
			if !ok {
				// stream finished, no more intents
				return results, nil
			}
			results = append(results, intent)

		case err := <-errChan:
			// received an error from the streaming goroutine
			return nil, err

		case <-ctx.Done():
			// context was cancelled or timed out
			return nil, ctx.Err()
		}
	}
}

type Intent struct {
	name string
	data []byte
}

func NewIntent(name string, data []byte) *Intent {
	return &Intent{
		name: name,
		data: data,
	}
}

func (i *Intent) Name() string {
	return i.name
}

func (i *Intent) Data() []byte {
	return i.data
}

func (i *Intent) String() string {
	return fmt.Sprintf("Name: %s\nData: %s\n", i.name, i.data)
}
