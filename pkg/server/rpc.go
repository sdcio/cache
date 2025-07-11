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

package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/sdcio/cache/pkg/cache"
	"github.com/sdcio/cache/pkg/types"
	"github.com/sdcio/cache/proto/cachepb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (s *Server) InstanceDelete(ctx context.Context, req *cachepb.InstanceDeleteRequest) (*cachepb.InstanceDeleteResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}
	err := s.cache.InstanceDelete(ctx, req.GetCacheInstanceName())
	return &cachepb.InstanceDeleteResponse{}, toGrpcCode(err)
}

func (s *Server) InstanceCreate(ctx context.Context, req *cachepb.InstanceCreateRequest) (*cachepb.InstanceCreateResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}
	err := s.cache.InstanceCreate(ctx, req.GetCacheInstanceName())
	return &cachepb.InstanceCreateResponse{}, toGrpcCode(err)
}

func (s *Server) InstancesList(ctx context.Context, req *cachepb.InstancesListRequest) (*cachepb.InstancesListResponse, error) {
	return &cachepb.InstancesListResponse{
		CacheInstances: s.cache.InstancesList(ctx),
	}, nil
}

func (s *Server) InstanceIntentModify(ctx context.Context, req *cachepb.InstanceIntentModifyRequest) (*cachepb.InstanceIntentModifyResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}
	if req.GetIntentName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "no intent name provided")
	}
	if req.GetIntent() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "no intent content provided")
	}

	err := s.cache.InstanceIntentModify(ctx, req.GetCacheInstanceName(), req.GetIntentName(), req.GetIntent())
	return &cachepb.InstanceIntentModifyResponse{}, toGrpcCode(err)
}

func (s *Server) InstanceIntentGet(ctx context.Context, req *cachepb.InstanceIntentGetRequest) (*cachepb.InstanceIntentGetResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}
	if req.GetIntentName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "intent name not set for cache %s", req.GetCacheInstanceName())
	}
	intent, err := s.cache.InstanceIntentGet(ctx, req.GetCacheInstanceName(), req.GetIntentName())
	if err != nil {
		return nil, err
	}

	// enable compression on large messages
	if len(intent) > 1000000 {
		grpc.SetHeader(ctx, metadata.Pairs("grpc-encoding", gzip.Name))
	}

	return &cachepb.InstanceIntentGetResponse{Intent: intent}, nil
}

func (s *Server) InstanceIntentsList(ctx context.Context, req *cachepb.InstanceIntentsListRequest) (*cachepb.InstanceIntentsListResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}

	intents, err := s.cache.InstanceIntentsList(ctx, req.GetCacheInstanceName())
	if err != nil {
		return nil, toGrpcCode(err)
	}

	result := &cachepb.InstanceIntentsListResponse{
		Intents: intents,
	}

	return result, nil
}

func (s *Server) InstanceIntentDelete(ctx context.Context, req *cachepb.InstanceIntentDeleteRequest) (*cachepb.InstanceIntentDeleteResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}
	if req.GetIntentName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "intent name not set for cache %s", req.GetCacheInstanceName())
	}

	err := s.cache.InstanceIntentDelete(ctx, req.GetCacheInstanceName(), req.GetIntentName(), req.GetIgnoreNonExisting())
	return nil, err
}

func (s *Server) InstanceIntentExists(ctx context.Context, req *cachepb.InstanceIntentExistsRequest) (*cachepb.InstanceIntentExistsResponse, error) {
	if req.GetCacheInstanceName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}
	if req.GetIntentName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "intent name not set for cache %s", req.GetCacheInstanceName())
	}

	exists, err := s.cache.InstanceIntentExists(ctx, req.GetCacheInstanceName(), req.GetIntentName())
	return &cachepb.InstanceIntentExistsResponse{
		Exists: exists,
	}, err
}

func (s *Server) InstanceIntentsGetAll(req *cachepb.InstanceIntentGetAllRequest, stream grpc.ServerStreamingServer[cachepb.InstanceIntentGetResponse]) error {
	if req.GetCacheInstanceName() == "" {
		return status.Errorf(codes.InvalidArgument, "cache instance name not set")
	}

	ctx := stream.Context()

	// Create buffered channels to prevent blocking
	intentChan := make(chan *types.Intent, 10)
	errChan := make(chan error)

	go s.cache.InstanceIntentGetAll(ctx, req.GetCacheInstanceName(), req.ExcludeIntents, intentChan, errChan)

	for {
		select {
		case err := <-errChan:
			if err != nil {
				log.Errorf("cache %s streaming error: %v", req.GetCacheInstanceName(), err)
				return fmt.Errorf("streaming error: %w", err)
			}
		case <-ctx.Done():
			log.Errorf("cache %s streaming canceled: %v", req.GetCacheInstanceName(), ctx.Err())
			return fmt.Errorf("streaming canceled: %v", ctx.Err())
		case intent, ok := <-intentChan:
			if !ok {
				return nil
			}
			log.Debugf("cache %s sending intent: %s\n", req.GetCacheInstanceName(), intent.Name())
			err := stream.Send(&cachepb.InstanceIntentGetResponse{IntentName: intent.Name(), Intent: intent.Data()})
			if err != nil {
				return err
			}
		}
	}
}

func toGrpcCode(err error) error {
	switch {
	case errors.Is(err, cache.ErrorCacheInstanceNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return err
	}
}
