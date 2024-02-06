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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/iptecharch/cache/proto/cachepb"
)

func (s *Server) Prune(ctx context.Context, req *cachepb.PruneRequest) (*cachepb.PruneResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	switch req.GetId() {
	case "": // create pruneID
		id, err := s.cache.CreatePruneID(ctx, req.GetName(), req.GetForce())
		if err != nil {
			return nil, err
		}
		return &cachepb.PruneResponse{Id: id}, nil
	default: // apply prune
		if req.GetId() == "" {
			return nil, status.Errorf(codes.InvalidArgument, "id cannot be empty")
		}
		err := s.cache.ApplyPrune(ctx, req.GetName(), req.GetId())
		if err != nil {
			return nil, err
		}
		return &cachepb.PruneResponse{}, nil
	}
}
