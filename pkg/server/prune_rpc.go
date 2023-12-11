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
		id, err := s.cache.CreatePruneID(ctx, req.GetName())
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
