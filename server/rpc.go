package server

import (
	"context"
	"errors"
	"strings"

	"github.com/iptecharch/cache/cache"
	"github.com/iptecharch/cache/proto/cachepb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func (s *Server[T]) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	cfg, err := s.cache.GetDetails(ctx, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	rs, err := s.cache.Candidates(ctx, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &cachepb.GetResponse{
		Name:      req.GetName(),
		Ephemeral: cfg.Ephemeral,
		Cached:    cfg.Cached,
		Candidate: rs,
	}, nil
}

func (s *Server[T]) Clone(ctx context.Context, req *cachepb.CloneRequest) (*cachepb.CloneResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCname() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "cname cannot be empty")
	}
	n, err := s.cache.Clone(ctx, req.GetName(), req.GetCname())
	if err != nil {
		return nil, err
	}
	return &cachepb.CloneResponse{
		Clone: n,
	}, nil
}

func (s *Server[T]) CreateCandidate(ctx context.Context, req *cachepb.CreateCandidateRequest) (*cachepb.CreateCandidateResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCandidate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "candidate cannot be empty")
	}
	n, err := s.cache.CreateCandidate(ctx, req.GetName(), req.GetCandidate())
	if err != nil {
		return nil, err
	}
	return &cachepb.CreateCandidateResponse{
		Name:      req.GetName(),
		Candidate: n,
	}, nil
}

func (s *Server[T]) Create(ctx context.Context, req *cachepb.CreateRequest) (*cachepb.CreateResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if strings.Contains(req.GetName(), "/") {
		return nil, status.Errorf(codes.InvalidArgument, "`/` is not allowed in a cache name")
	}
	if !req.GetCached() && req.GetEphemeral() {
		return nil, status.Errorf(codes.InvalidArgument, "a cache cannot be ephemeral and not cached")
	}
	cfg := &cache.CacheInstanceConfig{
		Name:      req.GetName(),
		StoreType: s.cfg.Cache.StoreType,
		Ephemeral: req.GetEphemeral(),
		Cached:    req.GetCached(),
		Dir:       s.cfg.Cache.Dir,
	}
	log.Debugf("creating a cache with config %+v", cfg)
	err := s.cache.Create(ctx, cfg)
	return &cachepb.CreateResponse{}, err
}

func (s *Server[T]) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	log.Debugf("received delete request %v", req)
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	err := s.cache.Delete(ctx, req.GetName())
	if err != nil {
		return nil, err
	}
	return &cachepb.DeleteResponse{}, nil
}

func (s *Server[T]) List(ctx context.Context, req *cachepb.ListRequest) (*cachepb.ListResponse, error) {
	return &cachepb.ListResponse{
		Cache: s.cache.List(ctx),
	}, nil
}

func (s *Server[T]) Read(req *cachepb.ReadRequest, stream cachepb.Cache_ReadServer) error {
	ctx := stream.Context()
	pr, _ := peer.FromContext(ctx)
	log.Debugf("Read request from peer: %v: %v", pr.Addr, req)
	return s.read(req, stream)
}

func (s *Server[T]) Modify(stream cachepb.Cache_ModifyServer) error {
	ctx := stream.Context()
	pr, _ := peer.FromContext(ctx)
	log.Debugf("Modify stream from peer: %v", pr.Addr)
	for {
		req, err := stream.Recv()
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				return nil
			}
			log.Errorf("peer=%s, Modify Stream ended with err: %v", pr.Addr.String(), err)
			return err
		}

		log.Debugf("peer=%s, modifyRequest: %s", pr.Addr.String(), req)
		err = s.validateModifyRequest(req)
		if err != nil {
			return err
		}
		s.modifyCh <- req
	}
}

func (s *Server[T]) validateModifyRequest(req *cachepb.ModifyRequest) error {
	if req == nil {
		return errors.New("nil modify request")
	}
	switch req := req.GetRequest().(type) {
	case *cachepb.ModifyRequest_Write:
		if req.Write.GetName() == "" {
			return status.Errorf(codes.InvalidArgument, "name cannot be empty")
		}
		if len(req.Write.GetPath()) == 0 {
			return status.Errorf(codes.InvalidArgument, "path cannot be empty")
		}
		if req.Write.GetValue() == nil {
			return status.Errorf(codes.InvalidArgument, "value cannot be nil")
		}
	case *cachepb.ModifyRequest_Delete:
		if req.Delete.GetName() == "" {
			return status.Errorf(codes.InvalidArgument, "name cannot be empty")
		}
		if len(req.Delete.GetPath()) == 0 {
			return status.Errorf(codes.InvalidArgument, "path cannot be empty")
		}
	case nil:
		return errors.New("nil modify request")
	}
	return nil
}

func (s *Server[T]) GetChanges(req *cachepb.GetChangesRequest, stream cachepb.Cache_GetChangesServer) error {
	if req.GetName() == "" {
		return status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCandidate() == "" {
		return status.Errorf(codes.InvalidArgument, "candidate cannot be empty")
	}
	ctx := stream.Context()
	dels, updates, err := s.cache.Diff(ctx, req.GetName(), req.GetCandidate())
	if err != nil {
		return err
	}
	// send deletes
	for _, del := range dels {
		err = stream.Send(&cachepb.GetChangesResponse{
			Name:      req.GetName(),
			Candidate: req.GetCandidate(),
			Delete:    del,
		})
		if err != nil {
			return err
		}
	}
	// send updates
	for _, upd := range updates {
		b, err := proto.Marshal(upd.V)
		if err != nil {
			return err
		}
		err = stream.Send(&cachepb.GetChangesResponse{
			Name:      req.GetName(),
			Candidate: req.GetCandidate(),
			Update: &cachepb.Update{
				Path: upd.P,
				Value: &anypb.Any{
					Value: b,
				},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server[T]) Discard(ctx context.Context, req *cachepb.DiscardRequest) (*cachepb.DiscardResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCandidate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "candidate cannot be empty")
	}
	err := s.cache.Discard(ctx, req.GetName(), req.GetCandidate())
	if err != nil {
		return nil, err
	}
	return &cachepb.DiscardResponse{}, nil
}

func (s *Server[T]) Stats(ctx context.Context, req *cachepb.StatsRequest) (*cachepb.StatsResponse, error) {
	ss, err := s.cache.Stats(ctx, req.GetName(), req.GetKeysCount())
	if err != nil {
		return nil, err
	}
	rsp := &cachepb.StatsResponse{
		NumCache: int64(ss.NumInstances),
		KeyCount: map[string]*cachepb.InstanceStats{},
	}
	for _, ssi := range ss.InstanceStats {
		rsp.KeyCount[ssi.Name] = &cachepb.InstanceStats{
			Name:              ssi.Name,
			KeyCountPerBucket: ssi.KeyCount,
		}
	}
	return rsp, nil
}

// helpers
func (s *Server[T]) read(req *cachepb.ReadRequest, stream cachepb.Cache_ReadServer) error {
	ctx := stream.Context()
	var store cache.Store
	switch req.GetStore() {
	case cachepb.Store_CONFIG:
		store = cache.StoreConfig
	case cachepb.Store_STATE:
		store = cache.StoreState
	}
	ch, err := s.cache.ReadValueCh(ctx, req.GetName(), store, req.GetPath())
	if err != nil {
		return err
	}
OUTER:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-ch:
			if !ok {
				break OUTER
			}
			b, err := proto.Marshal(e.V)
			if err != nil {
				return err
			}
			rsp := &cachepb.ReadRespone{
				Name: req.GetName(),
				Path: e.P,
				Value: &anypb.Any{
					TypeUrl: "",
					Value:   b,
				},
			}
			err = stream.Send(rsp)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server[T]) modifyWrite(ctx context.Context, req *cachepb.WriteValueRequest) error {
	m := s.bfn()
	err := proto.Unmarshal(req.GetValue().GetValue(), m)
	if err != nil {
		return err
	}
	var store cache.Store
	switch req.GetStore() {
	case cachepb.Store_CONFIG:
		store = cache.StoreConfig
	case cachepb.Store_STATE:
		store = cache.StoreState
	}

	err = s.cache.WriteValue(ctx, req.GetName(), store, req.GetPath(), m)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server[T]) modifyDelete(ctx context.Context, req *cachepb.DeleteValueRequest) error {
	var err error
	// delete value from cache
	var store cache.Store
	switch req.GetStore() {
	case cachepb.Store_CONFIG:
		store = cache.StoreConfig
	case cachepb.Store_STATE:
		store = cache.StoreState
	}

	err = s.cache.DeleteValue(ctx, req.GetName(), store, req.GetPath())
	if err != nil {
		return err
	}
	return nil
}
