package server

import (
	"context"
	"fmt"

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
	rs, err := s.cache.Candidates(ctx, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &cachepb.GetResponse{
		Name:      req.GetName(),
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
	err := s.cache.Create(ctx, req.GetName())
	return &cachepb.CreateResponse{}, err
}

func (s *Server[T]) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	err := s.cache.Delete(ctx, req.GetName())
	return &cachepb.DeleteResponse{}, err
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
			return err
		}
		log.Debugf("modifyRequest: %s", req)
		switch req := req.Request.(type) {
		case *cachepb.ModifyRequest_Write:
			err = s.modifyWrite(req.Write, stream)
		case *cachepb.ModifyRequest_Delete:
			err = s.modifyDelete(req.Delete, stream)
		}
		if err != nil {
			return err
		}
	}
}

func (s *Server[T]) GetChanges(req *cachepb.GetChangesRequest, stream cachepb.Cache_GetChangesServer) error {
	if req.GetName() == "" {
		return status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCandidate() == "" {
		return status.Errorf(codes.InvalidArgument, "candidate cannot be empty")
	}
	ctx := stream.Context()
	dels, updates, err := s.cache.GetChanges(ctx, req.GetName(), req.GetCandidate())
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
			err = stream.Send(&cachepb.ReadRespone{
				Name: req.GetName(),
				Path: e.P,
				Value: &anypb.Any{
					TypeUrl: "",
					Value:   b,
				},
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server[T]) modifyWrite(req *cachepb.WriteValueRequest, stream cachepb.Cache_ModifyServer) error {

	m := s.bfn()
	err := proto.Unmarshal(req.GetValue().GetValue(), m)
	if err != nil {
		err2 := stream.Send(&cachepb.ModifyResponse{
			Response: &cachepb.ModifyResponse_Write{
				Write: &cachepb.WriteValueResponse{
					Name:  req.GetName(),
					Path:  req.GetPath(),
					Error: fmt.Sprintf("%v", err),
				},
			},
		})
		if err2 != nil {
			return err2
		}
	}
	var store cache.Store
	switch req.GetStore() {
	case cachepb.Store_CONFIG:
		store = cache.StoreConfig
	case cachepb.Store_STATE:
		store = cache.StoreState
	}

	ctx := stream.Context()
	err = s.cache.WriteValue(ctx, req.GetName(), store, req.GetPath(), m)
	if err != nil {
		err2 := stream.Send(&cachepb.ModifyResponse{
			Response: &cachepb.ModifyResponse_Write{
				Write: &cachepb.WriteValueResponse{
					Name:  req.GetName(),
					Path:  req.GetPath(),
					Error: fmt.Sprintf("%v", err),
				},
			},
		})
		if err2 != nil {
			return err2
		}
	}
	return nil
}

func (s *Server[T]) modifyDelete(req *cachepb.DeleteValueRequest, stream cachepb.Cache_ModifyServer) error {
	var err error
	if req.GetName() == "" {
		err = stream.Send(&cachepb.ModifyResponse{
			Response: &cachepb.ModifyResponse_Delete{
				Delete: &cachepb.DeleteValueResponse{
					Name:  req.GetName(),
					Path:  req.GetPath(),
					Error: "name cannot be empty",
				},
			},
		})
		if err != nil {
			return err
		}
	}
	if len(req.GetPath()) == 0 {
		err = stream.Send(&cachepb.ModifyResponse{
			Response: &cachepb.ModifyResponse_Delete{
				Delete: &cachepb.DeleteValueResponse{
					Name:  req.GetName(),
					Path:  req.GetPath(),
					Error: "path cannot be empty",
				},
			},
		})
		if err != nil {
			return err
		}
	}
	// delete value from cache
	var store cache.Store
	switch req.GetStore() {
	case cachepb.Store_CONFIG:
		store = cache.StoreConfig
	case cachepb.Store_STATE:
		store = cache.StoreState
	}

	ctx := stream.Context()
	err = s.cache.DeleteValue(ctx, req.GetName(), store, req.GetPath())
	if err != nil {
		err2 := stream.Send(&cachepb.ModifyResponse{
			Response: &cachepb.ModifyResponse_Delete{
				Delete: &cachepb.DeleteValueResponse{
					Name:  req.GetName(),
					Path:  req.GetPath(),
					Error: fmt.Sprintf("%v", err),
				},
			},
		})
		if err2 != nil {
			return err2
		}
	}
	return nil
}
