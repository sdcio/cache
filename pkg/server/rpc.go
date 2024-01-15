package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/iptecharch/cache/pkg/cache"
	"github.com/iptecharch/cache/proto/cachepb"
)

func (s *Server) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	cands, err := s.cache.Candidates(ctx, req.GetName())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "could not retrieve candidates list: %v", err)
	}
	rsp := &cachepb.GetResponse{
		Name:      req.GetName(),
		Candidate: make([]*cachepb.Candidate, 0, len(cands)),
	}
	for _, cd := range cands {
		rsp.Candidate = append(rsp.Candidate, &cachepb.Candidate{
			Name:     cd.CandidateName,
			Owner:    cd.Owner,
			Priority: cd.Priority,
		})
	}
	return rsp, nil
}

func (s *Server) Clone(ctx context.Context, req *cachepb.CloneRequest) (*cachepb.CloneResponse, error) {
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

func (s *Server) CreateCandidate(ctx context.Context, req *cachepb.CreateCandidateRequest) (*cachepb.CreateCandidateResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCandidate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "candidate cannot be empty")
	}
	n, err := s.cache.CreateCandidate(ctx, req.GetName(), req.GetCandidate(), req.GetOwner(), req.GetPriority())
	if err != nil {
		return nil, err
	}
	return &cachepb.CreateCandidateResponse{
		Name:      req.GetName(),
		Candidate: n,
	}, nil
}

func (s *Server) Create(ctx context.Context, req *cachepb.CreateRequest) (*cachepb.CreateResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if strings.Contains(req.GetName(), "/") {
		return nil, status.Errorf(codes.InvalidArgument, "`/` is not allowed in a cache name")
	}
	// if !req.GetCached() && req.GetEphemeral() {
	// 	req.Cached = true
	// }
	cfg := &cache.CacheInstanceConfig{
		Name:      req.GetName(),
		StoreType: s.cfg.Cache.StoreType,
		// Ephemeral: req.GetEphemeral(),
		// Cached:    req.GetCached(),
		Dir:             s.cfg.Cache.Dir,
		PruneIDLifetime: s.cfg.Cache.PruneIDLifetime,
	}
	log.Debugf("creating a cache with config %+v", cfg)
	err := s.cache.Create(ctx, cfg)
	return &cachepb.CreateResponse{}, err
}

func (s *Server) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
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

func (s *Server) Exists(ctx context.Context, req *cachepb.ExistsRequest) (*cachepb.ExistsResponse, error) {
	log.Debugf("received exists request %v", req)
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	return &cachepb.ExistsResponse{
		Exists: s.cache.Exists(ctx, req.GetName()),
	}, nil
}

func (s *Server) List(ctx context.Context, req *cachepb.ListRequest) (*cachepb.ListResponse, error) {
	return &cachepb.ListResponse{
		Cache: s.cache.List(ctx),
	}, nil
}

func (s *Server) Read(req *cachepb.ReadRequest, stream cachepb.Cache_ReadServer) error {
	ctx := stream.Context()
	pr, _ := peer.FromContext(ctx)
	if log.GetLevel() >= log.DebugLevel {
		id := xid.New()
		log.Debugf("%s: Read request from peer: %v: %v", id.String(), pr.Addr, req)
		defer log.Debugf("%s: Read request from peer %v done", id.String(), pr.Addr)
	}
	return s.read(req, stream)
}

func (s *Server) Modify(stream cachepb.Cache_ModifyServer) error {
	ctx := stream.Context()
	pr, _ := peer.FromContext(ctx)
	if log.GetLevel() >= log.DebugLevel {
		id := xid.New()
		log.Debugf("%s: Modify stream from peer: %v", id.String(), pr.Addr)
		defer log.Debugf("%s: Modify stream from peer %v done", id.String(), pr.Addr)
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			if strings.Contains(err.Error(), "EOF") {
				return nil
			}
			if strings.Contains(err.Error(), "context canceled") {
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
		select {
		case s.modifyCh <- req:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Server) validateModifyRequest(req *cachepb.ModifyRequest) error {
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

func (s *Server) GetChanges(req *cachepb.GetChangesRequest, stream cachepb.Cache_GetChangesServer) error {
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
		err = stream.Send(&cachepb.GetChangesResponse{
			Name:      req.GetName(),
			Candidate: req.GetCandidate(),
			Update: &cachepb.Update{
				Path: upd.P,
				Value: &anypb.Any{
					Value: upd.V,
				},
			},
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) Discard(ctx context.Context, req *cachepb.DiscardRequest) (*cachepb.DiscardResponse, error) {
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

func (s *Server) Commit(ctx context.Context, req *cachepb.CommitRequest) (*cachepb.CommitResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "name cannot be empty")
	}
	if req.GetCandidate() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "candidate cannot be empty")
	}
	err := s.cache.Commit(ctx, req.GetName(), req.GetCandidate())
	if err != nil {
		return nil, err
	}
	return &cachepb.CommitResponse{}, nil
}

func (s *Server) Stats(ctx context.Context, req *cachepb.StatsRequest) (*cachepb.StatsResponse, error) {
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

func (s *Server) Watch(req *cachepb.WatchRequest, stream cachepb.Cache_WatchServer) error {
	ctx := stream.Context()
	pr, _ := peer.FromContext(ctx)
	if log.GetLevel() >= log.DebugLevel {
		id := xid.New()
		log.Debugf("%s: Watch request from peer: %v: %v", id.String(), pr.Addr, req)
		defer log.Debugf("%s: Watch request from peer %v done", id.String(), pr.Addr)
	}
	return s.watch(req, stream)
}

// helpers
func (s *Server) read(req *cachepb.ReadRequest, stream cachepb.Cache_ReadServer) error {
	ctx := stream.Context()
	var ch chan *cache.Entry
	var err error
	
	paths := make([][]string, 0, len(req.GetPath()))
	for _, pp := range req.GetPath() {
		paths = append(paths, pp.GetElem())
	}

	switch req.GetPeriod() {
	case 0:
		ch, err = s.cache.ReadValue(ctx, req.GetName(),
			&cache.Opts{
				Store:         getCacheStore(req.GetStore()),
				Path:          paths,
				Owner:         req.GetOwner(),
				Priority:      req.GetPriority(),
				PriorityCount: req.GetPriorityCount(),
				KeysOnly:      req.GetKeysOnly(),
			})
		if err != nil {
			return err
		}
	default:
		period := time.Duration(req.GetPeriod())
		if period < time.Second {
			period = time.Second
		}
		ch, err = s.cache.ReadValuePeriodic(ctx, req.GetName(), &cache.Opts{
			Store:    getCacheStore(req.GetStore()),
			Path:     paths,
			Owner:    req.GetOwner(),
			Priority: req.GetPriority(),
		}, period)
		if err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e, ok := <-ch:
			if !ok {
				return nil
			}
			rsp := &cachepb.ReadResponse{
				Name:      req.GetName(),
				Path:      e.P,
				Value:     &anypb.Any{Value: e.V},
				Store:     req.Store,
				Owner:     e.Owner,
				Priority:  e.Priority,
				Timestamp: int64(e.Timestamp),
			}
			err = stream.Send(rsp)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) modifyWrite(ctx context.Context, req *cachepb.WriteValueRequest) error {
	// write the bytes to the cache,
	// this method will not unmarshal the bytes into T
	return s.cache.WriteValue(ctx, req.GetName(), &cache.Opts{
		Owner:    req.GetOwner(),
		Priority: req.GetPriority(),
		Store:    getCacheStore(req.GetStore()),
		Path:     [][]string{req.GetPath()},
	}, req.GetValue().GetValue(),
	)
}

func (s *Server) modifyDelete(ctx context.Context, req *cachepb.DeleteValueRequest) error {
	// delete value from cache
	return s.cache.DeletePrefix(ctx, req.GetName(), &cache.Opts{
		Store:    getCacheStore(req.GetStore()),
		Path:     [][]string{req.GetPath()},
		Owner:    req.GetOwner(),
		Priority: req.GetPriority(),
	})
}

func (s *Server) watch(req *cachepb.WatchRequest, stream cachepb.Cache_WatchServer) error {
	ctx := stream.Context()
	var store cache.Store
	switch req.GetStore() {
	case cachepb.Store_CONFIG:
		store = cache.StoreConfig
	case cachepb.Store_STATE:
		store = cache.StoreState
	case cachepb.Store_INTENDED:
		store = cache.StoreIntended
	}

	prefixes := make([][]string, 0, len(req.GetPath()))
	for _, p := range req.GetPath() {
		prefixes = append(prefixes, p.GetElem())
	}

	ch, err := s.cache.Watch(ctx, req.GetName(), store, prefixes)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case e, ok := <-ch:
			if !ok {
				return nil
			}

			err = stream.Send(
				&cachepb.WatchResponse{
					Path:  e.P,
					Value: e.V,
				})
			if err != nil {
				return err
			}
		}
	}
}

func getCacheStore(pbStore cachepb.Store) cache.Store {
	switch pbStore {
	default: // cachepb.Store_CONFIG:
		return cache.StoreConfig
	case cachepb.Store_STATE:
		return cache.StoreState
	case cachepb.Store_INTENDED:
		return cache.StoreIntended
	case cachepb.Store_METADATA:
		return cache.StoreMetadata
	}
}
