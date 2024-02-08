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
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/sdcio/cache/pkg/cache"
	"github.com/sdcio/cache/pkg/config"
	"github.com/sdcio/cache/proto/cachepb"
)

const (
	writeTimeout = 30 * time.Second
)

type Server struct {
	cfg *config.Config

	cache cache.Cache
	srv   *grpc.Server
	cachepb.UnimplementedCacheServer
	//
	router  *mux.Router
	httpSrv *http.Server
	reg     *prometheus.Registry
	//
	modifyCh chan *cachepb.ModifyRequest
}

func NewServer(ctx context.Context, cfg *config.Config) (*Server, error) {
	s := &Server{
		cfg:      cfg,
		router:   mux.NewRouter(),
		reg:      prometheus.NewRegistry(),
		modifyCh: make(chan *cachepb.ModifyRequest, cfg.GRPCServer.BufferSize),
	}

	opts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(cfg.GRPCServer.MaxRecvMsgSize),
	}
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
			ctx, cfn := context.WithTimeout(ctx, cfg.GRPCServer.RPCTimeout)
			defer cfn()
			return handler(ctx, req)
		},
	}

	if cfg.Prometheus != nil {
		grpcMetrics := grpc_prometheus.NewServerMetrics()
		opts = append(opts,
			grpc.StreamInterceptor(grpcMetrics.StreamServerInterceptor()),
		)
		unaryInterceptors = append(unaryInterceptors, grpcMetrics.UnaryServerInterceptor())
		s.reg.MustRegister(grpcMetrics)
	}
	opts = append(opts, grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
	if cfg.GRPCServer.TLS != nil {
		tlsCfg, err := cfg.GRPCServer.TLS.NewConfig(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsCfg)))
	}
	//

	s.srv = grpc.NewServer(opts...)

	cachepb.RegisterCacheServer(s.srv, s)
	return s, nil
}

func (s *Server) Start(ctx context.Context) error {
	s.cache = cache.New(s.cfg.Cache)
	if s.cfg.Cache.StoreType != "" {
		err := s.cache.Init(ctx)
		if err != nil {
			return err
		}
	}
	go s.startWriteWorkers(ctx)
	l, err := net.Listen("tcp", s.cfg.GRPCServer.Address)
	if err != nil {
		return err
	}
	log.Infof("running gRPC server on %s", s.cfg.GRPCServer.Address)
	if s.cfg.Prometheus != nil {
		go s.ServeHTTP()
	}
	err = s.srv.Serve(l)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) ServeHTTP() {
	s.router.Handle("/metrics", promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{}))
	s.reg.MustRegister(collectors.NewGoCollector())
	s.reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	s.httpSrv = &http.Server{
		Addr:         s.cfg.Prometheus.Address,
		Handler:      s.router,
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	}
	log.Infof("running prometheus endpoint on %s", s.cfg.Prometheus.Address)
	err := s.httpSrv.ListenAndServe()
	if err != nil {
		log.Errorf("HTTP server stopped: %v", err)
	}
}

func (s *Server) Stop() {
	s.srv.Stop()
	if s.httpSrv != nil {
		s.httpSrv.Shutdown(context.TODO())
	}
	s.cache.Close()
}

func (s *Server) startWriteWorkers(ctx context.Context) {
	for i := 0; i < s.cfg.GRPCServer.WriteWorkers; i++ {
		go s.writeWorker(ctx)
	}
	<-ctx.Done()
}

func (s *Server) writeWorker(ctx context.Context) {
	var err error
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-s.modifyCh:
			ctx, cancel := context.WithTimeout(ctx, writeTimeout)
			switch req := req.Request.(type) {
			case *cachepb.ModifyRequest_Write:
				err = s.modifyWrite(ctx, req.Write)
				if err != nil {
					log.Errorf("failed modify write: %v", err)
				}
			case *cachepb.ModifyRequest_Delete:
				err = s.modifyDelete(ctx, req.Delete)
				if err != nil {
					log.Errorf("failed modify delete: %v", err)
				}
			}
			cancel()
		}
	}
}
