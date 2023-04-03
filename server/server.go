package server

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/iptecharch/cache/cache"
	"github.com/iptecharch/cache/config"
	"github.com/iptecharch/cache/proto/cachepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

type Server[T proto.Message] struct {
	cfg   *config.Config
	cache cache.Cache[T]
	bfn   func() T
	srv   *grpc.Server
	cachepb.UnimplementedCacheServer
	//
	router  *mux.Router
	httpSrv *http.Server
	reg     *prometheus.Registry
}

func NewServer[T proto.Message](ctx context.Context, cfg *config.Config, c cache.Cache[T], fn func() T) (*Server[T], error) {
	s := &Server[T]{
		cfg:    cfg,
		cache:  c,
		bfn:    fn,
		router: mux.NewRouter(),
		reg:    prometheus.NewRegistry(),
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

func (s *Server[T]) Start(ctx context.Context) error {
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

func (s *Server[T]) ServeHTTP() {
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

func (s *Server[T]) Stop() {
	s.srv.Stop()
	s.httpSrv.Shutdown(context.TODO())
	// s.cfn()
}
