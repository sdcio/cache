package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
)

const (
	defaultAddress       = ":50100"
	defaultMaxRcvMsgSize = 4 * 1024 * 1024
	defaultRPCTimeout    = time.Minute
	defaultBufferSize    = 100 * 1000
	numOfModifyWorkers   = 16
	//
	defaultCacheStoreType  = "badgerdbsingle"
	defaultCacheDir        = "./cached/caches"
	defaultPruneIDLifetime = 5 * time.Minute
)

type Config struct {
	GRPCServer *GRPCServer  `yaml:"grpc-server,omitempty" json:"grpc-server,omitempty"`
	Cache      *CacheConfig `yaml:"cache,omitempty" json:"cache,omitempty"`
	Prometheus *PromConfig  `yaml:"prometheus,omitempty" json:"prometheus,omitempty"`
}

type GRPCServer struct {
	Address        string        `yaml:"address,omitempty" json:"address,omitempty"`
	TLS            *TLS          `yaml:"tls,omitempty" json:"tls,omitempty"`
	MaxRecvMsgSize int           `yaml:"max-recv-msg-size,omitempty" json:"max-recv-msg-size,omitempty"`
	RPCTimeout     time.Duration `yaml:"rpc-timeout,omitempty" json:"rpc-timeout,omitempty"`
	BufferSize     int           `yaml:"buffer-size,omitempty" json:"buffer-size,omitempty"`
	WriteWorkers   int           `yaml:"write-workers,omitempty" json:"write-workers,omitempty"`
}

type CacheConfig struct {
	MaxCaches       int           `yaml:"max-caches,omitempty" json:"max-caches,omitempty"`
	StoreType       string        `yaml:"store-type,omitempty" json:"store-type,omitempty"`
	Dir             string        `yaml:"dir,omitempty" json:"dir,omitempty"`
	PruneIDLifetime time.Duration `yaml:"prune-id-lifetime,omitempty" json:"prune-id-lifetime,omitempty"`
}

func New(file string) (*Config, error) {
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	c := new(Config)
	err = yaml.Unmarshal(b, c)
	if err != nil {
		return nil, err
	}
	err = c.validateSetDefaults()
	return c, err
}

func (c *Config) validateSetDefaults() error {
	if c.GRPCServer == nil {
		c.GRPCServer = new(GRPCServer)
	}
	if c.GRPCServer.Address == "" {
		c.GRPCServer.Address = defaultAddress
	}
	if c.GRPCServer.MaxRecvMsgSize <= 0 {
		c.GRPCServer.MaxRecvMsgSize = defaultMaxRcvMsgSize
	}
	if c.GRPCServer.RPCTimeout <= 0 {
		c.GRPCServer.RPCTimeout = defaultRPCTimeout
	}
	if c.GRPCServer.BufferSize < 0 {
		c.GRPCServer.BufferSize = defaultBufferSize
	}
	if c.GRPCServer.WriteWorkers <= 0 {
		c.GRPCServer.WriteWorkers = numOfModifyWorkers
	}
	if c.Cache == nil {
		c.Cache = &CacheConfig{}
	}

	return c.Cache.validateSetDefaults()
}

func (cc *CacheConfig) validateSetDefaults() error {
	if cc.StoreType == "" {
		cc.StoreType = defaultCacheStoreType
	}
	if cc.Dir == "" {
		cc.Dir = defaultCacheDir
	}
	if cc.PruneIDLifetime <= 0 {
		cc.PruneIDLifetime = defaultPruneIDLifetime
	}
	return nil
}

type PromConfig struct {
	Address string `yaml:"address,omitempty" json:"address,omitempty"`
}

type TLS struct {
	CA         string `yaml:"ca,omitempty" json:"ca,omitempty"`
	Cert       string `yaml:"cert,omitempty" json:"cert,omitempty"`
	Key        string `yaml:"key,omitempty" json:"key,omitempty"`
	ClientAuth string `yaml:"client-auth,omitempty" json:"client-auth,omitempty"`
}

func (t *TLS) NewConfig(ctx context.Context) (*tls.Config, error) {
	tlsCfg := &tls.Config{
		InsecureSkipVerify: false,
	}

	switch t.ClientAuth {
	case "":
		if t.CA != "" {
			tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		}
	case "request":
		tlsCfg.ClientAuth = tls.RequestClientCert
	case "require":
		tlsCfg.ClientAuth = tls.RequireAnyClientCert
	case "verify-if-given":
		tlsCfg.ClientAuth = tls.VerifyClientCertIfGiven
	case "require-verify":
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if t.CA != "" {
		ca, err := os.ReadFile(t.CA)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA cert: %w", err)
		}
		if len(ca) != 0 {
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(ca)
			tlsCfg.RootCAs = caCertPool
		}
	}

	if t.Cert != "" && t.Key != "" {
		certWatcher, err := certwatcher.New(t.Cert, t.Key)
		if err != nil {
			return nil, err
		}

		go func() {
			if err := certWatcher.Start(ctx); err != nil {
				log.Errorf("certificate watcher error: %v", err)
			}
		}()
		tlsCfg.GetCertificate = certWatcher.GetCertificate
	}
	return tlsCfg, nil
}
