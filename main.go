package main

import (
	"context"
	"os"
	"time"

	"github.com/iptecharch/cache/cache"
	"github.com/iptecharch/cache/config"
	"github.com/iptecharch/cache/server"
	schemapb "github.com/iptecharch/schema-server/protos/schema_server"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

var configFile string
var debug bool
var trace bool

func main() {
	pflag.StringVarP(&configFile, "config", "c", "cache.yaml", "config file path")
	pflag.BoolVarP(&debug, "debug", "d", false, "set log level to DEBUG")
	pflag.BoolVarP(&trace, "trace", "t", false, "set log level to TRACE")
	pflag.Parse()

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	log.SetLevel(log.InfoLevel)
	if debug {
		log.SetLevel(log.DebugLevel)
	}
	if trace {
		log.SetLevel(log.TraceLevel)
	}
	var s *server.Server[*schemapb.TypedValue]
START:
	if s != nil {
		s.Stop()
	}
	cfg, err := config.New(configFile)
	if err != nil {
		log.Errorf("failed to read config: %v", err)
		os.Exit(1)
	}

	ctx := context.TODO() // TODO:

	s, err = server.NewServer(ctx,
		cfg,
		cache.New[*schemapb.TypedValue](),
		func() *schemapb.TypedValue { return &schemapb.TypedValue{} },
	)
	if err != nil {
		log.Errorf("failed to create server: %v", err)
		time.Sleep(time.Second)
		goto START
	}

	err = s.Start(ctx)
	if err != nil {
		log.Errorf("failed to run server: %v", err)
		time.Sleep(time.Second)
		goto START
	}
}
