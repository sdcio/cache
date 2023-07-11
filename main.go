package main

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/iptecharch/cache/config"
	"github.com/iptecharch/cache/server"
	sdcpb "github.com/iptecharch/sdc-protos/sdcpb"

	"net/http"
	_ "net/http/pprof"

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
	var s *server.Server[*sdcpb.TypedValue]

	// TO BE REMOVED
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()
	//
START:
	if s != nil {
		s.Stop()
	}
	cfg, err := config.New(configFile)
	if err != nil {
		log.Errorf("failed to read config: %v", err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(cfg, "", " ")
	log.Infof("\n%s", string(b))

	ctx := context.TODO() // TODO:

	bfn := func() *sdcpb.TypedValue { return &sdcpb.TypedValue{} }

	s, err = server.NewServer(ctx, cfg, bfn)
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
