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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/sdcio/logger"
	"github.com/spf13/pflag"

	"github.com/sdcio/cache/pkg/config"
	"github.com/sdcio/cache/pkg/server"
)

var version = "dev"
var commit string

var configFile string
var debug bool
var trace bool
var versionFlag bool

func main() {
	pflag.StringVarP(&configFile, "config", "c", "cache.yaml", "config file path")
	pflag.BoolVarP(&debug, "debug", "d", false, "set log level to DEBUG")
	pflag.BoolVarP(&trace, "trace", "t", false, "set log level to TRACE")
	pflag.BoolVarP(&versionFlag, "version", "v", false, "print version")
	pflag.Parse()

	if versionFlag {
		fmt.Println(version + "-" + commit)
		return
	}

	slogOpts := &slog.HandlerOptions{
		Level:       slog.LevelInfo,
		ReplaceAttr: logger.ReplaceTimeAttr,
	}
	if debug {
		slogOpts.Level = slog.Level(-logger.VDebug)
	}
	if trace {
		slogOpts.Level = slog.Level(-logger.VTrace)
	}

	log := logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, slogOpts))
	logger.SetDefaultLogger(log)
	ctx := logger.IntoContext(context.TODO(), log) // TODO:

	var s *server.Server

START:
	if s != nil {
		s.Stop(ctx)
	}
	cfg, err := config.New(configFile)
	if err != nil {
		log.Error(err, "failed to read config")
		os.Exit(1)
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Error(err, "failed to marshal config")
		os.Exit(1)
	}
	log.Info("read config", "config", string(b))

	s, err = server.NewServer(ctx, cfg)
	if err != nil {
		log.Error(err, "failed to create server")
		time.Sleep(time.Second)
		goto START
	}

	err = s.Start(ctx)
	if err != nil {
		log.Error(err, "failed to run server")
		time.Sleep(time.Second)
		goto START
	}
}
