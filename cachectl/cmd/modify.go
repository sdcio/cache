/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/iptecharch/cache/client"
	"github.com/iptecharch/cache/proto/cachepb"
	schemapb "github.com/iptecharch/schema-server/protos/schema_server"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

var updatePaths []string
var deletePaths []string

// modifyCmd represents the modify command
var modifyCmd = &cobra.Command{
	Use:   "modify",
	Short: "modify values in the cache",

	RunE: func(cmd *cobra.Command, _ []string) error {
		var store cachepb.Store
		switch storeName {
		case "config":
			store = cachepb.Store_CONFIG
		case "state":
			store = cachepb.Store_STATE
		default:
			return fmt.Errorf("unknown store name: %s", storeName)
		}

		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
		})
		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		dels := make([][]string, 0, len(deletePaths))

		for _, del := range deletePaths {
			dels = append(dels, strings.Split(del, ","))
		}

		upds := make([]*cachepb.Update, 0, len(updatePaths))
		for _, upd := range updatePaths {
			upd := strings.SplitN(upd, ":::", 3)
			if len(upd) != 3 {
				log.Errorf("path %q is malformed", upd)
				continue
			}
			tv, err := toTypedValue(upd[1], upd[2])
			if err != nil {
				return err
			}
			b, err := proto.Marshal(tv)
			if err != nil {
				return err
			}
			upds = append(upds, &cachepb.Update{
				Path: strings.Split(upd[0], ","),
				Value: &anypb.Any{
					Value: b,
				},
			})
		}
		err = c.Modify(ctx, cacheName, store, dels, upds)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(modifyCmd)
	modifyCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	modifyCmd.Flags().StringArrayVarP(&updatePaths, "update", "", []string{}, "path:::value to write")
	modifyCmd.Flags().StringArrayVarP(&deletePaths, "delete", "", []string{}, "paths to delete")
}

func toTypedValue(typ, val string) (*schemapb.TypedValue, error) {
	// TODO: switch over typ
	return &schemapb.TypedValue{
		Value: &schemapb.TypedValue_StringVal{
			StringVal: val,
		},
	}, nil
}
