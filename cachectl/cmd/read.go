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
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/prototext"
)

var readPath []string
var storeName string

// readCmd represents the read command
var readCmd = &cobra.Command{
	Use:   "read",
	Short: "read value from a cache instance",

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

		c, err := client.New(&client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
		})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		paths := make([][]string, 0, len(readPath))
		for _, p := range readPath {
			paths = append(paths, strings.Split(p, ","))
		}
		for rs := range c.Read(ctx, cacheName, store, paths) {
			fmt.Println(prototext.Format(rs))
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(readCmd)
	readCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	readCmd.Flags().StringArrayVarP(&readPath, "path", "p", []string{}, "paths to read")
	readCmd.Flags().StringVarP(&storeName, "store", "s", "config", "cache store to read from")
}
