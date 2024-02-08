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

package cmd

import (
	"fmt"
	"strings"

	sdcpb "github.com/sdcio/sdc-protos/sdcpb"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/sdcio/cache/pkg/cache"
	"github.com/sdcio/cache/pkg/client"
)

var watchPaths []string

var watchCmd = &cobra.Command{
	Use:   "watch",
	Short: "watch prefixes written to the cache",

	RunE: func(cmd *cobra.Command, _ []string) error {
		var store cache.Store
		switch storeName {
		case "config":
			store = cache.StoreConfig
		case "state":
			store = cache.StoreState
		case "intended":
			store = cache.StoreIntended
		default:
			return fmt.Errorf("unknown store name: %s", storeName)
		}
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
			Timeout:       timeout,
		})
		if err != nil {
			return err
		}
		paths := make([][]string, 0, len(watchPaths))
		for _, wp := range watchPaths {
			paths = append(paths, strings.Split(wp, ","))
		}
		rspCh, err := c.Watch(cmd.Context(), cacheName, store, paths)
		if err != nil {
			return err
		}
		for {
			select {
			case <-cmd.Context().Done():
				return cmd.Context().Err()
			case rsp, ok := <-rspCh:
				if !ok {
					fmt.Println("watch done...")
					return nil
				}
				switch format {
				case "":
					b := prototext.Format(rsp)
					fmt.Println(string(b))
				case "flat":
					tv := new(sdcpb.TypedValue)
					if len(rsp.GetValue()) != 0 {
						err = proto.Unmarshal(rsp.GetValue(), tv)
						if err != nil {
							return err
						}
					}
					fmt.Printf("%s: %s: %s\n", storeName, strings.Join(rsp.GetPath(), ","), tvSPrint(tv))
				}
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(watchCmd)
	watchCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	watchCmd.Flags().StringVarP(&storeName, "store", "", "config", "cache store name")
	watchCmd.Flags().StringArrayVarP(&watchPaths, "path", "p", []string{}, "paths to watch")
	watchCmd.Flags().StringVarP(&format, "format", "", "", "print format, '', 'flat' or 'json'")
}
