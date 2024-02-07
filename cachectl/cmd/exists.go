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

	"github.com/spf13/cobra"

	"github.com/sdcio/cache/pkg/client"
)

// existsCmd represents the exists command
var existsCmd = &cobra.Command{
	Use:     "exists",
	Aliases: []string{"ex"},
	Short:   "check if a cache instance exists",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
			Timeout:       timeout,
		})
		if err != nil {
			return err
		}
		ok, err := c.Exists(cmd.Context(), cacheName)
		if err != nil {
			return err
		}
		fmt.Println(ok)
		return nil
	},
}

func init() {
	rootCmd.AddCommand(existsCmd)
	existsCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
}
