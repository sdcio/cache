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
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/iptecharch/cache/pkg/client"
)

// getChangesCmd represents the get-changes command
var getChangesCmd = &cobra.Command{
	Use:   "get-changes",
	Short: "get changes made to a candidate",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
		})
		if err != nil {
			return err
		}
		changes, err := c.GetChanges(cmd.Context(), cacheName, candidateName)
		if err != nil {
			return err
		}
		for _, change := range changes {
			fmt.Println(prototext.Format(change))
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(getChangesCmd)
	getChangesCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	getChangesCmd.Flags().StringVarP(&candidateName, "candidate", "", "", "cache candidate name")
}
