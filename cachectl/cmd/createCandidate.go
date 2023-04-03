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
	"time"

	"github.com/iptecharch/cache/client"
	"github.com/spf13/cobra"
)

var candidateName string

// cloneCmd represents the clone command
var createCandidateCmd = &cobra.Command{
	Use:   "create-candidate",
	Short: "create a candidate from a cache instance",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(&client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
		})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
		defer cancel()
		err = c.CreateCandidate(ctx, cacheName, candidateName)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(createCandidateCmd)
	createCandidateCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	createCandidateCmd.Flags().StringVarP(&candidateName, "candidate", "", "", "cache candidate name")
}
