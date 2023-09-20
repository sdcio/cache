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
	"github.com/spf13/cobra"

	"github.com/iptecharch/cache/pkg/client"
)

var candidateOwner string
var candidateName string
var candidatePriority int32

// createCandidateCmd represents the create-candidate command
var createCandidateCmd = &cobra.Command{
	Use:   "create-candidate",
	Short: "create a candidate from a cache instance",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
			Timeout:       timeout,
		})
		if err != nil {
			return err
		}
		err = c.CreateCandidate(cmd.Context(),
			cacheName,
			candidateName,
			candidateOwner,
			candidatePriority,
		)
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
	createCandidateCmd.Flags().StringVarP(&candidateOwner, "owner", "", "", "cache candidate owner")
	createCandidateCmd.Flags().Int32VarP(&candidatePriority, "priority", "", 0, "cache candidate priority")
}
