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
	"github.com/iptecharch/cache/client"
	"github.com/spf13/cobra"
)

// discardCmd represents the discard command
var discardCmd = &cobra.Command{
	Use:     "discard-candidate",
	Aliases: []string{"discard"},
	Short:   "discard changes made to a candidate",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
			Timeout:       timeout,
		})
		if err != nil {
			return err
		}
		err = c.Discard(cmd.Context(), cacheName, candidateName)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(discardCmd)
	discardCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	discardCmd.Flags().StringVarP(&candidateName, "candidate", "", "", "cache candidate name")
}
