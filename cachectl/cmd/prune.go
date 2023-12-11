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
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/iptecharch/cache/pkg/client"
)

var pruneID string

// pruneCmd represents the prune command
var pruneCmd = &cobra.Command{
	Use:   "prune",
	Short: "prune a cache instance",

	RunE: func(cmd *cobra.Command, _ []string) error {
		if cacheName == "" {
			return errors.New("missing cache name")
		}
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
			Timeout:       timeout,
		})
		if err != nil {
			return err
		}
		rsp, err := c.Prune(cmd.Context(), cacheName, pruneID)
		if err != nil {
			return err
		}
		fmt.Println(rsp.GetId())
		return nil
	},
}

func init() {
	rootCmd.AddCommand(pruneCmd)
	pruneCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	pruneCmd.Flags().StringVarP(&pruneID, "id", "", "", "prune ID")
}
