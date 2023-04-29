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

var cacheName string
var ephemeral bool
var cached bool

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "create a cache instance",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
		})
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
		defer cancel()
		err = c.Create(ctx, cacheName, ephemeral, cached)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	rootCmd.AddCommand(createCmd)
	createCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	createCmd.Flags().BoolVarP(&ephemeral, "ephemeral", "", false, "create an ephemeral cache")
	createCmd.Flags().BoolVarP(&cached, "cached", "", false, "create a cached cache")
}
