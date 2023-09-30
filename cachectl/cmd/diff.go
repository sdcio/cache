package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/prototext"

	"github.com/iptecharch/cache/pkg/client"
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "return difference between a candidate and its main cache",

	RunE: func(cmd *cobra.Command, _ []string) error {
		c, err := client.New(cmd.Context(), &client.ClientConfig{
			Address:       address,
			MaxReadStream: 1,
			Timeout:       timeout,
		})
		if err != nil {
			return err
		}
		changes, err := c.GetChanges(cmd.Context(), cacheName, candidateName)
		if err != nil {
			return err
		}

		for _, change := range changes {
			b := prototext.Format(change)
			fmt.Println(string(b))
		}
		fmt.Printf("returned %d change(s)\n", len(changes))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(diffCmd)
	diffCmd.Flags().StringVarP(&cacheName, "name", "n", "", "cache name")
	diffCmd.Flags().StringVarP(&candidateName, "candidate", "", "", "cache candidate name")
}
