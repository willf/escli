/*
Copyright © 2023 Will Fitzgerald <willf@github.com>
*/
package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/willf/escli/lib/client"
)

// pingCmd represents the ping command
var pingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Is ther server alive?",
	Long:  `Pings the server to see if it is available.`,
	Run: func(cmd *cobra.Command, args []string) {
		// get a client connection to the server, call lib/client.go
		client := client.ElasticClient()
		// ping the server
		result := client.Ping()
		fmt.Println("ping result: ", result)
	},
}

func init() {
	rootCmd.AddCommand(pingCmd)

}
