/*
Copyright © 2023 Will Fitzgerald <willf@github.com>
*/
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/willf/escli/lib"
)

type PingResult struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// pingCmd represents the ping command
var pingCmd = &cobra.Command{
	Use:   "ping",
	Short: "Is ther server alive?",
	Long:  `Pings the server to see if it is available.`,
	Run: func(cmd *cobra.Command, args []string) {

		result := ping()
		jsonBytes, _ := json.Marshal(result)
		fmt.Println(string(jsonBytes))

	},
}

func init() {
	rootCmd.AddCommand(pingCmd)

}

func ping() (result PingResult) {
	// get a client connection to the server, call lib/client.go
	client, err := lib.ElasticClient()
	if err != nil {
		return PingResult{Ok: false, Error: err.Error()}
	}
	// ping the server
	_, err = client.API.Ping()
	if err != nil {
		return PingResult{Ok: false, Error: err.Error()}
	}
	return PingResult{Ok: true}
}
