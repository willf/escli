/*
Copyright © 2023 Will Fitzgerald <willf@github.com>
*/
package cmd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/willf/escli/lib"
)

type MappingResult struct {
	Ok    bool  `json:"ok"`
	Error error `json:"error,omitempty"`
}

var mappingsFile string

// mappingsCmd represents the mappings command
var mappingsCmd = &cobra.Command{
	Use:   "mappings",
	Short: "Create mappings on server",
	Long: `Create mappings on servier

This command will create mappings on the server.
The mappings file is a JSON file that contains the mappings to be created.
It can specified by the --mappings flag or read in from stdin.

For example:

	$ escli mappings --mappings mappings.json

	or

	$ cat mappings.json | escli mappings

`,
	Run: func(cmd *cobra.Command, args []string) {
		result := createMappings()
		jsonBytes, _ := json.Marshal(result)
		fmt.Println(string(jsonBytes))
	},
}

func init() {
	rootCmd.AddCommand(mappingsCmd)

	mappingsCmd.Flags().StringVarP(&mappingsFile, "mappings", "m", "", "mappings file")
}

func readInMappings() (mappings []byte, err error) {
	mappingsFile := viper.GetString("mappings")
	if mappingsFile != "" {
		return os.ReadFile(mappingsFile)
	} else {
		return io.ReadAll(os.Stdin)
	}
}

func createMappings() (mappingResult MappingResult) {
	mappings, err := readInMappings()
	if err != nil {
		mappingResult.Ok = false
		mappingResult.Error = errors.New("unable to read mappings file")
		return
	}
	es, err := lib.ElasticClient()
	if err != nil {
		mappingResult.Ok = false
		mappingResult.Error = errors.New("unable to create client")
		return
	}
	index := viper.GetString("ELASTICSEARCH_INDEX")
	if index == "" {
		mappingResult.Ok = false
		mappingResult.Error = errors.New("no index specified. Use --index flag or ELASTICSEARCH_INDEX configuration variable")
		return
	}

	req, err := http.NewRequest("PUT", "/", bytes.NewBuffer([]byte(mappings)))

	if err != nil {
		mappingResult.Ok = false
		mappingResult.Error = err
		return
	}

	req.Header.Set("Content-Type", "application/json")

	res, err := es.Perform(req)
	if err != nil {
		mappingResult.Ok = false
		mappingResult.Error = err
		return
	}
	defer res.Body.Close()
	return MappingResult{Ok: true}

}
