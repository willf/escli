/*
Copyright © 2023 Will Fitzgerald <willf@github.com>
*/
package cmd

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/willf/escli/lib"
)

type UploadResult struct {
	Took  int   `json:"took"`
	Error error `json:"error,omitempty"`
	Total int   `json:"total"`
}

// uploadCmd represents the upload command
var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload documents to an index",
	Long:  `Upload documents to an index from stdin.`,
	Run: func(cmd *cobra.Command, args []string) {
		uploadResult := UploadAll()
		jsonBytes, _ := json.Marshal(uploadResult)
		fmt.Println(string(jsonBytes))
	},
}

func UploadAll() (uploadResult UploadResult) {
	startTime := time.Now()
	es, err := lib.ElasticClient()
	if err != nil {
		uploadResult.Error = err
		return
	}
	total, err := ProcessStdinInBatches(es)
	endTime := time.Now()
	uploadResult.Total = total
	uploadResult.Took = int(endTime.Sub(startTime).Seconds())
	if err != nil {
		uploadResult.Error = err
	}
	return uploadResult
}

func init() {
	rootCmd.AddCommand(uploadCmd)

	uploadCmd.Flags().StringP("index", "i", "", "index name")
	uploadCmd.Flags().StringP("id", "d", "", "id key")
	uploadCmd.Flags().IntP("batchsize", "b", 100, "number of documents to upload at a time")
}

// a document will be a JSON object, with string keys
// and values that are JSON objects

type Document map[string]interface{}

// get the value of a key in a document
func (d Document) Get(key string) (interface{}, bool) {
	val, ok := d[key]
	return val, ok
}

// set the value of a key in a document
func (d Document) Set(key string, val interface{}) {
	d[key] = val
}

type IndexReference struct {
	Index string `json:"_index"`
	Id    string `json:"_id"`
}

type IndexInstruction struct {
	Index IndexReference `json:"index"`
}

// from a Document, a document id key, and an index name,
// create an IndexInstruction
func (d Document) ToIndexInstruction(idKey string, indexName string) (IndexInstruction, bool) {
	val, ok := d.Get(idKey)
	if !ok {
		return IndexInstruction{}, false
	}
	return IndexInstruction{
		Index: IndexReference{
			Index: indexName,
			Id:    val.(string),
		},
	}, true
}

func ProcessStdinInBatches(client *elastictransport.Client) (total int, err error) {

	batchsize := viper.GetInt("batchsize")
	indexName := viper.GetString("index")
	idKey := viper.GetString("id")

	fmt.Println("batchsize", batchsize)
	fmt.Println("index", indexName)
	fmt.Println("id", idKey)

	if indexName == "" {
		return 0, errors.New("index name must be specified")
	}

	if idKey == "" {
		return 0, errors.New("id key must be specified")
	}

	// create batchsize * 2 documents
	batch := make([][]byte, batchsize*2)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		// read in batchsize documents
		for i := 0; i < batchsize*2; i += 2 {
			if !scanner.Scan() {
				fmt.Println("done")
				break
			}
			// read in a document
			// create an IndexInstruction
			// add to batch
			var doc Document
			err = json.Unmarshal(scanner.Bytes(), &doc)
			if err != nil {
				return total, err
			}
			ii, ok := doc.ToIndexInstruction(idKey, indexName)
			if !ok {
				return total, errors.New("document missing id key")
			}
			marshalled_ii, err := json.Marshal(ii)
			if err != nil {
				return total, err
			}
			marshalled_doc, err := json.Marshal(doc)
			if err != nil {
				return total, err
			}

			total++

			batch[i] = marshalled_ii
			batch[i+1] = marshalled_doc
		}
		if len(batch) == 0 {
			break
		}
		// upload the batch to the index
		err := Upload(client, batch)
		if err != nil {
			return total - batchsize, err
		}
		batch = make([][]byte, batchsize*2)
	}
	return total, nil
}

func Upload(client *elastictransport.Client, batch [][]byte) (err error) {
	joined := bytes.Join(batch, []byte("\n"))

	req, err := http.NewRequest("POST", "/_bulk", bytes.NewBuffer(joined))

	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/x-ndjson")

	res, err := client.Perform(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()
	return nil
}
