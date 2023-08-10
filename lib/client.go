// code to create an Elastisearch client, reading from the environment or a config file

package lib

import (
	// elasticsearch
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/spf13/viper"
)

// ElasticClient creates an Elasticsearch client
func ElasticClient() (es *elasticsearch.Client, err error) {
	server := viper.GetString("ELASTICSEARCH_SERVER")
	user := viper.GetString("ELASTICSEARCH_USER")
	password := viper.GetString("ELASTICSEARCH_PASSWORD")
	certificate_fingerprint := viper.GetString("ELASTICSEARCH_CERTIFICATE_FINGERPRINT")
	api_key := viper.GetString("ELASTICSEARCH_API_KEY")
	// create a new client
	cfg := elasticsearch.Config{
		Addresses: []string{
			server,
		},
		Username:               user,
		Password:               password,
		CertificateFingerprint: certificate_fingerprint,
		APIKey:                 api_key,
	}
	es, err = elasticsearch.NewClient(cfg)
}
