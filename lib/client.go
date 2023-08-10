// code to create an Elastisearch client, reading from the environment or a config file

package lib

import (
	// elasticsearch
	"errors"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/spf13/viper"
)

// ElasticClient creates an Elasticsearch client
func ElasticClient() (es *elasticsearch.Client, err error) {
	servers := viper.GetStringSlice("ELASTICSEARCH_SERVERS")
	user := viper.GetString("ELASTICSEARCH_USER")
	password := viper.GetString("ELASTICSEARCH_PASSWORD")
	certificate_fingerprint := viper.GetString("ELASTICSEARCH_CERTIFICATE_FINGERPRINT")
	api_key := viper.GetString("ELASTICSEARCH_API_KEY")

	//
	if len(servers) == 0 {
		err = errors.New("no Elasticsearch servers specified; config file must contain ELASTICSEARCH_SERVERS")
		return
	}
	if user == "" {
		err = errors.New("no Elasticsearch user specified; config file must contain ELASTICSEARCH_USER")
		return
	}
	if password == "" {
		err = errors.New("no Elasticsearch password specified; config file must contain ELASTICSEARCH_PASSWORD")
		return
	}
	if certificate_fingerprint == "" {
		err = errors.New("no Elasticsearch certificate fingerprint specified; config file must contain ELASTICSEARCH_CERTIFICATE_FINGERPRINT")
		return
	}
	if api_key == "" {
		err = errors.New("no Elasticsearch API key specified; config file must contain ELASTICSEARCH_API_KEY")
		return
	}
	// create a new client
	cfg := elasticsearch.Config{
		Addresses:              servers,
		Username:               user,
		Password:               password,
		CertificateFingerprint: certificate_fingerprint,
		APIKey:                 api_key,
	}
	es, err = elasticsearch.NewClient(cfg)
	return
}
