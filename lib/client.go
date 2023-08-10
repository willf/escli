// code to create an Elastisearch client, reading from the environment or a config file

package lib

import (
	// elasticsearch
	"errors"
	"net/url"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/spf13/viper"
)

// ElasticClient creates an Elasticsearch client
func ElasticClient() (es *elastictransport.Client, err error) {

	servers := viper.GetStringSlice("ELASTICSEARCH_SERVERS")
	user := viper.GetString("ELASTICSEARCH_USER")
	password := viper.GetString("ELASTICSEARCH_PASSWORD")
	certificate_fingerprint := viper.GetString("ELASTICSEARCH_CERTIFICATE_FINGERPRINT")
	api_key := viper.GetString("ELASTICSEARCH_API_KEY")
	user_agent := viper.GetString("ELASTICSEARCH_USER_AGENT")

	// default to local
	if len(servers) == 0 {
		servers = []string{"https://localhost:9200"}
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

	if user_agent == "" {
		user_agent = "escli/"
	}

	// convert servers into URLs

	urls := make([]*url.URL, len(servers))
	for i, server := range servers {
		url, err0 := url.Parse(server)
		if err0 != nil {
			err = errors.New("invalid Elasticsearch server URL: " + server)
			return
		}
		urls[i] = url
	}

	// create a new client
	cfg := elastictransport.Config{
		URLs:                   urls,
		Username:               user,
		Password:               password,
		CertificateFingerprint: certificate_fingerprint,
		APIKey:                 api_key,
		UserAgent:              user_agent,
	}
	es, err = elastictransport.New(cfg)
	return
}
