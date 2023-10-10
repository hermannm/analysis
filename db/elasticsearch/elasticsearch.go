package elasticsearch

import (
	"github.com/elastic/go-elasticsearch/v8"
	"hermannm.dev/analysis/config"
	"hermannm.dev/wrap"
)

type ElasticsearchDB struct {
	client *elasticsearch.Client
}

func NewElasticsearchDB(config config.Config) (ElasticsearchDB, error) {
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:         []string{config.Elasticsearch.Address},
		EnableDebugLogger: config.Elasticsearch.Debug,
	})
	if err != nil {
		return ElasticsearchDB{}, wrap.Error(err, "failed to connect to Elasticsearch")
	}

	return ElasticsearchDB{client: client}, nil
}
