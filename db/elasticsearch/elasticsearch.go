package elasticsearch

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/config"
	"hermannm.dev/wrap"
)

// Implements db.AnalysisDB for Elasticsearch.
type ElasticsearchDB struct {
	client        *elasticsearch.TypedClient
	untypedClient *elasticsearch.Client
}

func NewElasticsearchDB(config config.Config) (ElasticsearchDB, error) {
	elasticConfig := elasticsearch.Config{
		Addresses:         []string{config.Elasticsearch.Address},
		EnableDebugLogger: config.Elasticsearch.Debug,
	}

	client, err := elasticsearch.NewTypedClient(elasticConfig)
	if err != nil {
		return ElasticsearchDB{}, wrap.Error(err, "failed to connect to Elasticsearch")
	}

	untypedClient, err := elasticsearch.NewClient(elasticConfig)
	if err != nil {
		return ElasticsearchDB{}, wrap.Error(err, "failed to connect untyped API to Elasticsearch")
	}

	return ElasticsearchDB{client: client, untypedClient: untypedClient}, nil
}

func (elastic ElasticsearchDB) DropTable(
	ctx context.Context,
	index string,
) (alreadyDropped bool, err error) {
	// See https://www.elastic.co/guide/en/elasticsearch/reference/8.10/troubleshooting-searches.html#troubleshooting-searches-exists
	const elasticIndexNotFoundException = "index_not_found_exception"

	if _, err := elastic.client.Indices.Delete(index).Do(ctx); err != nil {
		elasticErr, isElasticErr := err.(*types.ElasticsearchError)
		if isElasticErr && elasticErr.ErrorCause.Type == elasticIndexNotFoundException {
			return true, nil
		}

		return false, wrap.Error(err, "Elasticsearch index deletion request failed")
	}

	return false, nil
}
