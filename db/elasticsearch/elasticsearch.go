package elasticsearch

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8"
	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/config"
	"hermannm.dev/wrap"
)

type ElasticsearchDB struct {
	client *elasticsearch.TypedClient
}

func NewElasticsearchDB(config config.Config) (ElasticsearchDB, error) {
	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses:         []string{config.Elasticsearch.Address},
		EnableDebugLogger: config.Elasticsearch.Debug,
	})
	if err != nil {
		return ElasticsearchDB{}, wrap.Error(err, "failed to connect to Elasticsearch")
	}

	return ElasticsearchDB{client: client}, nil
}

const elasticIndexNotFoundException = "index_not_found_exception"

func (elastic ElasticsearchDB) DropTable(
	ctx context.Context,
	index string,
) (alreadyDropped bool, err error) {
	if _, err := elastic.client.Indices.Delete(index).Do(ctx); err != nil {
		elasticErr, isElasticErr := err.(*elastictypes.ElasticsearchError)
		if isElasticErr && elasticErr.ErrorCause.Type == elasticIndexNotFoundException {
			return true, nil
		}

		return false, wrap.Error(err, "delete index request failed")
	}

	return false, nil
}
