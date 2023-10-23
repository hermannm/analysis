package elasticsearch

import (
	"context"

	"github.com/elastic/go-elasticsearch/v8"
	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/log"
	"hermannm.dev/wrap"
)

type ElasticsearchDB struct {
	client *elasticsearch.TypedClient
}

func NewElasticsearchDB(config config.Config) (ElasticsearchDB, error) {
	ctx := context.Background()

	client, err := elasticsearch.NewTypedClient(elasticsearch.Config{
		Addresses:         []string{config.Elasticsearch.Address},
		EnableDebugLogger: config.Elasticsearch.Debug,
	})
	if err != nil {
		return ElasticsearchDB{}, wrap.Error(err, "failed to connect to Elasticsearch")
	}

	elastic := ElasticsearchDB{client: client}

	if err := elastic.createSchemaIndex(ctx); err != nil {
		return ElasticsearchDB{}, wrap.Error(err, "failed to create schema index")
	}

	indexToDrop := config.DropTableOnStartup
	if indexToDrop != "" && !config.IsProduction {
		alreadyDropped, err := elastic.deleteIndex(ctx, indexToDrop)
		if err != nil {
			log.Errorf(
				err,
				"failed to drop table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)",
				indexToDrop,
			)
		} else if !alreadyDropped {
			log.Infof("dropped table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)", indexToDrop)
		}
	}

	return elastic, nil
}

const elasticIndexNotFoundException = "index_not_found_exception"

func (elastic ElasticsearchDB) deleteIndex(
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
