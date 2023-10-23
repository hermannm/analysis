package elasticsearch

import (
	"context"
	"errors"

	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

const (
	schemaIndex           = "analysis_schemas"
	schemaName            = "name"
	schemaColumnNames     = "column_names"
	schemaColumnDataTypes = "column_data_types"
	schemaColumnOptionals = "column_optionals"
)

const elasticResourceAlreadyExistsException = "resource_already_exists_exception"

func (elastic ElasticsearchDB) createSchemaIndex(ctx context.Context) error {
	mappings := new(elastictypes.TypeMapping)
	mappings.Properties = make(map[string]elastictypes.Property, 4)

	mappings.Properties[schemaName] = elastictypes.NewTextProperty()

	// Array fields in Elasticsearch don't have their own mapping: any field can contain multiple
	// values of that type (see https://www.elastic.co/guide/en/elasticsearch/reference/8.10/array.html).
	mappings.Properties[schemaColumnNames] = elastictypes.NewTextProperty()
	mappings.Properties[schemaColumnDataTypes] = elastictypes.NewIntegerNumberProperty()
	mappings.Properties[schemaColumnOptionals] = elastictypes.NewBooleanProperty()

	if _, err := elastic.client.Indices.Create(schemaIndex).Mappings(mappings).Do(ctx); err != nil {
		elasticErr, isElasticErr := err.(*elastictypes.ElasticsearchError)
		if isElasticErr && elasticErr.ErrorCause.Type == elasticResourceAlreadyExistsException {
			return nil
		}

		return wrap.Error(err, "Elasticsearch index creation request failed")
	}

	return nil
}

func (elastic ElasticsearchDB) GetTableSchema(
	ctx context.Context,
	table string,
) (schema db.TableSchema, err error) {
	return db.TableSchema{}, errors.New("not implemented")
}
