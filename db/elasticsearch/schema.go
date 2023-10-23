package elasticsearch

import (
	"context"
	"encoding/json"

	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

const elasticResourceAlreadyExistsException = "resource_already_exists_exception"

func (elastic ElasticsearchDB) createSchemaIndex(ctx context.Context) error {
	mappings := new(elastictypes.TypeMapping)
	mappings.Properties = make(map[string]elastictypes.Property, 4)

	// Array fields in Elasticsearch don't have their own mapping: any field can contain multiple
	// values of that type (see https://www.elastic.co/guide/en/elasticsearch/reference/8.10/array.html).
	mappings.Properties[db.StoredSchemaColumnNames] = elastictypes.NewTextProperty()
	mappings.Properties[db.StoredSchemaColumnDataTypes] = elastictypes.NewIntegerNumberProperty()
	mappings.Properties[db.StoredSchemaColumnOptionals] = elastictypes.NewBooleanProperty()

	_, err := elastic.client.Indices.Create(db.StoredSchemasTable).Mappings(mappings).Do(ctx)
	if err != nil {
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
) (db.TableSchema, error) {
	schemaIndex, err := elastic.client.Get(db.StoredSchemasTable, table).Do(ctx)
	if err != nil {
		return db.TableSchema{}, wrap.Error(err, "Elasticsearch schema index get request failed")
	}

	var storedSchema db.StoredTableSchema
	if err := json.Unmarshal(schemaIndex.Source_, &storedSchema); err != nil {
		return db.TableSchema{}, wrap.Error(
			err,
			"failed to parse Elasticsearch response as table schema",
		)
	}

	schema, err := storedSchema.ToSchema()
	if err != nil {
		return db.TableSchema{}, wrap.Error(err, "failed to parse stored table schema")
	}

	return schema, nil
}

func (elastic ElasticsearchDB) storeTableSchema(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	storedSchema := schema.ToStored()

	_, err := elastic.client.Create(db.StoredSchemasTable, table).Document(storedSchema).Do(ctx)
	if err != nil {
		return wrap.Error(err, "Elasticsearch schema create request failed")
	}

	return nil
}
