package elasticsearch

import (
	"context"
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (elastic ElasticsearchDB) CreateStoredSchemasTable(ctx context.Context) error {
	mappings := new(types.TypeMapping)
	mappings.Properties = make(map[string]types.Property, 4)

	// Array fields in Elasticsearch don't have their own mapping: any field can contain multiple
	// values of that type (see https://www.elastic.co/guide/en/elasticsearch/reference/8.10/array.html).
	mappings.Properties[db.StoredSchemaColumnNames] = types.NewTextProperty()
	mappings.Properties[db.StoredSchemaColumnDataTypes] = types.NewByteNumberProperty()
	mappings.Properties[db.StoredSchemaColumnOptionals] = types.NewBooleanProperty()

	const elasticResourceAlreadyExistsException = "resource_already_exists_exception"

	_, err := elastic.client.Indices.Create(db.StoredSchemasTable).Mappings(mappings).Do(ctx)
	if err != nil {
		elasticErr, isElasticErr := err.(*types.ElasticsearchError)
		if isElasticErr && elasticErr.ErrorCause.Type == elasticResourceAlreadyExistsException {
			return nil
		}

		return wrapElasticError(err, "Elasticsearch index creation request failed")
	}

	return nil
}

func (elastic ElasticsearchDB) StoreTableSchema(
	ctx context.Context,
	schema db.TableSchema,
) error {
	storedSchema := schema.ToStored()

	_, err := elastic.client.Create(db.StoredSchemasTable, schema.TableName).
		Document(storedSchema).
		Do(ctx)
	if err != nil {
		return wrapElasticError(err, "Elasticsearch schema document creation request failed")
	}

	return nil
}

func (elastic ElasticsearchDB) GetTableSchema(
	ctx context.Context,
	table string,
) (db.TableSchema, error) {
	schemaIndex, err := elastic.client.Get(db.StoredSchemasTable, table).Do(ctx)
	if err != nil {
		return db.TableSchema{}, wrapElasticError(
			err,
			"Elasticsearch schema document get request failed",
		)
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

func (elastic ElasticsearchDB) DeleteTableSchema(ctx context.Context, table string) error {
	if _, err := elastic.client.Delete(db.StoredSchemasTable, table).Do(ctx); err != nil {
		return wrap.Error(err, "Elasticsearch schema document deletion request failed")
	}

	return nil
}
