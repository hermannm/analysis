package elasticsearch

import (
	"context"
	"encoding/json"

	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/google/uuid"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (elastic ElasticsearchDB) CreateTable(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	mappings, err := schemaToElasticMappings(schema)
	if err != nil {
		return wrap.Error(err, "failed to translate table schema to elastic mappings")
	}

	if _, err = elastic.client.Indices.Create(table).Mappings(mappings).Do(ctx); err != nil {
		return wrap.Errorf(err, "Elasticsearch index creation request failed for table '%s'", table)
	}

	return nil
}

func (elastic ElasticsearchDB) UpdateTableData(
	ctx context.Context,
	table string,
	schema db.TableSchema,
	data db.DataSource,
) error {
	bulk := elastic.client.Bulk()

	for {
		row, rowNumber, done, err := data.ReadRow()
		if done {
			break
		}
		if err != nil {
			return wrap.Error(err, "failed to read row")
		}

		id, err := uuid.NewUUID()
		if err != nil {
			return wrap.Errorf(err, "failed to generate unique ID for row %d", rowNumber)
		}
		idString := id.String()

		operation := elastictypes.CreateOperation{
			Id_:    &idString,
			Index_: &table,
		}

		rowMap, err := schema.ConvertRowToMap(row)
		if err != nil {
			return wrap.Errorf(
				err,
				"failed to convert row %d to data types expected by table schema",
				rowNumber,
			)
		}

		rowJSON, err := json.Marshal(rowMap)
		if err != nil {
			return wrap.Errorf(
				err,
				"failed to encode row %d to JSON for sending to Elasticsearch",
				rowNumber,
			)
		}

		if err := bulk.CreateOp(operation, rowJSON); err != nil {
			return wrap.Errorf(
				err,
				"failed to add create operation for row %d to bulk insert",
				rowNumber,
			)
		}
	}

	if _, err := bulk.Do(ctx); err != nil {
		return wrap.Error(err, "bulk insert request failed")
	}

	return nil
}
