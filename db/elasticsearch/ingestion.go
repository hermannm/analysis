package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/elastic/go-elasticsearch/v8/esutil"
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

const BulkInsertSize = 1000

func (elastic ElasticsearchDB) UpdateTableData(
	ctx context.Context,
	table string,
	schema db.TableSchema,
	data db.DataSource,
) error {
	bulk, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: elastic.untypedClient,
		Index:  table,
	})
	if err != nil {
		return wrap.Error(err, "failed to prepare bulk data insert")
	}

	ctx, cancel := context.WithCancelCause(ctx)

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

		if err := bulk.Add(ctx, esutil.BulkIndexerItem{
			DocumentID: idString,
			Body:       bytes.NewReader(rowJSON),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, response esutil.BulkIndexerResponseItem, err error) {
				cancel(wrap.Errorf(err, "failed to insert row %d", rowNumber))
			},
		}); err != nil {
			return wrap.Errorf(err, "failed to add row %d to bulk insert", rowNumber)
		}
	}

	if err := bulk.Close(ctx); err != nil {
		return wrap.Error(err, "failed to finish bulk insert")
	}

	if err := ctx.Err(); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		} else {
			return wrap.Error(err, "bulk insert was canceled with error")
		}
	}

	return nil
}
