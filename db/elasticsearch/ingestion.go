package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (elastic ElasticsearchDB) CreateTable(ctx context.Context, schema db.TableSchema) error {
	mappings, err := schemaToElasticMappings(schema)
	if err != nil {
		return wrap.Error(err, "failed to translate table schema to Elasticsearch mappings")
	}

	_, err = elastic.client.Indices.Create(schema.TableName).Mappings(mappings).Do(ctx)
	if err != nil {
		return wrapElasticErrorf(
			err,
			"Elasticsearch index creation request failed for table '%s'",
			schema.TableName,
		)
	}

	return nil
}

func (elastic ElasticsearchDB) InsertTableData(
	ctx context.Context,
	schema db.TableSchema,
	data db.DataSource,
) error {
	ctx, cancel := context.WithCancelCause(ctx)

	bulk, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: elastic.untypedClient,
		Index:  schema.TableName,
		OnError: func(ctx context.Context, err error) {
			cancel(formatElasticError(extractElasticError(err)))
		},
	})
	if err != nil {
		return wrap.Error(err, "failed to prepare bulk data insert")
	}

	for {
		row, rowNumber, done, err := data.ReadRow()
		if done {
			break
		}
		if err != nil {
			cancel(wrap.Error(err, "failed to read row"))
			break
		}

		id, err := uuid.NewUUID()
		if err != nil {
			cancel(wrap.Errorf(err, "failed to generate unique ID for row %d", rowNumber))
			break
		}
		idString := id.String()

		rowMap, err := schema.ConvertRowToMap(row)
		if err != nil {
			cancel(wrap.Errorf(
				err,
				"failed to convert row %d to data types expected by table schema",
				rowNumber,
			))
			break
		}

		rowJSON, err := json.Marshal(rowMap)
		if err != nil {
			cancel(wrap.Errorf(
				err,
				"failed to encode row %d to JSON for sending to Elasticsearch",
				rowNumber,
			))
			break
		}

		if err := bulk.Add(ctx, esutil.BulkIndexerItem{
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/docs-bulk.html#docs-bulk-api-desc
			Action:     "create",
			DocumentID: idString,
			Body:       bytes.NewReader(rowJSON),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, response esutil.BulkIndexerResponseItem, err error) {
				cancel(wrap.Errorf(err, "failed to insert row %d", rowNumber))
			},
		}); err != nil {
			if err != context.Canceled {
				cancel(wrap.Errorf(err, "failed to add row %d to bulk insert", rowNumber))
			}
			break
		}
	}

	if err := bulk.Close(ctx); err != nil {
		cause := context.Cause(ctx)
		if cause == nil {
			return wrap.Error(err, "failed to finish Elasticsearch bulk insert")
		} else if cause == context.Canceled {
			//lint:ignore ST1005 Names in errors should still be capitalized
			return errors.New("Elasticsearch bulk insert was canceled before completion")
		} else {
			return wrap.Error(cause, "Elasticsearch bulk insert was canceled by error")
		}
	}

	return nil
}
