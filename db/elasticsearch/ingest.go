package elasticsearch

import (
	"context"
	"errors"

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
		return wrap.Errorf(err, "elasticsearch index creation request failed to table '%s'", table)
	}

	return nil
}

func (elastic ElasticsearchDB) UpdateTableData(
	ctx context.Context,
	table string,
	schema db.TableSchema,
	data db.DataSource,
) error {
	return errors.New("not implemented")
}
