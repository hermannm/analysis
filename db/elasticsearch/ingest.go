package elasticsearch

import (
	"context"
	"errors"

	"hermannm.dev/analysis/db"
)

func (elastic ElasticsearchDB) CreateTable(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	return errors.New("not implemented")
}

func (elastic ElasticsearchDB) UpdateTableData(
	ctx context.Context,
	table string,
	schema db.TableSchema,
	data db.DataSource,
) error {
	return errors.New("not implemented")
}
