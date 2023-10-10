package elasticsearch

import (
	"context"
	"errors"

	"hermannm.dev/analysis/db"
)

func (elastic ElasticsearchDB) GetTableSchema(
	ctx context.Context,
	table string,
) (schema db.TableSchema, err error) {
	return db.TableSchema{}, errors.New("not implemented")
}
