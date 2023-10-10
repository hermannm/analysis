package elasticsearch

import (
	"context"
	"errors"

	"hermannm.dev/analysis/db"
)

func (elastic ElasticsearchDB) Query(
	ctx context.Context,
	query db.Query,
	table string,
) (db.QueryResult, error) {
	return db.QueryResult{}, errors.New("not implemented")
}
