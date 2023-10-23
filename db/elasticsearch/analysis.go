package elasticsearch

import (
	"context"
	"errors"

	"hermannm.dev/analysis/db"
)

func (elastic ElasticsearchDB) RunAnalysisQuery(
	ctx context.Context,
	query db.AnalysisQuery,
	table string,
) (db.AnalysisResult, error) {
	return db.AnalysisResult{}, errors.New("not implemented")
}
