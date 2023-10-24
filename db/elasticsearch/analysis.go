package elasticsearch

import (
	"context"
	"errors"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (elastic ElasticsearchDB) RunAnalysisQuery(
	ctx context.Context,
	analysis db.AnalysisQuery,
	table string,
) (db.AnalysisResult, error) {
	request, err := elastic.buildAnalysisQueryRequest(analysis)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "failed to parse query")
	}

	response, err := request.Do(ctx)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "Elasticsearch failed to execute query")
	}

	analysisResult, err := parseAnalysisQueryResponse(response, analysis)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "failed to parse query result")
	}

	return analysisResult, nil
}

func (elastic ElasticsearchDB) buildAnalysisQueryRequest(
	analysis db.AnalysisQuery,
) (*search.Search, error) {
	aggregations := make(map[string]elastictypes.Aggregations, 3)
	query := elastictypes.NewQuery()

	return elastic.client.Search().Query(query).Aggregations(aggregations), nil
}

func parseAnalysisQueryResponse(
	response *search.Response,
	analysis db.AnalysisQuery,
) (db.AnalysisResult, error) {
	return db.AnalysisResult{}, errors.New("not implemented")
}
