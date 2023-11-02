package elasticsearch

import (
	"context"
	"errors"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (elastic ElasticsearchDB) RunAnalysisQuery(
	ctx context.Context,
	analysis db.AnalysisQuery,
	table string,
) (db.AnalysisResult, error) {
	request, err := elastic.buildAnalysisQueryRequest(analysis, table)
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

const (
	columnSplitName      = "column_split"
	rowSplitName         = "row_split"
	valueAggregationName = "value_aggregation"
)

func (elastic ElasticsearchDB) buildAnalysisQueryRequest(
	analysis db.AnalysisQuery,
	table string,
) (*search.Search, error) {
	rowSplit, err := createSplit(analysis.RowSplit)
	if err != nil {
		return nil, wrap.Error(err, "failed to create row split")
	}

	columnSplit, err := createSplit(analysis.ColumnSplit)
	if err != nil {
		return nil, wrap.Error(err, "failed to create column split")
	}

	valueAggregation, err := createValueAggregation(analysis.ValueAggregation)
	if err != nil {
		return nil, wrap.Error(err, "failed to create value aggregation")
	}

	columnSplit.Aggregations = map[string]types.Aggregations{
		valueAggregationName: valueAggregation,
	}
	rowSplit.Aggregations = map[string]types.Aggregations{
		columnSplitName: columnSplit,
	}
	aggregations := map[string]types.Aggregations{
		rowSplitName: rowSplit,
	}

	// Size 0, since we only want aggregation results
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations.html#return-only-agg-results
	return elastic.client.Search().Index(table).Aggregations(aggregations).Size(0), nil
}

func createSplit(split db.Split) (types.Aggregations, error) {
	field := split.BaseColumnName

	switch split.BaseColumnDataType {
	case db.DataTypeInt, db.DataTypeFloat:
		isInt := split.BaseColumnDataType == db.DataTypeInt

		if (isInt && split.IntegerInterval != 0) || (!isInt && split.FloatInterval != 0) {
			var interval types.Float64
			if isInt {
				interval = types.Float64(split.IntegerInterval)
			} else {
				interval = types.Float64(split.FloatInterval)
			}

			sortOrder, err := sortOrderToElasticBucket(split.SortOrder)
			if err != nil {
				return types.Aggregations{}, err
			}

			// Histogram is a bucket aggregation for number ranges
			// The intervals are placed in buckets by value based on this formula:
			//   Math.floor((value - offset) / interval) * interval + offset
			// Since we don't give an offset, this is the same formula as the one we use for
			// ClickHouse (see clickhouse/query_builder.go -> QueryBuilder.WriteSplit)
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-histogram-aggregation.html
			return types.Aggregations{Histogram: &types.HistogramAggregation{
				Field:    &field,
				Interval: &interval,
				Order:    sortOrder,
			}}, nil
		}
	case db.DataTypeTimestamp:
		if split.DateInterval != nil {
			dateInterval, ok := dateIntervalToElastic(*split.DateInterval)
			if !ok {
				return types.Aggregations{}, errors.New("invalid date interval")
			}

			sortOrder, err := sortOrderToElasticBucket(split.SortOrder)
			if err != nil {
				return types.Aggregations{}, err
			}

			// DateHistogram is a bucket aggregation for date ranges
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-datehistogram-aggregation.html
			return types.Aggregations{DateHistogram: &types.DateHistogramAggregation{
				Field:            &field,
				CalendarInterval: &dateInterval,
				Order:            sortOrder,
			}}, nil
		}
	}

	// If we get here, no interval was specified, so we want to use the 'Terms' bucket aggregation
	// to group by unique values
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-terms-aggregation.html
	terms := types.NewTermsAggregation()
	terms.Field = &field
	return types.Aggregations{Terms: terms}, nil
}

func createValueAggregation(valueAggregation db.ValueAggregation) (types.Aggregations, error) {
	if err := valueAggregation.BaseColumnDataType.IsValidForAggregation(); err != nil {
		return types.Aggregations{}, err
	}

	field := valueAggregation.BaseColumnName

	switch valueAggregation.Aggregation {
	case db.AggregationSum:
		return types.Aggregations{Sum: &types.SumAggregation{Field: &field}}, nil
	case db.AggregationAverage:
		return types.Aggregations{Avg: &types.AverageAggregation{Field: &field}}, nil
	case db.AggregationMin:
		return types.Aggregations{Min: &types.MinAggregation{Field: &field}}, nil
	case db.AggregationMax:
		return types.Aggregations{Max: &types.MaxAggregation{Field: &field}}, nil
	case db.AggregationCount:
		return types.Aggregations{Cardinality: &types.CardinalityAggregation{Field: &field}}, nil
	default:
		return types.Aggregations{}, errors.New("invalid aggregation type")
	}
}

func parseAnalysisQueryResponse(
	response *search.Response,
	analysis db.AnalysisQuery,
) (db.AnalysisResult, error) {
	return db.AnalysisResult{}, errors.New("not implemented")
}
