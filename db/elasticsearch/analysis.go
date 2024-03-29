package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (elastic ElasticsearchDB) RunAnalysisQuery(
	ctx context.Context,
	analysis db.AnalysisQuery,
	table string,
) (db.AnalysisResult, error) {
	query, err := elastic.translateAnalysisQuery(analysis, table)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "failed to parse query")
	}

	response, err := executeAnalysisQuery(ctx, query)
	if err != nil {
		return db.AnalysisResult{}, wrapElasticError(err, "failed to execute query")
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
	aggregationName      = "aggregation"
	aggregationTotalName = "aggregation_total"
)

type analysisQueryResponse struct {
	Aggregations struct {
		RowSplit struct {
			Buckets []struct {
				Key         any `json:"key"`
				ColumnSplit struct {
					Buckets []struct {
						Key any `json:"key"`
						// Maps field name to the aggregated value
						Aggregation map[string]any `json:"aggregation"`
					} `json:"buckets"`
				} `json:"column_split"`
			} `json:"buckets"`
		} `json:"row_split"`
	} `json:"aggregations"`
}

func (elastic ElasticsearchDB) translateAnalysisQuery(
	analysis db.AnalysisQuery,
	table string,
) (*search.Search, error) {
	analysisAggregation, err := createAnalysisAggregation(analysis.Aggregation)
	if err != nil {
		return nil, wrap.Error(err, "failed to create aggregation")
	}

	rowSplit, err := createSplit(analysis.RowSplit, aggregationTotalName)
	if err != nil {
		return nil, wrap.Error(err, "failed to create row split")
	}

	columnSplit, err := createSplit(analysis.ColumnSplit, "_key")
	if err != nil {
		return nil, wrap.Error(err, "failed to create column split")
	}

	columnSplit.Aggregations = map[string]types.Aggregations{
		aggregationName: analysisAggregation,
	}
	rowSplit.Aggregations = map[string]types.Aggregations{
		columnSplitName:      columnSplit,
		aggregationTotalName: analysisAggregation,
	}
	aggregations := map[string]types.Aggregations{
		rowSplitName: rowSplit,
	}

	// Size 0, since we only want aggregation results
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations.html#return-only-agg-results
	return elastic.client.Search().Index(table).Aggregations(aggregations).Size(0), nil
}

func createAnalysisAggregation(aggregation db.Aggregation) (types.Aggregations, error) {
	if err := aggregation.DataType.IsValidForAggregation(); err != nil {
		return types.Aggregations{}, err
	}

	field := aggregation.FieldName

	switch aggregation.Kind {
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

func createSplit(split db.Split, orderKey string) (types.Aggregations, error) {
	field := split.FieldName

	sortOrder, ok := sortOrderToElastic(split.SortOrder)
	if !ok {
		return types.Aggregations{}, fmt.Errorf("invalid sort order '%v'", split.SortOrder)
	}
	orderField := map[string]sortorder.SortOrder{
		orderKey: sortOrder,
	}

	switch split.DataType {
	case db.DataTypeInt, db.DataTypeFloat:
		isInt := split.DataType == db.DataTypeInt

		if (isInt && split.IntegerInterval != 0) || (!isInt && split.FloatInterval != 0) {
			var interval types.Float64
			if isInt {
				interval = types.Float64(split.IntegerInterval)
			} else {
				interval = types.Float64(split.FloatInterval)
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
				Order:    orderField,
			}}, nil
		}
	case db.DataTypeDateTime:
		if !split.DateInterval.IsNone() {
			dateInterval, ok := dateIntervalToElastic(split.DateInterval)
			if !ok {
				return types.Aggregations{}, errors.New("invalid date interval")
			}

			// DateHistogram is a bucket aggregation for date ranges
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-datehistogram-aggregation.html
			return types.Aggregations{DateHistogram: &types.DateHistogramAggregation{
				Field:            &field,
				CalendarInterval: &dateInterval,
				Order:            orderField,
			}}, nil
		}
	}

	// If we get here, no interval was specified, so we want to use the Terms bucket aggregation to
	// group by unique values
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-terms-aggregation.html

	// By default, Elasticsearch only fetches the top (size * 1.5 + 10) terms from each shard
	// (https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-terms-aggregation.html#search-aggregations-bucket-terms-aggregation-shard-size)
	//
	// This can lead to some terms which _are_ part of the global top X terms, but not the top X
	// terms on a specific shard, not being included. Thus, we set a larger shard size to reduce the
	// chance of incorrect results.
	shardSize := split.Limit*10 + 100

	return types.Aggregations{Terms: &types.TermsAggregation{
		Field:     &field,
		Size:      &split.Limit,
		ShardSize: &shardSize,
		Order:     orderField,
	}}, nil
}

func executeAnalysisQuery(
	ctx context.Context,
	query *search.Search,
) (analysisQueryResponse, error) {
	response, err := query.Perform(ctx)
	if err != nil {
		return analysisQueryResponse{}, wrap.Error(err, "failed to send query to Elasticsearch")
	}
	defer response.Body.Close()

	if response.StatusCode > 299 {
		elasticErr := types.NewElasticsearchError()
		if err := json.NewDecoder(response.Body).Decode(elasticErr); err != nil {
			return analysisQueryResponse{}, wrap.Error(
				err,
				"failed to decode error from Elasticsearch",
			)
		}

		if elasticErr.Status == 0 {
			elasticErr.Status = response.StatusCode
		}

		return analysisQueryResponse{}, elasticErr
	}

	var decodedResponse analysisQueryResponse
	if err := json.NewDecoder(response.Body).Decode(&decodedResponse); err != nil {
		return analysisQueryResponse{}, wrap.Error(
			err,
			"failed to decode response from Elasticsearch",
		)
	}

	return decodedResponse, nil
}

func parseAnalysisQueryResponse(
	response analysisQueryResponse,
	analysis db.AnalysisQuery,
) (db.AnalysisResult, error) {
	analysisResult := db.NewAnalysisQueryResult(analysis)

	for _, rowSplit := range response.Aggregations.RowSplit.Buckets {
		for _, columnSplit := range rowSplit.ColumnSplit.Buckets {
			handle, err := analysisResult.NewResultHandle()
			if err != nil {
				return db.AnalysisResult{}, wrap.Error(err, "failed to initialize result handle")
			}

			aggregatedValue, ok := columnSplit.Aggregation[analysis.Aggregation.FieldName]
			if !ok {
				return db.AnalysisResult{}, fmt.Errorf(
					"expected aggregation result to have field name '%s' as key, but got %v",
					analysis.Aggregation.FieldName,
					columnSplit.Aggregation,
				)
			}
			if err := setResultValue(
				handle.Aggregation,
				aggregatedValue,
				analysisResult.AggregationDataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(err, "failed to set aggregation result")
			}

			if err := setResultValue(
				handle.Row,
				rowSplit.Key,
				analysisResult.RowsMeta.DataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(err, "failed to set row result")
			}

			if err := setResultValue(
				handle.Column,
				columnSplit.Key,
				analysisResult.ColumnsMeta.DataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(err, "failed to set column result")
			}

			if err := analysisResult.ParseResultHandle(handle); err != nil {
				return db.AnalysisResult{}, err
			}
		}
	}

	if err := analysisResult.Finalize(); err != nil {
		return db.AnalysisResult{}, err
	}
	return analysisResult, nil
}

func setResultValue(target db.DBValue, value any, dataType db.DataType) error {
	// Deserializing from JSON to any makes all numeric types floating-point, so we have to convert
	// them back to integers here before setting the value
	switch dataType {
	case db.DataTypeInt:
		if float, isFloat := value.(float64); isFloat {
			value = int64(float)
		}
	case db.DataTypeDateTime:
		if float, isFloat := value.(float64); isFloat {
			// Elasticsearch stores dates as milliseconds since the Unix epoch:
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/date.html
			value = time.UnixMilli(int64(float)).UTC()
		}
	}

	if ok := target.Set(value); !ok {
		return fmt.Errorf("failed to assign '%v' to type %v", value, dataType)
	}

	return nil
}
