package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/gappolicy"
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

	response, err := executeAnalysisQueryRequest(ctx, request)
	if err != nil {
		return db.AnalysisResult{}, wrapElasticError(
			err,
			"failed to execute query against Elasticsearch",
		)
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
				Key any `json:"key"`
				// Maps field name to total of aggregated values
				AggregationTotal map[string]any `json:"aggregation_total"`
				ColumnSplit      struct {
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

func (elastic ElasticsearchDB) buildAnalysisQueryRequest(
	analysis db.AnalysisQuery,
	table string,
) (*search.Search, error) {
	columnSplit, err := createSplit(analysis.ColumnSplit)
	if err != nil {
		return nil, wrap.Error(err, "failed to create column split")
	}

	rowSplit, err := createSplit(analysis.RowSplit)
	if err != nil {
		return nil, wrap.Error(err, "failed to create row split")
	}

	aggregation, err := createAnalysisAggregation(analysis.Aggregation)
	if err != nil {
		return nil, wrap.Error(err, "failed to create aggregation")
	}

	sortOrder, ok := sortOrderToElastic(analysis.RowSplit.SortOrder)
	if !ok {
		return nil, fmt.Errorf("invalid sort order '%v'", analysis.RowSplit.SortOrder)
	}
	bucketSort := &types.BucketSortAggregation{
		Sort: []types.SortCombinations{
			types.SortOptions{SortOptions: map[string]types.FieldSort{
				aggregationTotalName: {Order: &sortOrder},
			}},
		},
		Size:      &analysis.RowSplit.Limit,
		GapPolicy: &gappolicy.Insertzeros,
	}

	columnSplit.Aggregations = map[string]types.Aggregations{
		aggregationName: aggregation,
	}
	rowSplit.Aggregations = map[string]types.Aggregations{
		columnSplitName:                columnSplit,
		aggregationTotalName:           aggregation,
		aggregationTotalName + "_sort": {BucketSort: bucketSort},
	}
	aggregations := map[string]types.Aggregations{
		rowSplitName: rowSplit,
	}

	query := types.NewQuery()
	query.Match["supplierId"] = types.MatchQuery{
		Query: strings.ToUpper("889f32a5-b56c-466b-90c2-3cc0cfc76def"),
	}

	// Size 0, since we only want aggregation results
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations.html#return-only-agg-results
	return elastic.client.Search().Index(table).Aggregations(aggregations).Size(0), nil
}

func createSplit(split db.Split) (types.Aggregations, error) {
	field := split.FieldName
	/* sortOrder, err := sortOrderToElasticBucket(split.SortOrder)
	if err != nil {
		return types.Aggregations{}, err
	} */

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
				/* Order:    sortOrder, */
			}}, nil
		}
	case db.DataTypeTimestamp:
		if split.DateInterval != nil {
			dateInterval, ok := dateIntervalToElastic(*split.DateInterval)
			if !ok {
				return types.Aggregations{}, errors.New("invalid date interval")
			}

			// DateHistogram is a bucket aggregation for date ranges
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-datehistogram-aggregation.html
			return types.Aggregations{DateHistogram: &types.DateHistogramAggregation{
				Field:            &field,
				CalendarInterval: &dateInterval,
				/* Order:            sortOrder, */
			}}, nil
		}
	}

	// Large size, to ensure we get enough values for aggregation
	size := split.Limit*10 + 100

	// If we get here, no interval was specified, so we want to use the 'Terms' bucket aggregation
	// to group by unique values
	// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/search-aggregations-bucket-terms-aggregation.html
	return types.Aggregations{Terms: &types.TermsAggregation{
		Field: &field,
		Size:  &size,
		/* Order: sortOrder, */
	}}, nil
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

func executeAnalysisQueryRequest(
	ctx context.Context,
	request *search.Search,
) (analysisQueryResponse, error) {
	response, err := request.Perform(ctx)
	if err != nil {
		return analysisQueryResponse{}, wrap.Error(err, "failed to send query request")
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
		aggregationTotal, ok := rowSplit.AggregationTotal[analysis.Aggregation.FieldName]
		if !ok {
			return db.AnalysisResult{}, fmt.Errorf(
				"expected aggregation total to have field name '%s' as key, but got %v",
				analysis.Aggregation.FieldName,
				rowSplit.AggregationTotal,
			)
		}

		for _, columnSplit := range rowSplit.ColumnSplit.Buckets {
			resultHandle, err := analysisResult.NewResultHandle()
			if err != nil {
				return db.AnalysisResult{}, wrap.Error(err, "failed to initialize result handle")
			}

			if err := setResultValue(
				resultHandle.Column,
				columnSplit.Key,
				analysisResult.ColumnsMeta.DataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(
					err,
					"failed to set result value for column split",
				)
			}

			if err := setResultValue(
				resultHandle.Row,
				rowSplit.Key,
				analysisResult.RowsMeta.DataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(
					err,
					"failed to set result value for row split",
				)
			}

			if err := setResultValue(
				resultHandle.Total,
				aggregationTotal,
				analysisResult.AggregationDataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(
					err,
					"failed to set result value for aggregation total",
				)
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
				resultHandle.Aggregation,
				aggregatedValue,
				analysisResult.AggregationDataType,
			); err != nil {
				return db.AnalysisResult{}, wrap.Error(
					err,
					"failed to set result value for aggregation",
				)
			}

			if err := analysisResult.ParseResultHandle(resultHandle); err != nil {
				return db.AnalysisResult{}, err
			}
		}
	}

	analysisResult.FillEmptyAggregations()
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
	case db.DataTypeTimestamp:
		if float, isFloat := value.(float64); isFloat {
			// Elasticsearch stores dates as milliseconds since the Unix epoch:
			// https://www.elastic.co/guide/en/elasticsearch/reference/8.10/date.html
			value = time.UnixMilli(int64(float))
		}
	}

	if ok := target.Set(value); !ok {
		return fmt.Errorf("failed to assign '%v' to type %v", value, dataType)
	}

	return nil
}
