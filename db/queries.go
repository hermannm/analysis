package db

import (
	"errors"
	"fmt"

	"hermannm.dev/wrap"
)

type Query struct {
	ValueAggregation ValueAggregation `json:"valueAggregations"`
	RowSplit         Split            `json:"rowSplit"`
	ColumnSplit      Split            `json:"columnSplit"`
}

type ValueAggregation struct {
	BaseColumnName     string      `json:"baseColumnName"`
	BaseColumnDataType DataType    `json:"baseColumnDataType"`
	Aggregation        Aggregation `json:"aggregation"`
}

type Split struct {
	BaseColumnName     string    `json:"baseColumnName"`
	BaseColumnDataType DataType  `json:"baseColumnDataType"`
	Limit              int       `json:"limit"`
	SortOrder          SortOrder `json:"sortOrder"`
	// May only be present if BaseColumnDataType is INTEGER.
	IntegerInterval int `json:"numberIntervalInt"`
	// May only be present if BaseColumnDataType is FLOAT.
	FloatInterval float64 `json:"numberIntervalFloat"`
	// May only be present if BaseColumnDataType is TIMESTAMP.
	DateInterval *DateInterval `json:"dateInterval,omitempty"`
}

type QueryResult struct {
	ValueAggregationDataType DataType `json:"valueAggregationDataType"`

	Rows     []RowResult `json:"rows"`
	RowsMeta Split       `json:"rowsMeta"`

	Columns     []ColumnResult `json:"columns"`
	ColumnsMeta Split          `json:"columnsMeta"`
}

type RowResult struct {
	BaseColumnValue DynamicValue `json:"baseColumnValue"`
	Values          DynamicList  `json:"values"`
}

type ColumnResult struct {
	BaseColumnValue DynamicValue `json:"baseColumnValue"`
}

func InitializeQueryResult(query Query) QueryResult {
	return QueryResult{
		ValueAggregationDataType: query.ValueAggregation.BaseColumnDataType,
		Rows:                     make([]RowResult, 0, query.RowSplit.Limit),
		RowsMeta:                 query.RowSplit,
		Columns:                  make([]ColumnResult, 0, query.ColumnSplit.Limit),
		ColumnsMeta:              query.ColumnSplit,
	}
}

func (queryResult *QueryResult) ParseResult(resultHandle ResultHandle) error {
	if err := queryResult.InitializeColumnResult(resultHandle.ColumnValue.Value()); err != nil {
		return wrap.Error(err, "failed to parse column result")
	}

	if err := queryResult.InitializeRowResult(resultHandle.RowValue.Value()); err != nil {
		return wrap.Error(err, "failed to parse row result")
	}

	rowResult, index, hasResults := queryResult.LatestRowResult()
	if !hasResults {
		return errors.New("row results were empty after initialization")
	}

	if ok := rowResult.Values.Append(resultHandle.ValueAggregation.Value()); !ok {
		return errors.New("failed to append value aggregation from result handle")
	}

	queryResult.Rows[index] = rowResult
	return nil
}

func (queryResult *QueryResult) InitializeRowResult(baseColumnValue any) error {
	latestRowResult, _, hasLatest := queryResult.LatestRowResult()
	if hasLatest && latestRowResult.BaseColumnValue.Equals(baseColumnValue) {
		return nil
	}

	dynValue, err := NewDynamicValue(queryResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return wrap.Error(err, "failed to initialize base column value")
	}
	if ok := dynValue.Set(baseColumnValue); !ok {
		return fmt.Errorf(
			"failed to set base column value of type %v to value '%v'",
			queryResult.RowsMeta.BaseColumnDataType,
			baseColumnValue,
		)
	}

	values, err := NewDynamicList(
		queryResult.ValueAggregationDataType,
		queryResult.ColumnsMeta.Limit,
	)
	if err != nil {
		return wrap.Error(err, "failed to initialize query result values list")
	}

	rowResult := RowResult{BaseColumnValue: dynValue, Values: values}
	queryResult.Rows = append(queryResult.Rows, rowResult)
	return nil
}

func (queryResult *QueryResult) InitializeColumnResult(baseColumnValue any) error {
	latestColumnResult, _, hasLatest := queryResult.LatestColumnResult()
	if hasLatest && latestColumnResult.BaseColumnValue.Equals(baseColumnValue) {
		return nil
	}

	dynValue, err := NewDynamicValue(queryResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return wrap.Error(err, "failed to initialize base column value")
	}
	if ok := dynValue.Set(baseColumnValue); !ok {
		return fmt.Errorf(
			"failed to set base column value of type %v to value '%v'",
			queryResult.ColumnsMeta.BaseColumnDataType,
			baseColumnValue,
		)
	}

	columnResult := ColumnResult{BaseColumnValue: dynValue}
	queryResult.Columns = append(queryResult.Columns, columnResult)
	return nil
}

func (queryResult *QueryResult) LatestRowResult() (result RowResult, index int, hasResults bool) {
	if len(queryResult.Rows) == 0 {
		return RowResult{}, 0, false
	}

	index = len(queryResult.Rows) - 1
	return queryResult.Rows[index], index, true
}

func (queryResult *QueryResult) LatestColumnResult() (result ColumnResult, index int, hasResults bool) {
	if len(queryResult.Columns) == 0 {
		return ColumnResult{}, 0, false
	}

	index = len(queryResult.Columns) - 1
	return queryResult.Columns[index], index, true
}

type ResultHandle struct {
	ColumnValue      DynamicValue
	RowValue         DynamicValue
	ValueAggregation DynamicValue
}

func (queryResult *QueryResult) NewResultHandle() (handle ResultHandle, err error) {
	handle.ColumnValue, err = NewDynamicValue(queryResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize column value")
	}

	handle.RowValue, err = NewDynamicValue(queryResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize row value")
	}

	handle.ValueAggregation, err = NewDynamicValue(queryResult.ValueAggregationDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize value aggregation")
	}

	return handle, nil
}
