package db

import (
	"errors"
	"fmt"

	"hermannm.dev/wrap"
)

type QueryResult struct {
	ValueAggregationDataType DataType `json:"valueAggregationDataType"`

	Rows     []RowResult `json:"rows"`
	RowsMeta Split       `json:"rowsMeta"`

	Columns     []ColumnResult `json:"columns"`
	ColumnsMeta Split          `json:"columnsMeta"`
}

type RowResult struct {
	BaseColumnValue  DynamicValue `json:"baseColumnValue"`
	AggregatedValues DynamicList  `json:"aggregatedValues"`
}

type ColumnResult struct {
	BaseColumnValue DynamicValue `json:"baseColumnValue"`
}

type ResultHandle struct {
	ColumnValue      DynamicValue
	RowValue         DynamicValue
	ValueAggregation DynamicValue
}

func NewQueryResult(query Query) QueryResult {
	return QueryResult{
		ValueAggregationDataType: query.ValueAggregation.BaseColumnDataType,
		Rows:                     make([]RowResult, 0, query.RowSplit.Limit),
		RowsMeta:                 query.RowSplit,
		Columns:                  make([]ColumnResult, 0, query.ColumnSplit.Limit),
		ColumnsMeta:              query.ColumnSplit,
	}
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

func (queryResult *QueryResult) ParseResult(handle ResultHandle) error {
	if err := queryResult.InitializeColumnResult(handle.ColumnValue.Value()); err != nil {
		return wrap.Error(err, "failed to parse column result")
	}

	if err := queryResult.InitializeRowResult(handle.RowValue.Value()); err != nil {
		return wrap.Error(err, "failed to parse row result")
	}

	rowResult, index, hasResults := queryResult.LatestRowResult()
	if !hasResults {
		return errors.New("row results were empty after initialization")
	}

	if ok := rowResult.AggregatedValues.Append(handle.ValueAggregation.Value()); !ok {
		return errors.New("failed to append value aggregation from result handle")
	}

	queryResult.Rows[index] = rowResult
	return nil
}

func (queryResult *QueryResult) InitializeRowResult(rowValue any) error {
	latestRowResult, _, hasLatest := queryResult.LatestRowResult()
	if hasLatest && latestRowResult.BaseColumnValue.Equals(rowValue) {
		return nil
	}

	baseColumnValue, err := NewDynamicValue(queryResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return wrap.Error(err, "failed to initialize base column value")
	}
	if ok := baseColumnValue.Set(rowValue); !ok {
		return fmt.Errorf(
			"failed to set base column value of type %v to value '%v'",
			queryResult.RowsMeta.BaseColumnDataType,
			rowValue,
		)
	}

	aggregatedValues, err := NewDynamicList(
		queryResult.ValueAggregationDataType,
		queryResult.ColumnsMeta.Limit,
	)
	if err != nil {
		return wrap.Error(err, "failed to initialize query result values list")
	}

	rowResult := RowResult{
		BaseColumnValue:  baseColumnValue,
		AggregatedValues: aggregatedValues,
	}
	queryResult.Rows = append(queryResult.Rows, rowResult)
	return nil
}

func (queryResult *QueryResult) InitializeColumnResult(columnValue any) error {
	latestColumnResult, _, hasLatest := queryResult.LatestColumnResult()
	if hasLatest && latestColumnResult.BaseColumnValue.Equals(columnValue) {
		return nil
	}

	baseColumnValue, err := NewDynamicValue(queryResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return wrap.Error(err, "failed to initialize base column value")
	}
	if ok := baseColumnValue.Set(columnValue); !ok {
		return fmt.Errorf(
			"failed to set base column value of type %v to value '%v'",
			queryResult.ColumnsMeta.BaseColumnDataType,
			columnValue,
		)
	}

	columnResult := ColumnResult{BaseColumnValue: baseColumnValue}
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
