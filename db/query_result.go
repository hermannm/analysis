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
	FieldValue       DBValue     `json:"fieldValue"`
	AggregatedValues DBValueList `json:"aggregatedValues"`
}

type ColumnResult struct {
	FieldValue DBValue `json:"fieldValue"`
}

type ResultHandle struct {
	ColumnValue      DBValue
	RowValue         DBValue
	ValueAggregation DBValue
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
	handle.ColumnValue, err = NewDBValue(queryResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize column value")
	}

	handle.RowValue, err = NewDBValue(queryResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize row value")
	}

	handle.ValueAggregation, err = NewDBValue(queryResult.ValueAggregationDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize value aggregation")
	}

	return handle, nil
}

func (queryResult *QueryResult) ParseResult(handle ResultHandle) error {
	if err := queryResult.InitializeColumnResult(handle.ColumnValue.Value()); err != nil {
		return wrap.Error(err, "failed to parse column result")
	}

	rowResult, rowResultIndex, err := queryResult.GetOrCreateRowResult(handle.RowValue.Value())
	if err != nil {
		return wrap.Error(err, "failed to parse row result")
	}

	ok := rowResult.AggregatedValues.Insert(
		queryResult.currentColumnIndex(),
		handle.ValueAggregation.Value(),
	)
	if !ok {
		return errors.New("failed to append value aggregation from result handle")
	}

	queryResult.Rows[rowResultIndex] = rowResult
	return nil
}

func (queryResult *QueryResult) GetOrCreateRowResult(
	rowValue any,
) (rowResult RowResult, index int, err error) {
	for i, candidate := range queryResult.Rows {
		if candidate.FieldValue.Equals(rowValue) {
			return candidate, i, nil
		}
	}

	baseColumnValue, err := NewDBValue(queryResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return RowResult{}, 0, wrap.Error(err, "failed to initialize row field value")
	}
	if ok := baseColumnValue.Set(rowValue); !ok {
		return RowResult{}, 0, fmt.Errorf(
			"failed to set row field value of type %v to '%v'",
			queryResult.RowsMeta.BaseColumnDataType,
			rowValue,
		)
	}

	aggregatedValues, err := NewDBValueList(
		queryResult.ValueAggregationDataType,
		queryResult.ColumnsMeta.Limit,
	)
	if err != nil {
		return RowResult{}, 0, wrap.Error(err, "failed to initialize query result values list")
	}

	rowResult = RowResult{
		FieldValue:       baseColumnValue,
		AggregatedValues: aggregatedValues,
	}
	queryResult.Rows = append(queryResult.Rows, rowResult)
	return rowResult, len(queryResult.Rows) - 1, nil
}

func (queryResult *QueryResult) InitializeColumnResult(columnValue any) error {
	if len(queryResult.Columns) > 0 {
		currentColumnValue := queryResult.Columns[queryResult.currentColumnIndex()].FieldValue
		if currentColumnValue.Equals(columnValue) {
			return nil
		}
	}

	fieldValue, err := NewDBValue(queryResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return wrap.Error(err, "failed to initialize column field value")
	}
	if ok := fieldValue.Set(columnValue); !ok {
		return fmt.Errorf(
			"failed to set column field value of type %v to '%v'",
			queryResult.ColumnsMeta.BaseColumnDataType,
			columnValue,
		)
	}

	columnResult := ColumnResult{FieldValue: fieldValue}
	queryResult.Columns = append(queryResult.Columns, columnResult)
	return nil
}

func (queryResult *QueryResult) TruncateValuesForInsufficientColumns() {
	columnCount := len(queryResult.Columns)
	if columnCount < queryResult.ColumnsMeta.Limit {
		for _, row := range queryResult.Rows {
			row.AggregatedValues.Truncate(columnCount)
		}
	}
}

func (queryResult *QueryResult) currentColumnIndex() int {
	return len(queryResult.Columns) - 1
}
