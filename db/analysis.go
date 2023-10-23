package db

import (
	"errors"
	"fmt"

	"hermannm.dev/wrap"
)

type AnalysisQuery struct {
	ValueAggregation ValueAggregation `json:"valueAggregation"`
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

type AnalysisResult struct {
	Rows     []RowResult `json:"rows"`
	RowsMeta Split       `json:"rowsMeta"`

	Columns     []ColumnResult `json:"columns"`
	ColumnsMeta Split          `json:"columnsMeta"`

	ValueAggregationDataType DataType `json:"valueAggregationDataType"`
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

func NewAnalysisQueryResult(analysis AnalysisQuery) AnalysisResult {
	return AnalysisResult{
		Rows:                     make([]RowResult, 0, analysis.RowSplit.Limit),
		RowsMeta:                 analysis.RowSplit,
		Columns:                  make([]ColumnResult, 0, analysis.ColumnSplit.Limit),
		ColumnsMeta:              analysis.ColumnSplit,
		ValueAggregationDataType: analysis.ValueAggregation.BaseColumnDataType,
	}
}

func (analysisResult *AnalysisResult) NewResultHandle() (handle ResultHandle, err error) {
	handle.ColumnValue, err = NewDBValue(analysisResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize column value")
	}

	handle.RowValue, err = NewDBValue(analysisResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize row value")
	}

	handle.ValueAggregation, err = NewDBValue(analysisResult.ValueAggregationDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize value aggregation")
	}

	return handle, nil
}

func (analysisResult *AnalysisResult) ParseResultHandle(handle ResultHandle) error {
	if err := analysisResult.InitializeColumnResult(handle.ColumnValue.Value()); err != nil {
		return wrap.Error(err, "failed to parse column result")
	}

	rowResult, rowResultIndex, err := analysisResult.GetOrCreateRowResult(handle.RowValue.Value())
	if err != nil {
		return wrap.Error(err, "failed to parse row result")
	}

	ok := rowResult.AggregatedValues.Insert(
		analysisResult.currentColumnIndex(),
		handle.ValueAggregation.Value(),
	)
	if !ok {
		return errors.New("failed to insert value aggregation into query result")
	}

	analysisResult.Rows[rowResultIndex] = rowResult
	return nil
}

func (analysisResult *AnalysisResult) GetOrCreateRowResult(
	rowValue any,
) (rowResult RowResult, index int, err error) {
	for i, candidate := range analysisResult.Rows {
		if candidate.FieldValue.Equals(rowValue) {
			return candidate, i, nil
		}
	}

	baseColumnValue, err := NewDBValue(analysisResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return RowResult{}, 0, wrap.Error(err, "failed to initialize row field value")
	}
	if ok := baseColumnValue.Set(rowValue); !ok {
		return RowResult{}, 0, fmt.Errorf(
			"failed to set row field value of type %v to '%v'",
			analysisResult.RowsMeta.BaseColumnDataType,
			rowValue,
		)
	}

	aggregatedValues, err := NewDBValueList(
		analysisResult.ValueAggregationDataType,
		analysisResult.ColumnsMeta.Limit,
	)
	if err != nil {
		return RowResult{}, 0, wrap.Error(err, "failed to initialize query result values list")
	}

	rowResult = RowResult{
		FieldValue:       baseColumnValue,
		AggregatedValues: aggregatedValues,
	}
	analysisResult.Rows = append(analysisResult.Rows, rowResult)
	return rowResult, len(analysisResult.Rows) - 1, nil
}

func (analysisResult *AnalysisResult) InitializeColumnResult(columnValue any) error {
	if len(analysisResult.Columns) > 0 {
		currentColumnValue := analysisResult.Columns[analysisResult.currentColumnIndex()].FieldValue
		if currentColumnValue.Equals(columnValue) {
			return nil
		}
	}

	fieldValue, err := NewDBValue(analysisResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return wrap.Error(err, "failed to initialize column field value")
	}
	if ok := fieldValue.Set(columnValue); !ok {
		return fmt.Errorf(
			"failed to set column field value of type %v to '%v'",
			analysisResult.ColumnsMeta.BaseColumnDataType,
			columnValue,
		)
	}

	columnResult := ColumnResult{FieldValue: fieldValue}
	analysisResult.Columns = append(analysisResult.Columns, columnResult)
	return nil
}

func (analysisResult *AnalysisResult) TruncateColumns() {
	columnCount := len(analysisResult.Columns)
	columnLimit := analysisResult.ColumnsMeta.Limit

	if columnCount == columnLimit {
		return
	} else if columnCount < columnLimit {
		for _, row := range analysisResult.Rows {
			row.AggregatedValues.Truncate(columnCount)
		}
	} else {
		analysisResult.Columns = analysisResult.Columns[:columnLimit]
	}
}

func (analysisResult *AnalysisResult) currentColumnIndex() int {
	return len(analysisResult.Columns) - 1
}
