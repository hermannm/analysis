package db

import (
	"fmt"
	"slices"

	"hermannm.dev/devlog/log"
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
	IntegerInterval int `json:"numberIntervalInt,omitempty"`
	// May only be present if BaseColumnDataType is FLOAT.
	FloatInterval float64 `json:"numberIntervalFloat,omitempty"`
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
	FieldValue       TypedValue     `json:"fieldValue"`
	AggregatedValues TypedValueList `json:"aggregatedValues"`
}

type ColumnResult struct {
	FieldValue TypedValue `json:"fieldValue"`
}

type ResultHandle struct {
	ColumnValue      TypedValue
	RowValue         TypedValue
	ValueAggregation TypedValue
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
	handle.ColumnValue, err = NewTypedValue(analysisResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize column value")
	}

	handle.RowValue, err = NewTypedValue(analysisResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize row value")
	}

	handle.ValueAggregation, err = NewTypedValue(analysisResult.ValueAggregationDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize value aggregation")
	}

	return handle, nil
}

func (analysisResult *AnalysisResult) ParseResultHandle(handle ResultHandle) error {
	log.DebugJSON(handle, "result handle")

	rowResult, err := analysisResult.GetOrCreateRowResult(handle.RowValue.Value())
	if err != nil {
		return wrap.Error(err, "failed to parse row result")
	}

	log.DebugJSON(rowResult, "row result")

	columnIndex, err := analysisResult.InitializeColumnResult(handle.ColumnValue.Value())
	if err != nil {
		return wrap.Error(err, "failed to parse column result")
	}

	ok := rowResult.AggregatedValues.Insert(columnIndex, handle.ValueAggregation.Value())
	if !ok {
		return fmt.Errorf(
			"failed to insert aggregated value '%v' as %v into query result",
			handle.ValueAggregation.Value(),
			analysisResult.ValueAggregationDataType,
		)
	}

	return nil
}

func (analysisResult *AnalysisResult) GetOrCreateRowResult(
	rowValue any,
) (rowResult RowResult, err error) {
	// Iterates in reverse, as the row result we want is likely the previous element
	for i := len(analysisResult.Rows) - 1; i >= 0; i-- {
		rowResult = analysisResult.Rows[i]
		if rowResult.FieldValue.Equals(rowValue) {
			return rowResult, nil
		}
	}

	baseColumnValue, err := NewTypedValue(analysisResult.RowsMeta.BaseColumnDataType)
	if err != nil {
		return RowResult{}, wrap.Error(err, "failed to initialize row field value")
	}
	if ok := baseColumnValue.Set(rowValue); !ok {
		return RowResult{}, fmt.Errorf(
			"failed to set row field value of type %v to '%v'",
			analysisResult.RowsMeta.BaseColumnDataType,
			rowValue,
		)
	}

	aggregatedValues, err := NewTypedValueList(
		analysisResult.ValueAggregationDataType,
		analysisResult.ColumnsMeta.Limit,
	)
	if err != nil {
		return RowResult{}, wrap.Error(err, "failed to initialize query result values list")
	}

	rowResult = RowResult{FieldValue: baseColumnValue, AggregatedValues: aggregatedValues}
	analysisResult.Rows = append(analysisResult.Rows, rowResult)
	return rowResult, nil
}

func (analysisResult *AnalysisResult) InitializeColumnResult(
	columnValue any,
) (columnIndex int, err error) {
	// If the column is already added, we return its index.
	for i, column := range analysisResult.Columns {
		if column.FieldValue.Equals(columnValue) {
			return i, nil
		}
	}

	// If the column is not added previously, we parse the column value.
	fieldValue, err := NewTypedValue(analysisResult.ColumnsMeta.BaseColumnDataType)
	if err != nil {
		return 0, wrap.Error(err, "failed to initialize column field value")
	}
	if ok := fieldValue.Set(columnValue); !ok {
		return 0, fmt.Errorf(
			"failed to set column field value of type %v to '%v'",
			analysisResult.ColumnsMeta.BaseColumnDataType,
			columnValue,
		)
	}

	// Now we have to insert the column value at the correct index in the column list.
	// If the column list is empty, the new index is 0.
	// Otherwise, we go through the list to see where the new column value should be.
	newColumnIndex := 0
	if len(analysisResult.Columns) > 0 {
		ascending := analysisResult.ColumnsMeta.SortOrder == SortOrderAscending
		if ascending {
			// If columns are sorted in ascending order, we want to insert the new value at the end
			// if it's greater than all other values.
			newColumnIndex = len(analysisResult.Columns)
		}

		for i, column := range analysisResult.Columns {
			less, err := fieldValue.LessThan(column.FieldValue.Value())
			if err != nil {
				return 0, wrap.Error(err, "failed to compare column values")
			}

			log.Debugf("%v < %v: %v", columnValue, column.FieldValue.Value(), less)

			if (less && ascending) || (!less && !ascending) {
				newColumnIndex = i
				break
			}
		}
	}
	analysisResult.Columns = slices.Insert(
		analysisResult.Columns,
		newColumnIndex,
		ColumnResult{FieldValue: fieldValue},
	)

	// Go through all rows before the one currently being processed, to insert 0 at the new column
	// index.
	for i := 0; i < len(analysisResult.Rows)-1; i++ {
		analysisResult.Rows[i].AggregatedValues.InsertZero(newColumnIndex)
	}

	return newColumnIndex, nil
}

func (analysisResult *AnalysisResult) FillEmptyValueAggregations() {
	for _, row := range analysisResult.Rows {
		row.AggregatedValues.AddZeroesUpToLength(len(analysisResult.Columns))
	}
}
