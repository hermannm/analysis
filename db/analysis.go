package db

import (
	"fmt"
	"slices"

	"hermannm.dev/devlog/log"
	"hermannm.dev/wrap"
)

type AnalysisQuery struct {
	RowSplit    Split       `json:"rowSplit"`
	ColumnSplit Split       `json:"columnSplit"`
	Aggregation Aggregation `json:"aggregation"`
}

type Aggregation struct {
	Kind      AggregationKind `json:"kind"`
	FieldName string          `json:"fieldName"`
	DataType  DataType        `json:"dataType"`
}

type Split struct {
	FieldName string    `json:"fieldName"`
	DataType  DataType  `json:"dataType"`
	Limit     int       `json:"limit"`
	SortOrder SortOrder `json:"sortOrder"`
	// May only be present if DataType is INTEGER.
	IntegerInterval int `json:"integerInterval,omitempty"`
	// May only be present if DataType is FLOAT.
	FloatInterval float64 `json:"floatInterval,omitempty"`
	// May only be present if DataType is TIMESTAMP.
	DateInterval *DateInterval `json:"dateInterval,omitempty"`
}

type AnalysisResult struct {
	Rows     []RowResult `json:"rows"`
	RowsMeta Split       `json:"rowsMeta"`

	Columns     []ColumnResult `json:"columns"`
	ColumnsMeta Split          `json:"columnsMeta"`

	AggregationDataType DataType `json:"aggregationDataType"`
}

type RowResult struct {
	FieldValue           DBValue          `json:"fieldValue"`
	AggregationTotal     DBValue          `json:"aggregationTotal"`
	AggregationsByColumn AggregatedValues `json:"aggregationsByColumn"`
}

type ColumnResult struct {
	FieldValue DBValue `json:"fieldValue"`
}

type ResultHandle struct {
	Column      DBValue
	Row         DBValue
	Aggregation DBValue
}

func NewAnalysisQueryResult(analysis AnalysisQuery) AnalysisResult {
	return AnalysisResult{
		Rows:                make([]RowResult, 0, analysis.RowSplit.Limit),
		RowsMeta:            analysis.RowSplit,
		Columns:             make([]ColumnResult, 0, analysis.ColumnSplit.Limit),
		ColumnsMeta:         analysis.ColumnSplit,
		AggregationDataType: analysis.Aggregation.DataType,
	}
}

func (analysisResult *AnalysisResult) NewResultHandle() (handle ResultHandle, err error) {
	handle.Column, err = NewDBValue(analysisResult.ColumnsMeta.DataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize column value")
	}

	handle.Row, err = NewDBValue(analysisResult.RowsMeta.DataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize row value")
	}

	handle.Aggregation, err = NewDBValue(analysisResult.AggregationDataType)
	if err != nil {
		return ResultHandle{}, wrap.Error(err, "failed to initialize aggregation")
	}

	return handle, nil
}

func (analysisResult *AnalysisResult) ParseResultHandle(handle ResultHandle) error {
	rowResult, err := analysisResult.GetOrCreateRowResult(handle)
	if err != nil {
		return wrap.Error(err, "failed to parse row result")
	}

	columnIndex, err := analysisResult.InitializeColumnResult(handle)
	if err != nil {
		return wrap.Error(err, "failed to parse column result")
	}

	ok := rowResult.AggregationsByColumn.Insert(columnIndex, handle.Aggregation.Value())
	if !ok {
		return fmt.Errorf(
			"failed to insert aggregated value '%v' as %v into query result",
			handle.Aggregation.Value(),
			analysisResult.AggregationDataType,
		)
	}

	return nil
}

func (analysisResult *AnalysisResult) GetOrCreateRowResult(
	handle ResultHandle,
) (rowResult RowResult, err error) {
	// Iterates in reverse, as the row result we want is likely the previous element.
	for i := len(analysisResult.Rows) - 1; i >= 0; i-- {
		rowResult = analysisResult.Rows[i]
		if rowResult.FieldValue.Equals(handle.Row.Value()) {
			return rowResult, nil
		}
	}

	rowValue, err := NewDBValue(analysisResult.RowsMeta.DataType)
	if err != nil {
		return RowResult{}, wrap.Error(err, "failed to initialize row field value")
	}
	if ok := rowValue.Set(handle.Row.Value()); !ok {
		return RowResult{}, fmt.Errorf(
			"failed to set row field value of type %v to '%v'",
			analysisResult.RowsMeta.DataType,
			handle.Row.Value(),
		)
	}

	aggregationsByColumn, err := NewAggregatedValues(
		analysisResult.AggregationDataType,
		analysisResult.ColumnsMeta.Limit,
	)
	if err != nil {
		return RowResult{}, wrap.Error(err, "failed to initialize aggregations in query result")
	}

	rowResult = RowResult{
		FieldValue:           rowValue,
		AggregationsByColumn: aggregationsByColumn,
	}
	analysisResult.Rows = append(analysisResult.Rows, rowResult)
	return rowResult, nil
}

func (analysisResult *AnalysisResult) InitializeColumnResult(
	handle ResultHandle,
) (columnIndex int, err error) {
	// If the column is already added, we return its index.
	for i, column := range analysisResult.Columns {
		if column.FieldValue.Equals(handle.Column.Value()) {
			return i, nil
		}
	}

	// If the column is not added previously, we parse the column value.
	columnValue, err := NewDBValue(analysisResult.ColumnsMeta.DataType)
	if err != nil {
		return 0, wrap.Error(err, "failed to initialize column field value")
	}
	if ok := columnValue.Set(handle.Column.Value()); !ok {
		return 0, fmt.Errorf(
			"failed to set column field value of type %v to '%v'",
			analysisResult.ColumnsMeta.DataType,
			handle.Column.Value(),
		)
	}

	// Now we have to insert the column value at the correct index in the column list.
	// If the column list is empty, the new index is 0.
	// Otherwise, we go through the list to see where the new column value should be.
	newColumnIndex := len(analysisResult.Columns)
	if len(analysisResult.Columns) > 0 {
		ascending := analysisResult.ColumnsMeta.SortOrder == SortOrderAscending

		for i, column := range analysisResult.Columns {
			less, err := columnValue.LessThan(column.FieldValue.Value())
			if err != nil {
				return 0, wrap.Error(err, "failed to compare column values")
			}

			if less {
				log.Debugf("'%v' less than '%v'", columnValue.Value(), column.FieldValue.Value())
			} else {
				log.Debugf("'%v' greater than '%v'", columnValue.Value(), column.FieldValue.Value())
			}

			if (ascending && less) || (!ascending && !less) {
				newColumnIndex = i
				break
			}
		}
	}

	analysisResult.Columns = slices.Insert(
		analysisResult.Columns,
		newColumnIndex,
		ColumnResult{FieldValue: columnValue},
	)

	// Go through all rows before the one currently being processed, to insert 0 at the new column
	// index.
	for i := 0; i < len(analysisResult.Rows)-1; i++ {
		analysisResult.Rows[i].AggregationsByColumn.InsertZero(newColumnIndex)
	}

	return newColumnIndex, nil
}

func (analysisResult *AnalysisResult) Finalize() error {
	if err := analysisResult.calculateAggregationTotals(); err != nil {
		return wrap.Error(err, "failed to calculate aggregation totals")
	}

	if err := analysisResult.sortRowsByAggregationTotals(); err != nil {
		return wrap.Error(err, "failed to sort rows by aggregation totals")
	}

	analysisResult.fillEmptyAggregations()
	return nil
}

func (analysisResult *AnalysisResult) calculateAggregationTotals() error {
	for i, row := range analysisResult.Rows {
		total, err := row.AggregationsByColumn.Total(analysisResult.AggregationDataType)
		if err != nil {
			return err
		}
		analysisResult.Rows[i].AggregationTotal = total
	}
	return nil
}

func (analysisResult *AnalysisResult) sortRowsByAggregationTotals() error {
	var sortErr error

	slices.SortFunc(analysisResult.Rows, func(row1 RowResult, row2 RowResult) int {
		row2Total := row2.AggregationTotal.Value()
		if row1.AggregationTotal.Equals(row2Total) {
			return 0
		}

		less, err := row1.AggregationTotal.LessThan(row2Total)
		if err != nil {
			sortErr = err
		}

		var result int
		if less {
			result = -1
		} else {
			result = 1
		}

		switch analysisResult.RowsMeta.SortOrder {
		case SortOrderAscending:
			return result
		case SortOrderDescending:
			return -result
		default:
			return 0
		}
	})

	return sortErr
}

func (analysisResult *AnalysisResult) fillEmptyAggregations() {
	for _, row := range analysisResult.Rows {
		row.AggregationsByColumn.AddZeroesUpToLength(len(analysisResult.Columns))
	}
}
