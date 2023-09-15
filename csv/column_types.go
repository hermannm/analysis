package csv

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"hermannm.dev/analysis/column"
	"hermannm.dev/wrap"
)

func (reader *Reader) DeduceColumnTypes(maxRowsToCheck int) (columns []column.Column, err error) {
	// Resets reader position in file before returning, so its data can be read subsequently
	defer func() {
		if resetErr := reader.setPositionToAfterHeaderRow(); resetErr != nil {
			err = wrap.Error(resetErr, "failed to reset CSV file after parsing its column types")
		}
	}()

	columns, err = reader.parseColumnNames()
	if err != nil {
		return nil, wrap.Error(err, "failed to parse CSV column names")
	}

	for {
		row, finished, err := reader.ReadRow()
		if finished {
			break
		}
		if err != nil {
			return nil, wrap.Errorf(err, "failed to read row %d of CSV file", reader.CurrentRow())
		}

		if err := deduceColumnTypesFromRow(columns, row); err != nil {
			return nil, wrap.Errorf(
				err, "failed to parse CSV field types from row %d", reader.CurrentRow(),
			)
		}
	}

	if errs := column.ValidateColumns(columns); len(errs) > 0 {
		return nil, wrap.Errors("failed to deduce data types for all given CSV columns", errs...)
	}

	return columns, nil
}

func (reader Reader) parseColumnNames() ([]column.Column, error) {
	headers, finished, err := reader.ReadRow()
	if reader.CurrentRow() != 1 {
		return nil, errors.New("tried to read column names after first row")
	}
	if finished {
		return nil, errors.New("csv file ended before getting to parse column names")
	}
	if err != nil {
		return nil, wrap.Error(err, "failed to read CSV header row")
	}

	columns := make([]column.Column, 0, len(headers))
	for _, header := range headers {
		columns = append(columns, column.Column{
			Name: header, DataType: "", Optional: false,
		})
	}

	return columns, nil
}

func deduceColumnTypesFromRow(columns []column.Column, row []string) error {
	for i, field := range row {
		if i >= len(columns) {
			return errors.New("row contains more fields than there are columns")
		}

		column := columns[i]

		deducedType, isBlank := deduceColumnTypeFromField(field)
		if isBlank {
			column.Optional = true
		} else if column.DataType == "" {
			column.DataType = deducedType
		} else if column.DataType != deducedType {
			return fmt.Errorf(
				"found incompatible data types '%s' and '%s' in column '%s'",
				column.DataType, deducedType, column.Name,
			)
		}

		columns[i] = column
	}

	return nil
}

func deduceColumnTypeFromField(field string) (deducedType column.DataType, isBlank bool) {
	if field == "" {
		return "", true
	}
	if _, err := strconv.ParseInt(field, 10, 64); err == nil {
		return column.DataTypeInt, false
	}
	if _, err := strconv.ParseFloat(field, 64); err == nil {
		return column.DataTypeFloat, false
	}
	if _, err := time.Parse(time.RFC3339, field); err == nil {
		return column.DataTypeTimestamp, false
	}
	if _, err := uuid.Parse(field); err == nil {
		return column.DataTypeUUID, false
	}
	return column.DataTypeString, false
}
