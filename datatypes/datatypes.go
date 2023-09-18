package datatypes

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"hermannm.dev/wrap"
)

type DataType string

const (
	DataTypeString    DataType = "String"
	DataTypeInt       DataType = "Integer"
	DataTypeFloat     DataType = "Float"
	DataTypeTimestamp DataType = "Timestamp"
	DataTypeUUID      DataType = "UUID"
)

type Column struct {
	Name     string
	DataType DataType
	Optional bool
}

type Schema struct {
	Columns []Column
}

func NewSchema(columnNames []string) Schema {
	columns := make([]Column, 0, len(columnNames))
	for _, columnName := range columnNames {
		columns = append(columns, Column{
			Name: columnName, DataType: "", Optional: false,
		})
	}

	return Schema{Columns: columns}
}

func (schema Schema) DeduceColumnTypesFromRow(row []string) error {
	for i, field := range row {
		if i >= len(schema.Columns) {
			return errors.New("row contains more fields than there are columns")
		}

		column := schema.Columns[i]

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

		schema.Columns[i] = column
	}

	return nil
}

func deduceColumnTypeFromField(field string) (deducedType DataType, isBlank bool) {
	if field == "" {
		return "", true
	}
	if _, err := strconv.ParseInt(field, 10, 64); err == nil {
		return DataTypeInt, false
	}
	if _, err := strconv.ParseFloat(field, 64); err == nil {
		return DataTypeFloat, false
	}
	if _, err := time.Parse(time.RFC3339, field); err == nil {
		return DataTypeTimestamp, false
	}
	if _, err := uuid.Parse(field); err == nil {
		return DataTypeUUID, false
	}
	return DataTypeString, false
}

func (schema Schema) ConvertRow(row []string) (convertedRow []any, err error) {
	if len(row) != len(schema.Columns) {
		return nil, errors.New("given row has more fields than there are columns in the schema")
	}

	convertedRow = make([]any, 0, len(row))

	for i, field := range row {
		column := schema.Columns[i]

		convertedField, err := convertField(field, column)
		if err != nil {
			return nil, wrap.Errorf(
				err, "failed to convert field '%s' to %s for column '%s'",
				field, column.DataType, column.Name,
			)
		}

		convertedRow = append(convertedRow, convertedField)
	}

	return convertedRow, nil
}

func convertField(field string, column Column) (convertedField any, err error) {
	if field == "" {
		if column.Optional {
			return nil, nil
		} else {
			return nil, errors.New("tried to insert empty value into non-optional column")
		}
	}

	switch column.DataType {
	case DataTypeInt:
		return strconv.ParseInt(field, 10, 64)
	case DataTypeFloat:
		return strconv.ParseFloat(field, 64)
	case DataTypeTimestamp:
		value, err := time.Parse(time.RFC3339, field)
		if err == nil {
			return value.UnixMilli(), nil
		} else {
			return nil, err
		}
	case DataTypeUUID:
		if _, err := uuid.Parse(field); err != nil {
			return nil, wrap.Errorf(
				err, "failed to parse value '%s' as UUID for column '%s'",
				field, column.Name,
			)
		}
	case DataTypeString:
		return field, nil
	}

	return nil, fmt.Errorf("unrecognized data type '%s' in column", column.DataType)
}

func (schema Schema) Validate() []error {
	var errs []error

	for i, column := range schema.Columns {
		if err := column.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("column %d ('%s'): %w", i, column.Name, err))
		}
	}

	return errs
}

func (column Column) Validate() error {
	if column.Name == "" {
		return errors.New("column name is blank")
	}

	switch column.DataType {
	case DataTypeString, DataTypeInt, DataTypeFloat, DataTypeTimestamp, DataTypeUUID:
		break
	case "":
		return errors.New("column data type is blank")
	default:
		return fmt.Errorf("unrecognized column data type '%s'", column.DataType)
	}

	return nil
}