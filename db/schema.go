package db

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"hermannm.dev/wrap"
)

type TableSchema struct {
	Columns []Column `json:"columns"`
}

type Column struct {
	Name     string   `json:"name"`
	DataType DataType `json:"dataType"`
	Optional bool     `json:"optional"`
}

func NewTableSchema(columnNames []string) TableSchema {
	columns := make([]Column, 0, len(columnNames))
	for _, columnName := range columnNames {
		columns = append(columns, Column{Name: columnName})
	}

	return TableSchema{Columns: columns}
}

func (schema TableSchema) DeduceDataTypesFromRow(row []string) error {
	for i, field := range row {
		if i >= len(schema.Columns) {
			return errors.New("row contains more fields than there are columns")
		}

		column := schema.Columns[i]

		deducedType, isBlank := deduceDataTypeFromField(field)
		if isBlank {
			column.Optional = true
		} else if !column.DataType.IsValid() {
			column.DataType = deducedType
		} else if column.DataType != deducedType {
			return fmt.Errorf(
				"found incompatible data types '%s' and '%s' in column '%s'",
				column.DataType.String(),
				deducedType.String(),
				column.Name,
			)
		}

		schema.Columns[i] = column
	}

	return nil
}

func deduceDataTypeFromField(field string) (deducedType DataType, isBlank bool) {
	if field == "" {
		return 0, true
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
	return DataTypeText, false
}

func (schema TableSchema) ConvertRowToMap(rawRow []string) (map[string]any, error) {
	if len(rawRow) != len(schema.Columns) {
		return nil, errors.New(
			"given row has more fields than there are columns in the table schema",
		)
	}

	rowMap := make(map[string]any, len(schema.Columns))

	for i, field := range rawRow {
		column := schema.Columns[i]

		convertedField, err := convertField(field, column)
		if err != nil {
			return nil, wrap.Errorf(
				err,
				"failed to convert field '%s' to %s for column '%s'",
				field,
				column.DataType,
				column.Name,
			)
		}

		rowMap[column.Name] = convertedField
	}

	return rowMap, nil
}

func (schema TableSchema) ConvertAndAppendRow(convertedRow []any, rawRow []string) ([]any, error) {
	if len(rawRow) != len(schema.Columns) {
		return nil, errors.New(
			"given row has more fields than there are columns in the table schema",
		)
	}

	for i, field := range rawRow {
		column := schema.Columns[i]

		convertedField, err := convertField(field, column)
		if err != nil {
			return nil, wrap.Errorf(
				err,
				"failed to convert field '%s' to %s for column '%s'",
				field,
				column.DataType,
				column.Name,
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
				err,
				"failed to parse value '%s' as UUID for column '%s'",
				field,
				column.Name,
			)
		}
		return field, nil
	case DataTypeText:
		return field, nil
	}

	return nil, fmt.Errorf("unrecognized data type '%s' in column", column.DataType)
}

func (schema TableSchema) Validate() []error {
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

	if !column.DataType.IsValid() {
		return errors.New("invalid column data type")
	}

	return nil
}

const (
	StoredSchemasTable          = "analysis_schemas"
	StoredSchemaName            = "table_name"
	StoredSchemaColumnNames     = "column_names"
	StoredSchemaColumnDataTypes = "column_data_types"
	StoredSchemaColumnOptionals = "column_optionals"
)

type StoredTableSchema struct {
	ColumnNames []string `json:"column_names"`
	DataTypes   []uint8  `json:"column_data_types"`
	Optionals   []bool   `json:"column_optionals"`
}

func (storedSchema StoredTableSchema) ToSchema() (TableSchema, error) {
	columnCount := len(storedSchema.ColumnNames)
	if len(storedSchema.DataTypes) != columnCount || len(storedSchema.Optionals) != columnCount {
		return TableSchema{}, errors.New("stored table schema had inconsistent column counts")
	}

	schema := TableSchema{Columns: make([]Column, columnCount)}
	for i := 0; i < columnCount; i++ {
		schema.Columns[i] = Column{
			Name:     storedSchema.ColumnNames[i],
			DataType: DataType(storedSchema.DataTypes[i]),
			Optional: storedSchema.Optionals[i],
		}
	}
	if errs := schema.Validate(); len(errs) != 0 {
		return TableSchema{}, wrap.Errors("stored table schema was invalid", errs...)
	}

	return schema, nil
}

func (schema TableSchema) ToStored() StoredTableSchema {
	columnCount := len(schema.Columns)

	storedSchema := StoredTableSchema{
		ColumnNames: make([]string, columnCount),
		DataTypes:   make([]uint8, columnCount),
		Optionals:   make([]bool, columnCount),
	}

	for i, column := range schema.Columns {
		storedSchema.ColumnNames[i] = column.Name
		storedSchema.DataTypes[i] = uint8(column.DataType)
		storedSchema.Optionals[i] = column.Optional
	}

	return storedSchema
}
