package column

import (
	"errors"
	"fmt"
)

type Column struct {
	Name     string
	DataType DataType
	Optional bool
}

type DataType string

const (
	DataTypeString    DataType = "String"
	DataTypeInt       DataType = "Integer"
	DataTypeFloat     DataType = "Float"
	DataTypeTimestamp DataType = "Timestamp"
	DataTypeUUID      DataType = "UUID"
)

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

func ValidateColumns(columns []Column) []error {
	var errs []error

	for i, column := range columns {
		if err := column.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("column %d ('%s'): %w", i, column.Name, err))
		}
	}

	return errs
}
