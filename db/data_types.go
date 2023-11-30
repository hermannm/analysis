package db

import (
	"fmt"
	"time"

	"hermannm.dev/enumnames"
)

type DataType int8

const (
	DataTypeText DataType = iota + 1
	DataTypeInt
	DataTypeFloat
	DataTypeTimestamp
	DataTypeUUID
)

var dataTypeMap = enumnames.NewMap(map[DataType]string{
	DataTypeText:      "TEXT",
	DataTypeInt:       "INTEGER",
	DataTypeFloat:     "FLOAT",
	DataTypeTimestamp: "TIMESTAMP",
	DataTypeUUID:      "UUID",
})

func (dataType DataType) IsValid() bool {
	return dataTypeMap.ContainsEnumValue(dataType)
}

func (dataType DataType) IsValidForAggregation() error {
	if dataType != DataTypeInt && dataType != DataTypeFloat {
		return fmt.Errorf(
			"value aggregation can only be done on INTEGER or FLOAT columns, not %v",
			dataType,
		)
	}

	return nil
}

func (dataType DataType) String() string {
	return dataTypeMap.GetNameOrFallback(dataType, "INVALID_DATA_TYPE")
}

func (dataType DataType) MarshalJSON() ([]byte, error) {
	return dataTypeMap.MarshalToNameJSON(dataType)
}

func (dataType *DataType) UnmarshalJSON(bytes []byte) error {
	return dataTypeMap.UnmarshalFromNameJSON(bytes, dataType)
}

func compareValues(value1 any, value2 any, dataType DataType) (value1LessThan2 bool, err error) {
	switch dataType {
	case DataTypeText, DataTypeUUID:
		if value1, value2, err := convertValues[string](value1, value2, dataType); err == nil {
			return value1 < value2, nil
		} else {
			return false, err
		}
	case DataTypeInt:
		if value1, value2, err := convertValues[int](value1, value2, dataType); err == nil {
			return value1 < value2, nil
		} else {
			return false, err
		}
	case DataTypeFloat:
		if value1, value2, err := convertValues[float64](value1, value2, dataType); err == nil {
			return value1 < value2, nil
		} else {
			return false, err
		}
	case DataTypeTimestamp:
		if value1, value2, err := convertValues[time.Time](value1, value2, dataType); err == nil {
			return value1.Before(value2), nil
		} else {
			return false, err
		}
	default:
		return false, fmt.Errorf("unrecognized data type '%v'", dataType)
	}
}

func convertValues[T any](
	value1 any,
	value2 any,
	dataType DataType,
) (converted1 T, converted2 T, err error) {
	converted1, ok := value1.(T)
	if !ok {
		return converted1, converted2, fmt.Errorf(
			"failed to convert value '%v' to data type %v",
			value1,
			dataType,
		)
	}
	converted2, ok = value2.(T)
	if !ok {
		return converted1, converted2, fmt.Errorf(
			"failed to convert value '%v' to data type %v",
			value2,
			dataType,
		)
	}
	return converted1, converted2, nil
}
