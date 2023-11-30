package db

import (
	"fmt"

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
	switch dataType {
	case DataTypeInt, DataTypeFloat:
		return nil
	default:
		return fmt.Errorf(
			"aggregation can only be done on %v/%v columns, not %v",
			DataTypeInt,
			DataTypeFloat,
			dataType,
		)
	}
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
