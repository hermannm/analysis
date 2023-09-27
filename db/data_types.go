package db

import (
	"hermannm.dev/enumnames"
)

type DataType uint8

const (
	invalidDataType DataType = 0

	DataTypeText      DataType = 1
	DataTypeInt       DataType = 2
	DataTypeFloat     DataType = 3
	DataTypeTimestamp DataType = 4
	DataTypeUUID      DataType = 5
)

var dataTypeNames = enumnames.NewMap(map[DataType]string{
	DataTypeText:      "Text",
	DataTypeInt:       "Integer",
	DataTypeFloat:     "Float",
	DataTypeTimestamp: "Timestamp",
	DataTypeUUID:      "UUID",
})

func (dataType DataType) IsValid() bool {
	return dataTypeNames.ContainsEnumValue(dataType)
}

func (dataType DataType) String() string {
	return dataTypeNames.GetNameOrFallback(dataType, "[INVALID DATA TYPE]")
}

func (dataType DataType) MarshalJSON() ([]byte, error) {
	return dataTypeNames.MarshalToNameJSON(dataType)
}

func (dataType *DataType) UnmarshalJSON(bytes []byte) error {
	return dataTypeNames.UnmarshalFromNameJSON(bytes, dataType)
}
