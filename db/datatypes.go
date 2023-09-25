package db

import (
	"encoding/json"
	"errors"
)

type DataType uint8

const (
	DataTypeString    DataType = 0
	DataTypeInt       DataType = 1
	DataTypeFloat     DataType = 2
	DataTypeTimestamp DataType = 3
	DataTypeUUID      DataType = 4

	invalidDataType DataType = 255
)

var dataTypeNames = map[DataType]string{
	DataTypeString:    "String",
	DataTypeInt:       "Integer",
	DataTypeFloat:     "Float",
	DataTypeTimestamp: "Timestamp",
	DataTypeUUID:      "UUID",
}

func (dataType DataType) IsValid() bool {
	_, ok := dataTypeNames[dataType]
	return ok
}

func (dataType DataType) String() string {
	if name, ok := dataTypeNames[dataType]; ok {
		return name
	} else {
		return "[INVALID]"
	}
}

func (dataType DataType) MarshalJSON() ([]byte, error) {
	if name, ok := dataTypeNames[dataType]; ok {
		return json.Marshal(name)
	} else {
		return nil, errors.New("unrecognized data type")
	}
}

func (dataType *DataType) UnmarshalJSON(bytes []byte) error {
	for candidate, name := range dataTypeNames {
		if name == string(bytes) {
			*dataType = candidate
			return nil
		}
	}

	return errors.New("unrecognized data type")
}
