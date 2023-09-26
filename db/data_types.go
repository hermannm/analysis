package db

import (
	"encoding/json"
	"errors"
)

type DataType uint8

const (
	invalidDataType DataType = 0

	DataTypeString    DataType = 1
	DataTypeInt       DataType = 2
	DataTypeFloat     DataType = 3
	DataTypeTimestamp DataType = 4
	DataTypeUUID      DataType = 5
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
