package datatypes

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
