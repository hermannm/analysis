package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type DBValueList interface {
	Insert(index int, item any) (ok bool)
	Truncate(maxLength int)
}

type dbValueList[T any] struct {
	values []T
}

func NewDBValueList(dataType DataType, length int) (DBValueList, error) {
	switch dataType {
	case DataTypeText:
		return &dbValueList[string]{make([]string, length)}, nil
	case DataTypeInt:
		return &dbValueList[int64]{make([]int64, length)}, nil
	case DataTypeFloat:
		return &dbValueList[float64]{make([]float64, length)}, nil
	case DataTypeTimestamp:
		return &dbValueList[time.Time]{make([]time.Time, length)}, nil
	case DataTypeUUID:
		return &dbValueList[string]{make([]string, length)}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (list *dbValueList[T]) Insert(index int, item any) (ok bool) {
	if index < 0 || index >= len(list.values) {
		return true
	}

	if item, ok := item.(T); ok {
		list.values[index] = item
		return true
	} else {
		return false
	}
}

func (list *dbValueList[T]) Truncate(maxLength int) {
	list.values = list.values[:maxLength]
}

func (list dbValueList[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(list.values)
}

func (list *dbValueList[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &list.values)
}
