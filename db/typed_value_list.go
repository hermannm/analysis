package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type TypedValueList interface {
	Insert(index int, item any) (ok bool)
	Truncate(maxLength int)
}

type typedValueList[T any] struct {
	values []T
}

func NewTypedValueList(dataType DataType, length int) (TypedValueList, error) {
	switch dataType {
	case DataTypeText:
		return &typedValueList[string]{make([]string, length)}, nil
	case DataTypeInt:
		return &typedValueList[int64]{make([]int64, length)}, nil
	case DataTypeFloat:
		return &typedValueList[float64]{make([]float64, length)}, nil
	case DataTypeTimestamp:
		return &typedValueList[time.Time]{make([]time.Time, length)}, nil
	case DataTypeUUID:
		return &typedValueList[string]{make([]string, length)}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (list *typedValueList[T]) Insert(index int, item any) (ok bool) {
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

func (list *typedValueList[T]) Truncate(maxLength int) {
	list.values = list.values[:maxLength]
}

func (list typedValueList[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(list.values)
}

func (list *typedValueList[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &list.values)
}
