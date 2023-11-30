package db

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

type TypedValueList interface {
	Insert(index int, item any) (ok bool)
	InsertZero(index int)
	AddZeroesUpToLength(length int)
}

type typedValueList[T any] struct {
	values []T
}

func NewTypedValueList(dataType DataType, capacity int) (TypedValueList, error) {
	switch dataType {
	case DataTypeText:
		return &typedValueList[string]{make([]string, 0, capacity)}, nil
	case DataTypeInt:
		return &typedValueList[int64]{make([]int64, 0, capacity)}, nil
	case DataTypeFloat:
		return &typedValueList[float64]{make([]float64, 0, capacity)}, nil
	case DataTypeTimestamp:
		return &typedValueList[time.Time]{make([]time.Time, 0, capacity)}, nil
	case DataTypeUUID:
		return &typedValueList[string]{make([]string, 0, capacity)}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (list *typedValueList[T]) Insert(index int, value any) (ok bool) {
	if value, ok := value.(T); ok {
		if index > len(list.values) {
			list.AddZeroesUpToLength(index)
		}

		list.values = slices.Insert(list.values, index, value)
		return true
	} else {
		return false
	}
}

func (list *typedValueList[T]) InsertZero(index int) {
	var zero T
	list.values = slices.Insert(list.values, index, zero)
}

func (list *typedValueList[T]) AddZeroesUpToLength(length int) {
	zeroes := make([]T, length-len(list.values))
	list.values = append(list.values, zeroes...)
}

func (list typedValueList[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(list.values)
}

func (list *typedValueList[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &list.values)
}
