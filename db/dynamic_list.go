package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type DynamicList interface {
	Append(item any) (ok bool)
}

func NewDynamicList(dataType DataType, capacity int) (DynamicList, error) {
	switch dataType {
	case DataTypeText:
		return newDynamicList[string](capacity), nil
	case DataTypeInt:
		return newDynamicList[int64](capacity), nil
	case DataTypeFloat:
		return newDynamicList[float64](capacity), nil
	case DataTypeTimestamp:
		return newDynamicList[time.Time](capacity), nil
	case DataTypeUUID:
		return newDynamicList[string](capacity), nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

type dynamicList[T any] struct {
	items []T
}

func newDynamicList[T any](capacity int) *dynamicList[T] {
	return &dynamicList[T]{make([]T, 0, capacity)}
}

func (list *dynamicList[T]) Append(item any) (ok bool) {
	if item, ok := item.(T); ok {
		list.items = append(list.items, item)
		return true
	} else {
		return false
	}
}

func (list dynamicList[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(list.items)
}

func (list *dynamicList[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &list.items)
}
