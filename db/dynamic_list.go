package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type DynamicList interface {
	Append(item any) (ok bool)
}

type dynamicList[T any] struct {
	items []T
}

func NewDynamicList(dataType DataType, capacity int) (DynamicList, error) {
	switch dataType {
	case DataTypeText:
		return &dynamicList[string]{}, nil
	case DataTypeInt:
		return &dynamicList[int64]{}, nil
	case DataTypeFloat:
		return &dynamicList[float64]{}, nil
	case DataTypeTimestamp:
		return &dynamicList[time.Time]{}, nil
	case DataTypeUUID:
		return &dynamicList[string]{}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
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
