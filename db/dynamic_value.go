package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type DynamicValue interface {
	Set(value any) (ok bool)
	Equals(value any) bool
	Value() any
	Pointer() any
}

type dynamicValue[T comparable] struct {
	value T
}

func NewDynamicValue(dataType DataType) (DynamicValue, error) {
	switch dataType {
	case DataTypeText:
		return &dynamicValue[string]{}, nil
	case DataTypeInt:
		return &dynamicValue[int64]{}, nil
	case DataTypeFloat:
		return &dynamicValue[float64]{}, nil
	case DataTypeTimestamp:
		return &dynamicValue[time.Time]{}, nil
	case DataTypeUUID:
		return &dynamicValue[string]{}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (dynValue *dynamicValue[T]) Set(value any) (ok bool) {
	if value, ok := value.(T); ok {
		dynValue.value = value
		return true
	} else {
		return false
	}
}

func (dynValue *dynamicValue[T]) Equals(value any) bool {
	if value, ok := value.(T); ok {
		return dynValue.value == value
	} else {
		return false
	}
}

func (dynValue *dynamicValue[T]) Value() any {
	return dynValue.value
}

func (dynValue *dynamicValue[T]) Pointer() any {
	return &dynValue.value
}

func (dynValue dynamicValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(dynValue.value)
}

func (dynValue *dynamicValue[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &dynValue.value)
}
