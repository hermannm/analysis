package db

import (
	"fmt"
	"time"
)

type DynamicValue interface {
	Set(value any) (ok bool)
	Equals(value any) bool
	Value() any
	Pointer() any
}

func NewDynamicValue(dataType DataType) (DynamicValue, error) {
	switch dataType {
	case DataTypeText:
		return newDynamicValue[string](), nil
	case DataTypeInt:
		return newDynamicValue[int64](), nil
	case DataTypeFloat:
		return newDynamicValue[float64](), nil
	case DataTypeTimestamp:
		return newDynamicValue[time.Time](), nil
	case DataTypeUUID:
		return newDynamicValue[string](), nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

type dynamicValue[T comparable] struct {
	value T
}

func newDynamicValue[T comparable]() *dynamicValue[T] {
	var value T
	return &dynamicValue[T]{value: value}
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
