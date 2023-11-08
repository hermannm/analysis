package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type TypedValue interface {
	Set(value any) (ok bool)
	Equals(value any) bool
	Value() any
	Pointer() any
}

type typedValue[T comparable] struct {
	value T
}

func NewTypedValue(dataType DataType) (TypedValue, error) {
	switch dataType {
	case DataTypeText:
		return &typedValue[string]{}, nil
	case DataTypeInt:
		return &typedValue[int64]{}, nil
	case DataTypeFloat:
		return &typedValue[float64]{}, nil
	case DataTypeTimestamp:
		return &typedValue[time.Time]{}, nil
	case DataTypeUUID:
		return &typedValue[string]{}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (typedValue *typedValue[T]) Set(value any) (ok bool) {
	if value, ok := value.(T); ok {
		typedValue.value = value
		return true
	} else {
		return false
	}
}

func (typedValue *typedValue[T]) Equals(value any) bool {
	if value, ok := value.(T); ok {
		return typedValue.value == value
	} else {
		return false
	}
}

func (typedValue *typedValue[T]) Value() any {
	return typedValue.value
}

func (typedValue *typedValue[T]) Pointer() any {
	return &typedValue.value
}

func (typedValue typedValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(typedValue.value)
}

func (typedValue *typedValue[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &typedValue.value)
}
