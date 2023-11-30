package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type TypedValue interface {
	Value() any
	Pointer() any
	DataType() DataType
	Set(value any) (ok bool)
	Equals(value any) bool
	LessThan(value any) (less bool, err error)
}

type typedValue[T comparable] struct {
	dataType DataType
	value    T
}

func NewTypedValue(dataType DataType) (TypedValue, error) {
	switch dataType {
	case DataTypeText:
		return &typedValue[string]{dataType: dataType}, nil
	case DataTypeInt:
		return &typedValue[int64]{dataType: dataType}, nil
	case DataTypeFloat:
		return &typedValue[float64]{dataType: dataType}, nil
	case DataTypeTimestamp:
		return &typedValue[time.Time]{dataType: dataType}, nil
	case DataTypeUUID:
		return &typedValue[string]{dataType: dataType}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (typedValue *typedValue[T]) Value() any {
	return typedValue.value
}

func (typedValue *typedValue[T]) Pointer() any {
	return &typedValue.value
}

func (typedValue *typedValue[T]) DataType() DataType {
	return typedValue.dataType
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

func (typedValue *typedValue[T]) LessThan(value any) (less bool, err error) {
	switch typedValue.dataType {
	case DataTypeText, DataTypeUUID:
		if first, second, err := convertValues[string](typedValue, value); err == nil {
			return first < second, nil
		} else {
			return false, err
		}
	case DataTypeInt:
		if first, second, err := convertValues[int](typedValue, value); err == nil {
			return first < second, nil
		} else {
			return false, err
		}
	case DataTypeFloat:
		if first, second, err := convertValues[float64](typedValue, value); err == nil {
			return first < second, nil
		} else {
			return false, err
		}
	case DataTypeTimestamp:
		if first, second, err := convertValues[time.Time](typedValue, value); err == nil {
			return first.Before(second), nil
		} else {
			return false, err
		}
	default:
		return false, fmt.Errorf("unrecognized data type '%v'", typedValue.dataType)
	}
}

func convertValues[T any](value1 TypedValue, value2 any) (converted1 T, converted2 T, err error) {
	converted1, ok := value1.Value().(T)
	if !ok {
		return converted1, converted2, fmt.Errorf(
			"failed to convert value '%v' to data type %v",
			value1,
			value1.DataType(),
		)
	}
	converted2, ok = value2.(T)
	if !ok {
		return converted1, converted2, fmt.Errorf(
			"failed to convert value '%v' to data type %v",
			value2,
			value1.DataType(),
		)
	}
	return converted1, converted2, nil
}

func (typedValue typedValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(typedValue.value)
}

func (typedValue *typedValue[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &typedValue.value)
}
