package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type DBValue interface {
	Value() any
	Pointer() any
	Set(value any) (ok bool)
	Equals(value any) bool
	LessThan(value any) (less bool, err error)
}

// Implements all DBValue methods except LessThan, as that requires type-specific implementation.
type dbValue[T comparable] struct {
	value T
}

// Implements DBValue.LessThan for types that support the < operator.
type orderedDBValue[T string | int64 | float64] struct {
	dbValue[T]
}

// Implements DBValue.LessThan for time.Time, as we have to use Time.Before for it.
type timeDBValue struct {
	dbValue[time.Time]
}

func NewDBValue(dataType DataType) (DBValue, error) {
	switch dataType {
	case DataTypeText, DataTypeUUID:
		return &orderedDBValue[string]{}, nil
	case DataTypeInt:
		return &orderedDBValue[int64]{}, nil
	case DataTypeFloat:
		return &orderedDBValue[float64]{}, nil
	case DataTypeDateTime:
		return &timeDBValue{}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
}

func (dbValue *dbValue[T]) Value() any {
	return dbValue.value
}

func (dbValue *dbValue[T]) Pointer() any {
	return &dbValue.value
}

func (dbValue *dbValue[T]) Set(value any) (ok bool) {
	if value, ok := value.(T); ok {
		dbValue.value = value
		return true
	} else {
		return false
	}
}

func (dbValue *dbValue[T]) Equals(value any) bool {
	if value, ok := value.(T); ok {
		return dbValue.value == value
	} else {
		return false
	}
}

func (dbValue *orderedDBValue[T]) LessThan(value any) (less bool, err error) {
	if value, ok := value.(T); ok {
		return dbValue.value < value, nil
	}
	return false, fmt.Errorf("failed to convert '%v' to expected data type", value)
}

func (dbValue *timeDBValue) LessThan(value any) (less bool, err error) {
	if value, ok := value.(time.Time); ok {
		return dbValue.value.Before(value), nil
	}
	return false, fmt.Errorf("failed to convert '%v' to time.Time", value)
}

func (dbValue dbValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbValue.value)
}

func (dbValue *dbValue[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &dbValue.value)
}
