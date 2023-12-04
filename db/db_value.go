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

type dbValue[T comparable] struct {
	value   T
	compare comparator
}

type comparator func(otherValue any) (less bool, err error)

func NewDBValue(dataType DataType) (DBValue, error) {
	switch dataType {
	case DataTypeText, DataTypeUUID:
		dbValue := dbValue[string]{}
		dbValue.compare = createComparator(&dbValue.value)
		return &dbValue, nil
	case DataTypeInt:
		dbValue := dbValue[int64]{}
		dbValue.compare = createComparator(&dbValue.value)
		return &dbValue, nil
	case DataTypeFloat:
		dbValue := dbValue[float64]{}
		dbValue.compare = createComparator(&dbValue.value)
		return &dbValue, nil
	case DataTypeTimestamp:
		dbValue := dbValue[time.Time]{}
		dbValue.compare = createTimeComparator(&dbValue.value)
		return &dbValue, nil
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

func (dbValue *dbValue[T]) LessThan(value any) (less bool, err error) {
	return dbValue.compare(value)
}

func (dbValue dbValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbValue.value)
}

func (dbValue *dbValue[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &dbValue.value)
}

func createComparator[T interface{ string | float64 | int64 }](value *T) comparator {
	return func(otherValue any) (less bool, err error) {
		if otherValue, ok := otherValue.(T); ok {
			return *value < otherValue, nil
		}
		return false, fmt.Errorf("failed to convert '%v' to string", otherValue)
	}
}

func createTimeComparator(value *time.Time) comparator {
	return func(otherValue any) (less bool, err error) {
		if otherValue, ok := otherValue.(time.Time); ok {
			return value.Before(otherValue), nil
		}
		return false, fmt.Errorf("failed to convert '%v' to time.Time", otherValue)
	}
}
