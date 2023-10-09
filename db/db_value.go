package db

import (
	"encoding/json"
	"fmt"
	"time"
)

type DBValue interface {
	Set(value any) (ok bool)
	Equals(value any) bool
	Value() any
	Pointer() any
}

type dbValue[T comparable] struct {
	value T
}

func NewDBValue(dataType DataType) (DBValue, error) {
	switch dataType {
	case DataTypeText:
		return &dbValue[string]{}, nil
	case DataTypeInt:
		return &dbValue[int64]{}, nil
	case DataTypeFloat:
		return &dbValue[float64]{}, nil
	case DataTypeTimestamp:
		return &dbValue[time.Time]{}, nil
	case DataTypeUUID:
		return &dbValue[string]{}, nil
	default:
		return nil, fmt.Errorf("unrecognized data type %v", dataType)
	}
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

func (dbValue *dbValue[T]) Value() any {
	return dbValue.value
}

func (dbValue *dbValue[T]) Pointer() any {
	return &dbValue.value
}

func (dbValue dbValue[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(dbValue.value)
}

func (dbValue *dbValue[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &dbValue.value)
}
