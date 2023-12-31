package db

import (
	"encoding/json"
	"fmt"
	"slices"

	"hermannm.dev/wrap"
)

type AggregatedValues interface {
	Insert(index int, item any) (ok bool)
	InsertZero(index int)
	AddZeroesUpToLength(length int)
	Total(dataType DataType) (DBValue, error)
	Truncate(maxLength int)
}

type aggregatedValues[T interface{ int64 | float64 }] struct {
	values []T
}

func NewAggregatedValues(dataType DataType, capacity int) (AggregatedValues, error) {
	switch dataType {
	case DataTypeInt:
		return &aggregatedValues[int64]{make([]int64, 0, capacity)}, nil
	case DataTypeFloat:
		return &aggregatedValues[float64]{make([]float64, 0, capacity)}, nil
	default:
		return nil, fmt.Errorf("invalid data type %v", dataType)
	}
}

func (list *aggregatedValues[T]) Insert(index int, value any) (ok bool) {
	if value, ok := value.(T); ok {
		if index >= len(list.values) {
			list.AddZeroesUpToLength(index + 1)
		}

		list.values[index] = value
		return true
	} else {
		return false
	}
}

func (list *aggregatedValues[T]) InsertZero(index int) {
	if index <= len(list.values) {
		var zero T
		list.values = slices.Insert(list.values, index, zero)
	} else {
		list.AddZeroesUpToLength(index + 1)
	}
}

func (list *aggregatedValues[T]) AddZeroesUpToLength(length int) {
	zeroes := make([]T, length-len(list.values))
	list.values = append(list.values, zeroes...)
}

func (list *aggregatedValues[T]) Total(dataType DataType) (DBValue, error) {
	var total T
	for _, value := range list.values {
		total += value
	}

	totalValue, err := NewDBValue(dataType)
	if err != nil {
		return nil, wrap.Error(err, "failed to create value for total of aggregated values")
	}

	if ok := totalValue.Set(total); !ok {
		return nil, fmt.Errorf(
			"failed to set aggregated values total '%v' with data type '%v'",
			total,
			dataType,
		)
	}

	return totalValue, nil
}

func (list *aggregatedValues[T]) Truncate(maxLength int) {
	if len(list.values) > maxLength {
		list.values = list.values[:maxLength]
	}
}

func (list aggregatedValues[T]) MarshalJSON() ([]byte, error) {
	return json.Marshal(list.values)
}

func (list *aggregatedValues[T]) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &list.values)
}
