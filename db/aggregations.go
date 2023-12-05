package db

import (
	"hermannm.dev/enumnames"
)

type AggregationKind int8

const (
	AggregationSum AggregationKind = iota + 1
	AggregationAverage
	AggregationMin
	AggregationMax
	AggregationCount
)

var aggregationMap = enumnames.NewMap(map[AggregationKind]string{
	AggregationSum:     "SUM",
	AggregationAverage: "AVERAGE",
	AggregationMin:     "MIN",
	AggregationMax:     "MAX",
	AggregationCount:   "COUNT",
})

func (kind AggregationKind) IsValid() bool {
	return aggregationMap.ContainsKey(kind)
}

func (kind AggregationKind) String() string {
	return aggregationMap.GetNameOrFallback(kind, "INVALID_AGGREGATION")
}

func (kind AggregationKind) MarshalJSON() ([]byte, error) {
	return aggregationMap.MarshalToNameJSON(kind)
}

func (kind *AggregationKind) UnmarshalJSON(bytes []byte) error {
	return aggregationMap.UnmarshalFromNameJSON(bytes, kind)
}
