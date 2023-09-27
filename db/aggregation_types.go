package db

import (
	"hermannm.dev/enumnames"
)

type AggregationType uint8

const (
	AggregationTypeSum AggregationType = iota + 1
	AggregationTypeAverage
	AggregationTypeMax
	AggregationTypeMin
	AggregationTypeValueCount
	AggregationTypeCardinality
	AggregationTypePercentiles
)

var aggregationTypeNames = enumnames.NewMap(map[AggregationType]string{
	AggregationTypeSum:         "SUM",
	AggregationTypeAverage:     "AVERAGE",
	AggregationTypeMax:         "MAX",
	AggregationTypeMin:         "MIN",
	AggregationTypeValueCount:  "VALUE_COUNT",
	AggregationTypeCardinality: "CARDINALITY",
	AggregationTypePercentiles: "PERCENTILES",
})

func (aggregationType AggregationType) IsValid() bool {
	return aggregationTypeNames.ContainsEnumValue(aggregationType)
}

func (aggregationType AggregationType) String() string {
	return aggregationTypeNames.GetNameOrFallback(aggregationType, "INVALID_AGGREGATION_TYPE")
}

func (aggregationType AggregationType) MarshalJSON() ([]byte, error) {
	return aggregationTypeNames.MarshalToNameJSON(aggregationType)
}

func (aggregationType *AggregationType) UnmarshalJSON(bytes []byte) error {
	return aggregationTypeNames.UnmarshalFromNameJSON(bytes, aggregationType)
}
