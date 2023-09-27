package db

import (
	"hermannm.dev/enumnames"
)

type AggregationType uint8

const (
	AggregationTypeSum         AggregationType = 1
	AggregationTypeAverage     AggregationType = 2
	AggregationTypeMax         AggregationType = 3
	AggregationTypeMin         AggregationType = 4
	AggregationTypeValueCount  AggregationType = 5
	AggregationTypeCardinality AggregationType = 6
	AggregationTypePercentiles AggregationType = 7
)

var aggregationTypeNames = enumnames.NewMap(map[AggregationType]string{
	AggregationTypeSum:         "Sum",
	AggregationTypeAverage:     "Average",
	AggregationTypeMax:         "Max",
	AggregationTypeMin:         "Min",
	AggregationTypeValueCount:  "Value count",
	AggregationTypeCardinality: "Cardinality",
	AggregationTypePercentiles: "Percentiles",
})

func (aggregationType AggregationType) IsValid() bool {
	return aggregationTypeNames.ContainsEnumValue(aggregationType)
}

func (aggregationType AggregationType) String() string {
	return aggregationTypeNames.GetNameOrFallback(aggregationType, "[INVALID AGGREGATION TYPE]")
}

func (aggregationType AggregationType) MarshalJSON() ([]byte, error) {
	return aggregationTypeNames.MarshalToNameJSON(aggregationType)
}

func (aggregationType *AggregationType) UnmarshalJSON(bytes []byte) error {
	return aggregationTypeNames.UnmarshalFromNameJSON(bytes, aggregationType)
}
