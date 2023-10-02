package queries

import (
	"hermannm.dev/enumnames"
)

type Aggregation uint8

const (
	AggregationSum Aggregation = iota + 1
	AggregationAverage
	AggregationMax
	AggregationMin
	AggregationValueCount
	AggregationCardinality
	AggregationPercentiles
)

var aggregationTypeNames = enumnames.NewMap(map[Aggregation]string{
	AggregationSum:         "SUM",
	AggregationAverage:     "AVERAGE",
	AggregationMax:         "MAX",
	AggregationMin:         "MIN",
	AggregationValueCount:  "VALUE_COUNT",
	AggregationCardinality: "CARDINALITY",
	AggregationPercentiles: "PERCENTILES",
})

func (aggregationType Aggregation) IsValid() bool {
	return aggregationTypeNames.ContainsEnumValue(aggregationType)
}

func (aggregationType Aggregation) String() string {
	return aggregationTypeNames.GetNameOrFallback(aggregationType, "INVALID_AGGREGATION_TYPE")
}

func (aggregationType Aggregation) MarshalJSON() ([]byte, error) {
	return aggregationTypeNames.MarshalToNameJSON(aggregationType)
}

func (aggregationType *Aggregation) UnmarshalJSON(bytes []byte) error {
	return aggregationTypeNames.UnmarshalFromNameJSON(bytes, aggregationType)
}
