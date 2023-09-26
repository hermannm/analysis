package db

import (
	"encoding/json"
	"errors"
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

var aggregationTypeNames = map[AggregationType]string{
	AggregationTypeSum:         "Sum",
	AggregationTypeAverage:     "Average",
	AggregationTypeMax:         "Max",
	AggregationTypeMin:         "Min",
	AggregationTypeValueCount:  "Value count",
	AggregationTypeCardinality: "Cardinality",
	AggregationTypePercentiles: "Percentiles",
}

func (aggregationType AggregationType) IsValid() bool {
	_, ok := aggregationTypeNames[aggregationType]
	return ok
}

func (aggregationType AggregationType) String() string {
	if name, ok := aggregationTypeNames[aggregationType]; ok {
		return name
	} else {
		return "[INVALID AGGREGATION TYPE]"
	}
}

func (aggregationType AggregationType) MarshalJSON() ([]byte, error) {
	if name, ok := aggregationTypeNames[aggregationType]; ok {
		return json.Marshal(name)
	} else {
		return nil, errors.New("unrecognized aggregation type")
	}
}

func (aggregationType *AggregationType) UnmarshalJSON(bytes []byte) error {
	for candidate, name := range aggregationTypeNames {
		if name == string(bytes) {
			*aggregationType = candidate
			return nil
		}
	}

	return errors.New("unrecognized aggregation type")
}
