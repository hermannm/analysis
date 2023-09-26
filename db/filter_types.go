package db

import (
	"encoding/json"
	"errors"
)

type FilterType uint8

const (
	FilterTypeInclude        FilterType = 1
	FilterTypeExists         FilterType = 2
	FilterTypeRange          FilterType = 3
	FilterTypeExclude        FilterType = 4
	FilterTypeSearchTerm     FilterType = 5
	FilterTypeNumericInclude FilterType = 6
	FilterTypeNumericExclude FilterType = 7
	FilterTypeStaticDate     FilterType = 8
	FilterTypePopulatedDate  FilterType = 9
	FilterTypeBoolean        FilterType = 10
	FilterTypeConditional    FilterType = 11
)

var filterTypeNames = map[FilterType]string{
	FilterTypeInclude:        "Include",
	FilterTypeExists:         "Exists",
	FilterTypeRange:          "Range",
	FilterTypeExclude:        "Exclude",
	FilterTypeSearchTerm:     "Search term",
	FilterTypeNumericInclude: "Numeric include",
	FilterTypeNumericExclude: "Numeric exclude",
	FilterTypeStaticDate:     "Static date",
	FilterTypePopulatedDate:  "Populated date",
	FilterTypeBoolean:        "Boolean",
	FilterTypeConditional:    "Conditional",
}

func (filterType FilterType) IsValid() bool {
	_, ok := filterTypeNames[filterType]
	return ok
}

func (filterType FilterType) String() string {
	if name, ok := filterTypeNames[filterType]; ok {
		return name
	} else {
		return "[INVALID FILTER TYPE]"
	}
}

func (filterType FilterType) MarshalJSON() ([]byte, error) {
	if name, ok := filterTypeNames[filterType]; ok {
		return json.Marshal(name)
	} else {
		return nil, errors.New("unrecognized filter type")
	}
}

func (filterType *FilterType) UnmarshalJSON(bytes []byte) error {
	for candidate, name := range filterTypeNames {
		if name == string(bytes) {
			*filterType = candidate
			return nil
		}
	}

	return errors.New("unrecognized filter type")
}
