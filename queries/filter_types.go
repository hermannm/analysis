package queries

import (
	"hermannm.dev/enumnames"
)

type FilterType uint8

const (
	FilterTypeInclude FilterType = iota + 1
	FilterTypeExists
	FilterTypeRange
	FilterTypeExclude
	FilterTypeSearchTerm
	FilterTypeNumericInclude
	FilterTypeNumericExclude
	FilterTypeStaticDate
	FilterTypePopulatedDate
	FilterTypeBoolean
	FilterTypeConditional
)

var filterTypeNames = enumnames.NewMap(map[FilterType]string{
	FilterTypeInclude:        "INCLUDE",
	FilterTypeExists:         "EXISTS",
	FilterTypeRange:          "RANGE",
	FilterTypeExclude:        "EXCLUDE",
	FilterTypeSearchTerm:     "SEARCH_TERM",
	FilterTypeNumericInclude: "NUMERIC_INCLUDE",
	FilterTypeNumericExclude: "NUMERIC_EXCLUDE",
	FilterTypeStaticDate:     "STATIC_DATE",
	FilterTypePopulatedDate:  "POPULATED_DATE",
	FilterTypeBoolean:        "BOOLEAN",
	FilterTypeConditional:    "CONDITIONAL",
})

func (filterType FilterType) IsValid() bool {
	return filterTypeNames.ContainsEnumValue(filterType)
}

func (filterType FilterType) String() string {
	return filterTypeNames.GetNameOrFallback(filterType, "INVALID_FILTER_TYPE")
}

func (filterType FilterType) MarshalJSON() ([]byte, error) {
	return filterTypeNames.MarshalToNameJSON(filterType)
}

func (filterType *FilterType) UnmarshalJSON(bytes []byte) error {
	return filterTypeNames.UnmarshalFromNameJSON(bytes, filterType)
}
