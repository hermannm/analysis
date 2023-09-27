package db

import (
	"hermannm.dev/enumnames"
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

var filterTypeNames = enumnames.NewMap(map[FilterType]string{
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
})

func (filterType FilterType) IsValid() bool {
	return filterTypeNames.ContainsEnumValue(filterType)
}

func (filterType FilterType) String() string {
	return filterTypeNames.GetNameOrFallback(filterType, "[INVALID FILTER TYPE]")
}

func (filterType FilterType) MarshalJSON() ([]byte, error) {
	return filterTypeNames.MarshalToNameJSON(filterType)
}

func (filterType *FilterType) UnmarshalJSON(bytes []byte) error {
	return filterTypeNames.UnmarshalFromNameJSON(bytes, filterType)
}
