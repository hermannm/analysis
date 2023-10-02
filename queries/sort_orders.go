package queries

import "hermannm.dev/enumnames"

type SortOrder uint8

const (
	SortOrderAscending SortOrder = iota + 1
	SortOrderDescending
)

var sortOrderNames = enumnames.NewMap(map[SortOrder]string{
	SortOrderAscending:  "ASCENDING",
	SortOrderDescending: "DESCENDING",
})

func (sortOrder SortOrder) IsValid() bool {
	return sortOrderNames.ContainsEnumValue(sortOrder)
}

func (sortOrder SortOrder) String() string {
	return sortOrderNames.GetNameOrFallback(sortOrder, "INVALID_SORT_ORDER")
}

func (sortOrder SortOrder) MarshalJSON() ([]byte, error) {
	return sortOrderNames.MarshalToNameJSON(sortOrder)
}

func (sortOrder *SortOrder) UnmarshalJSON(bytes []byte) error {
	return sortOrderNames.UnmarshalFromNameJSON(bytes, sortOrder)
}
