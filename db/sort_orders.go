package db

import "hermannm.dev/enumnames"

type SortOrder int8

const (
	SortOrderAscending SortOrder = iota + 1
	SortOrderDescending
)

var sortOrderMap = enumnames.NewMap(map[SortOrder]string{
	SortOrderAscending:  "ASCENDING",
	SortOrderDescending: "DESCENDING",
})

func (sortOrder SortOrder) IsValid() bool {
	return sortOrderMap.ContainsKey(sortOrder)
}

func (sortOrder SortOrder) String() string {
	return sortOrderMap.GetNameOrFallback(sortOrder, "INVALID_SORT_ORDER")
}

func (sortOrder SortOrder) MarshalJSON() ([]byte, error) {
	return sortOrderMap.MarshalToNameJSON(sortOrder)
}

func (sortOrder *SortOrder) UnmarshalJSON(bytes []byte) error {
	return sortOrderMap.UnmarshalFromNameJSON(bytes, sortOrder)
}
