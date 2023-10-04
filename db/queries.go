package db

type Query struct {
	ValueAggregation ValueAggregation `json:"valueAggregations"`
	RowSplit         Split            `json:"rowSplit"`
	ColumnSplit      Split            `json:"columnSplit"`
}

type ValueAggregation struct {
	BaseColumnName     string      `json:"baseColumnName"`
	BaseColumnDataType DataType    `json:"baseColumnDataType"`
	Aggregation        Aggregation `json:"aggregation"`
}

type Split struct {
	SplitMetadata
	SortOrder SortOrder `json:"sortOrder"`
	Limit     int       `json:"limit"`
}

type SplitMetadata struct {
	BaseColumnName     string   `json:"baseColumnName"`
	BaseColumnDataType DataType `json:"baseColumnDataType"`
	// May only be present if BaseColumnDataType is INTEGER.
	IntegerInterval int `json:"numberIntervalInt"`
	// May only be present if BaseColumnDataType is FLOAT.
	FloatInterval float64 `json:"numberIntervalFloat"`
	// May only be present if BaseColumnDataType is TIMESTAMP.
	DateInterval *DateInterval `json:"dateInterval,omitempty"`
}

type QueryResult struct {
	ValueAggregationDataType DataType `json:"valueAggregationDataType"`

	Rows     []RowResult   `json:"rows"`
	RowsMeta SplitMetadata `json:"rowsMeta"`

	Columns     []ColumnResult `json:"columns"`
	ColumnsMeta SplitMetadata  `json:"columnsMeta"`
}

type RowResult struct {
	BaseColumnValue any `json:"baseColumnValue"`
	// Either []int64 or []float64.
	Values any `json:"values"`
	// Either int64 or float64 (same as the type of Values).
	Total any `json:"total"`
}

type ColumnResult struct {
	BaseColumnValue any `json:"baseColumnValue"`
	// Either int64 or float64.
	Total any
}
