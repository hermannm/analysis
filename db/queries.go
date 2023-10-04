package db

type Query struct {
	ValueAggregation ValueAggregation `json:"valueAggregations"`
	RowSplit         Split            `json:"rowSplits"`
	ColumnSplit      Split            `json:"columnSplits"`
}

type ValueAggregation struct {
	BaseColumnName     string      `json:"baseColumnName"`
	BaseColumnDataType DataType    `json:"baseColumnDataType"`
	Aggregation        Aggregation `json:"aggregation"`
}

type Split struct {
	BaseColumnName     string    `json:"baseColumnName"`
	BaseColumnDataType DataType  `json:"baseColumnDataType"`
	SortOrder          SortOrder `json:"sortOrder"`
	Limit              int       `json:"limit"`
	// Only valid if BaseColumnDataType is INTEGER.
	IntegerInterval int `json:"numberIntervalInt"`
	// Only valid if BaseColumnDataType is FLOAT.
	FloatInterval float64 `json:"numberIntervalFloat"`
	// Only valid if BaseColumnDataType is TIMESTAMP.
	DateInterval *DateInterval `json:"dateInterval,omitempty"`
}

type QueryResult struct {
	Rows    []RowResult    `json:"rows"`
	Columns []ColumnResult `json:"columns"`
}

type RowResult struct {
	ColumnName  string `json:"field"`
	ColumnValue string `json:"key"`
	// Either [][]int64 or [][]float64.
	Values any `json:"values"`
	// Either int64 or float64 (same as the type of Values).
	Total any `json:"total"`
}

type ColumnResult struct {
	Field string `json:"field"`
	Key   string `json:"key"`
	// Either int64 or float64.
	Total any
}

type ResultMetadata struct {
	// Either int64 or float64 (same as the value type in the connected Result).
	NumberInterval any
	DateInterval   DateInterval
	// Either int64 or float64.
	NumericKey any
}
