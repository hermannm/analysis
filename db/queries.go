package db

type Query struct {
	ValueAggregations []ValueAggregation `json:"valueAggregations"`
	RowSplits         []Split            `json:"rowSplits"`
	ColumnSplits      []Split            `json:"columnSplits"`
}

type ValueAggregation struct {
	Field       string      `json:"field"`
	Aggregation Aggregation `json:"aggregation"`
}

type Split struct {
	Field       string      `json:"field"`
	Aggregation Aggregation `json:"aggregation"`
	SortOrder   SortOrder   `json:"sortOrder"`
	Size        int         `json:"size"`
}

type QueryResult struct {
	Rows    []RowResult    `json:"rows"`
	Columns []ColumnResult `json:"columns"`
}

type RowResult struct {
	Field string `json:"field"`
	Key   string `json:"key"`
	// Either []int64 or []float64.
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
