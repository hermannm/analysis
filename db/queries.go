package db

type Query struct {
	Aggregations []Aggregation `json:"aggregations"`
	RowSplits    []RowSplit    `json:"rowSplits"`
	ColumnSplits []ColumnSplit `json:"columnSplits"`
}

type Aggregation struct {
	Field string          `json:"field"`
	Type  AggregationType `json:"aggregation"`
}

type RowSplit struct{}

type ColumnSplit struct{}

type QueryResult struct {
	Rows    []RowResult
	Columns []ColumnResult
	Totals  []float64
}

type RowResult struct{}

type ColumnResult struct{}
