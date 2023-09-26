package db

type Query struct {
	Aggregations []Aggregation `json:"aggregations"`
	RowSplits    []RowSplit    `json:"rowSplits"`
	ColumnSplits []ColumnSplit `json:"columnSplits"`
}

type Aggregation struct {
	Field       string          `json:"field"`
	Aggregation AggregationType `json:"aggregation"`
}

type RowSplit struct{}

type ColumnSplit struct{}
