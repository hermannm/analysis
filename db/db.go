package db

import "context"

type AnalysisDB interface {
	CreateTable(ctx context.Context, table string, schema TableSchema) error

	UpdateTableData(
		ctx context.Context,
		table string,
		schema TableSchema,
		data DataSource,
	) error

	Aggregate(
		ctx context.Context,
		tableName string,
		groupColumn string,
		aggregationColumn string,
		limit int,
	) (aggregates []Aggregate, err error)
}

type DataSource interface {
	ReadRow() (row []string, rowNumber int, done bool, err error)
}

type Aggregate struct {
	Column string `ch:"analysis_group_column" json:"column"`
	Sum    int64  `ch:"analysis_aggregate"    json:"sum"`
}
