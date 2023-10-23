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

	Query(ctx context.Context, query Query, table string) (QueryResult, error)

	GetTableSchema(ctx context.Context, table string) (TableSchema, error)
}

type DataSource interface {
	ReadRow() (row []string, rowNumber int, done bool, err error)
}
