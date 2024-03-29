package db

import "context"

type AnalysisDB interface {
	RunAnalysisQuery(
		ctx context.Context,
		analysis AnalysisQuery,
		table string,
	) (AnalysisResult, error)

	CreateTable(ctx context.Context, schema TableSchema) error

	IngestData(ctx context.Context, data DataSource, schema TableSchema) error

	DropTable(ctx context.Context, table string) (alreadyDropped bool, err error)

	CreateStoredSchemasTable(ctx context.Context) error

	StoreTableSchema(ctx context.Context, schema TableSchema) error

	GetTableSchema(ctx context.Context, table string) (TableSchema, error)

	DeleteTableSchema(ctx context.Context, table string) error
}

type DataSource interface {
	ReadRow() (row []string, rowNumber int, done bool, err error)
}
