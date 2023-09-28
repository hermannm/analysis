package api

import (
	"context"
	"fmt"
	"net/http"

	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/datatypes"
	"hermannm.dev/analysis/queries"
)

type AnalysisAPI struct {
	db     AnalysisDB
	router *http.ServeMux
	config config.API
}

type AnalysisDB interface {
	CreateTableSchema(ctx context.Context, table string, schema datatypes.Schema) error

	UpdateTableData(
		ctx context.Context,
		table string,
		schema datatypes.Schema,
		data datatypes.DataSource,
	) error

	Aggregate(
		ctx context.Context,
		tableName string,
		groupColumn string,
		aggregationColumn string,
		limit int,
	) (aggregates []queries.Aggregate, err error)
}

func NewAnalysisAPI(db AnalysisDB, router *http.ServeMux, config config.Config) AnalysisAPI {
	api := AnalysisAPI{db: db, router: router, config: config.API}

	api.router.HandleFunc("/deduce-csv-data-types", api.DeduceCSVDataTypes)
	api.router.HandleFunc("/create-table-from-csv", api.CreateTableFromCSV)
	api.router.HandleFunc("/update-table-with-csv", api.UpdateTableWithCSV)
	api.router.HandleFunc("/aggregate", api.Aggregate)

	return api
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}
