package api

import (
	"fmt"
	"net/http"

	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/db"
)

type AnalysisAPI struct {
	db     db.AnalysisDatabase
	router *http.ServeMux
	config config.API
}

func NewAnalysisAPI(
	db db.AnalysisDatabase,
	router *http.ServeMux,
	config config.Config,
) AnalysisAPI {
	api := AnalysisAPI{db: db, router: router, config: config.API}

	api.router.HandleFunc("/create-table-from-csv", api.CreateTableFromCSV)
	api.router.HandleFunc("/update-table-with-csv", api.UpdateTableWithCSV)
	api.router.HandleFunc("/aggregate", api.Aggregate)

	return api
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}
