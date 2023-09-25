package api

import (
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/db"
)

type AnalysisAPI struct {
	db     db.AnalysisDB
	router *httprouter.Router
	config config.API
}

func NewAnalysisAPI(db db.AnalysisDB, config config.Config) AnalysisAPI {
	api := AnalysisAPI{db: db, router: httprouter.New(), config: config.API}

	api.router.HandlerFunc(http.MethodPost, "/create-table-from-csv", api.CreateTableFromCSV)
	api.router.HandlerFunc(http.MethodPatch, "/update-table-with-csv", api.UpdateTableWithCSV)
	api.router.HandlerFunc(http.MethodPost, "/aggregate", api.Aggregate)

	return api
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}
