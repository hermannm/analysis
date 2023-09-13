package api

import (
	"fmt"
	"net/http"

	"hermannm.dev/analysis/db"
)

type AnalysisAPI struct {
	db     db.AnalysisDatabase
	router *http.ServeMux
	config Config
}

type Config struct {
	Port string
}

func NewAnalysisAPI(db db.AnalysisDatabase, router *http.ServeMux, config Config) AnalysisAPI {
	return AnalysisAPI{db: db, router: router, config: config}
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}
