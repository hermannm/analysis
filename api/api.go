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
	api := AnalysisAPI{db: db, router: router, config: config}

	api.router.HandleFunc("create-from-csv", api.CreateTableFromCSV)
	api.router.HandleFunc("update-with-csv", api.UpdateTableWithCSV)

	return api
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}

type CreateTableResponse struct {
	TableName string `json:"tableName"`
}

func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
	file, _, err := req.FormFile("upload")
	if err != nil {
		sendError("failed to get file upload from request", http.StatusBadRequest, err, res)
		return
	}
	defer file.Close()

	tableName, err := api.db.CreateTableFromCSV(req.Context(), file)
	if err != nil {
		sendError(
			"failed to create table from uploaded CSV", http.StatusInternalServerError, err, res,
		)
		return
	}

	sendJSON(CreateTableResponse{TableName: tableName}, res)
}

func (api AnalysisAPI) UpdateTableWithCSV(res http.ResponseWriter, req *http.Request) {
	file, _, err := req.FormFile("upload")
	if err != nil {
		sendError("failed to get file upload from request", http.StatusBadRequest, err, res)
		return
	}
	defer file.Close()

	table := req.URL.Query().Get("table")
	if table == "" {
		sendError("missing query parameter 'table'", http.StatusBadRequest, err, res)
		return
	}

	if err := api.db.UpdateTableWithCSV(req.Context(), file, table); err != nil {
		sendError(
			"failed to update table with uploaded CSV", http.StatusInternalServerError, err, res,
		)
		return
	}
}
