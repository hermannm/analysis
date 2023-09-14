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
	api.router.HandleFunc("update-with-csv", api.UpdateTableDataWithCSV)

	return api
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}

// Endpointing for creating a new table from an uploaded CSV file.
func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
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

	if err := api.db.CreateTableSchemaFromCSV(req.Context(), table, file); err != nil {
		sendError(
			"failed to create table from uploaded CSV", http.StatusInternalServerError, err, res,
		)
		return
	}

	if err := api.db.UpdateTableDataWithCSV(req.Context(), table, file); err != nil {
		sendError(
			"failed to insert CSV data after creating table", http.StatusInternalServerError, err, res,
		)
		return
	}
}

// Endpoint for uploading CSV data to an existing table.
func (api AnalysisAPI) UpdateTableDataWithCSV(res http.ResponseWriter, req *http.Request) {
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

	if err := api.db.UpdateTableDataWithCSV(req.Context(), table, file); err != nil {
		sendError(
			"failed to update table with uploaded CSV", http.StatusInternalServerError, err, res,
		)
		return
	}
}
