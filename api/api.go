package api

import (
	"fmt"
	"net/http"

	"hermannm.dev/analysis/csv"
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

	api.router.HandleFunc("/create-table-from-csv", api.CreateTableFromCSV)
	api.router.HandleFunc("/update-table-with-csv", api.UpdateTableWithCSV)

	return api
}

func (api AnalysisAPI) ListenAndServe() error {
	return http.ListenAndServe(fmt.Sprintf(":%s", api.config.Port), api.router)
}

// Endpointing for creating a new table from an uploaded CSV file.
func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("upload")
	if err != nil {
		sendError("failed to get file upload from request", http.StatusBadRequest, err, res)
		return
	}
	defer csvFile.Close()

	tableName := req.URL.Query().Get("table")
	if tableName == "" {
		sendError("missing query parameter 'table'", http.StatusBadRequest, err, res)
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendError("failed to read uploaded CSV file", http.StatusInternalServerError, err, res)
		return
	}

	columns, err := csvReader.DeduceColumnTypes(100)
	if err != nil {
		sendError(
			"failed to deduce column data types from uploaded CSV",
			http.StatusInternalServerError, err, res,
		)
		return
	}

	if err := api.db.CreateTableSchema(req.Context(), tableName, columns); err != nil {
		sendError(
			"failed to create table from uploaded CSV", http.StatusInternalServerError, err, res,
		)
		return
	}

	if err := api.db.UpdateTableWithCSV(req.Context(), tableName, csvReader); err != nil {
		sendError(
			"failed to insert CSV data after creating table",
			http.StatusInternalServerError, err, res,
		)
		return
	}
}

// Endpoint for uploading CSV data to an existing table.
func (api AnalysisAPI) UpdateTableWithCSV(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("upload")
	if err != nil {
		sendError("failed to get file upload from request", http.StatusBadRequest, err, res)
		return
	}
	defer csvFile.Close()

	tableName := req.URL.Query().Get("table")
	if tableName == "" {
		sendError("missing query parameter 'table'", http.StatusBadRequest, err, res)
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendError("failed to read uploaded CSV file", http.StatusInternalServerError, err, res)
		return
	}

	if err := api.db.UpdateTableWithCSV(req.Context(), tableName, csvReader); err != nil {
		sendError(
			"failed to update table with uploaded CSV", http.StatusInternalServerError, err, res,
		)
		return
	}
}
