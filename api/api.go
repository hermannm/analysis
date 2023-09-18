package api

import (
	"fmt"
	"net/http"

	"hermannm.dev/analysis/csv"
	"hermannm.dev/analysis/datatypes"
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
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	tableName := req.URL.Query().Get("table")
	if tableName == "" {
		sendClientError(res, nil, "missing query parameter 'table'")
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, err, "failed to read uploaded CSV file")
		return
	}

	schema, err := csvReader.DeduceDataSchema(100)
	if err != nil {
		sendServerError(res, err, "failed to deduce column data types from uploaded CSV")
		return
	}

	if err := api.db.CreateTableSchema(req.Context(), tableName, schema); err != nil {
		sendServerError(res, err, "failed to create table from uploaded CSV")
		return
	}

	if err := api.db.UpdateTableWithCSV(req.Context(), tableName, schema, csvReader); err != nil {
		sendServerError(res, err, "failed to insert CSV data after creating table")
		return
	}
}

// Endpoint for uploading CSV data to an existing table.
func (api AnalysisAPI) UpdateTableWithCSV(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("upload")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	tableName := req.URL.Query().Get("table")
	if tableName == "" {
		sendClientError(res, nil, "missing query parameter 'table'")
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, nil, "failed to read uploaded CSV file")
		return
	}

	if err := api.db.UpdateTableWithCSV(
		req.Context(), tableName, datatypes.Schema{}, csvReader,
	); err != nil {
		sendServerError(res, err, "failed to update table with uploaded CSV")
		return
	}
}
