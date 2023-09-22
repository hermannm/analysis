package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/csv"
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

const maxRowsToCheckForCSVSchemaDeduction = 100

// Endpointing for creating a new table from an uploaded CSV file.
func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("upload")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing query parameter 'table'")
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, err, "failed to read uploaded CSV file")
		return
	}

	schema, err := csvReader.DeduceDataSchema(maxRowsToCheckForCSVSchemaDeduction)
	if err != nil {
		sendServerError(res, err, "failed to deduce column data types from uploaded CSV")
		return
	}

	if err := api.db.CreateTableSchema(req.Context(), table, schema); err != nil {
		sendServerError(res, err, "failed to create table from uploaded CSV")
		return
	}

	if err := api.db.UpdateTableData(req.Context(), table, schema, csvReader); err != nil {
		sendServerError(res, err, "failed to insert CSV data after creating table")
		return
	}

	sendJSON(res, schema)
}

// Endpoint for uploading CSV data to an existing table.
func (api AnalysisAPI) UpdateTableWithCSV(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("upload")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing query parameter 'table'")
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, nil, "failed to read uploaded CSV file")
		return
	}

	schema, err := csvReader.DeduceDataSchema(maxRowsToCheckForCSVSchemaDeduction)
	if err != nil {
		sendServerError(res, err, "failed to deduce column data types from uploaded CSV")
		return
	}

	if err := api.db.UpdateTableData(req.Context(), table, schema, csvReader); err != nil {
		sendServerError(res, err, "failed to update table with uploaded CSV")
		return
	}
}

type AggregateRequest struct {
	Table             string `json:"table"`
	GroupColumn       string `json:"groupColumn"`
	AggregationColumn string `json:"aggregationColumn"`
	Limit             int    `json:"limit"`
}

func (api AnalysisAPI) Aggregate(res http.ResponseWriter, req *http.Request) {
	var body AggregateRequest
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		sendClientError(res, err, "invalid request body")
		return
	}

	aggregates, err := api.db.Aggregate(
		req.Context(),
		body.Table,
		body.GroupColumn,
		body.AggregationColumn,
		body.Limit,
	)
	if err != nil {
		sendServerError(res, err, "")
		return
	}

	sendJSON(res, aggregates)
}
