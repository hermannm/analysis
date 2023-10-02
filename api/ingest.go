package api

import (
	"encoding/json"
	"net/http"

	"hermannm.dev/analysis/csv"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

const maxRowsToCheckForCSVSchemaDeduction = 100

// Expects:
//   - multipart form field 'csvFile': CSV file to deduce types from
//
// Returns:
//   - JSON-encoded db.Schema
func (api AnalysisAPI) DeduceCSVDataTypes(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("csvFile")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, err, "failed to read uploaded CSV file")
		return
	}

	schema, err := csvReader.DeduceDataTypes(maxRowsToCheckForCSVSchemaDeduction)
	if err != nil {
		sendServerError(res, err, "failed to deduce data types from uploaded CSV")
		return
	}

	sendJSON(res, schema)
}

// Expects:
//   - query parameter 'table': name of table to create
//   - multipart form field 'schema': JSON-encoded db.Schema
//   - multipart form field 'csvFile': CSV file to read data from
func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing 'table' query parameter in request")
		return
	}

	var schema db.Schema
	schemaInput := req.FormValue("schema")
	if schemaInput == "" {
		sendClientError(res, nil, "missing 'schema' field in request")
		return
	}
	if err := json.Unmarshal([]byte(schemaInput), &schema); err != nil {
		sendClientError(res, err, "failed to parse schema from request")
		return
	}
	if errs := schema.Validate(); len(errs) > 0 {
		sendClientError(res, wrap.Errors("invalid schema", errs...), "")
		return
	}

	csvFile, _, err := req.FormFile("csvFile")
	if err != nil {
		sendClientError(res, err, "failed to get CSV file from request")
		return
	}
	defer csvFile.Close()

	if err := api.db.CreateTableSchema(req.Context(), table, schema); err != nil {
		sendServerError(res, err, "failed to create table from uploaded CSV")
		return
	}

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, err, "failed to read uploaded CSV file")
		return
	}

	if err := api.db.UpdateTableData(req.Context(), table, schema, csvReader); err != nil {
		sendServerError(res, err, "failed to insert CSV data after creating table")
		return
	}
}

// Expects:
//   - query parameter 'table': name of table to update
//   - multipart form field 'csvFile': CSV file to read data from
func (api AnalysisAPI) UpdateTableWithCSV(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing query parameter 'table'")
		return
	}

	csvFile, _, err := req.FormFile("csvFile")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	csvReader, err := csv.NewReader(csvFile)
	if err != nil {
		sendServerError(res, nil, "failed to read uploaded CSV file")
		return
	}

	schema, err := csvReader.DeduceDataTypes(maxRowsToCheckForCSVSchemaDeduction)
	if err != nil {
		sendServerError(res, err, "failed to deduce column data types from uploaded CSV")
		return
	}

	if err := api.db.UpdateTableData(req.Context(), table, schema, csvReader); err != nil {
		sendServerError(res, err, "failed to update table with uploaded CSV")
		return
	}
}
