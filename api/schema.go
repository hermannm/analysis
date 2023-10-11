package api

import (
	"net/http"

	"hermannm.dev/analysis/csv"
)

// Expects:
//   - query parameter 'table': name of existing table to get schema for
//
// Returns:
//   - JSON-encoded db.TableSchema
func (api AnalysisAPI) GetTableSchema(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing 'table' query parameter in request")
		return
	}

	schema, err := api.db.GetTableSchema(req.Context(), table)
	if err != nil {
		sendServerError(res, err, "failed to get table schema")
		return
	}

	sendJSON(res, schema)
}

const maxRowsToCheckForCSVSchemaDeduction = 100

// Expects:
//   - multipart form field 'csvFile': CSV file to deduce types from
//
// Returns:
//   - JSON-encoded db.TableSchema
func (api AnalysisAPI) DeduceCSVTableSchema(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("csvFile")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	csvReader, err := csv.NewReader(csvFile, false)
	if err != nil {
		sendServerError(res, err, "failed to read uploaded CSV file")
		return
	}

	schema, err := csvReader.DeduceTableSchema(maxRowsToCheckForCSVSchemaDeduction)
	if err != nil {
		sendServerError(res, err, "failed to deduce table schema from uploaded CSV")
		return
	}

	sendJSON(res, schema)
}
