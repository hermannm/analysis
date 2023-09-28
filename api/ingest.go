package api

import (
	"net/http"

	"hermannm.dev/analysis/csv"
)

const maxRowsToCheckForCSVSchemaDeduction = 100

func (api AnalysisAPI) DeduceCSVDataTypes(res http.ResponseWriter, req *http.Request) {
	csvFile, _, err := req.FormFile("upload")
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

	schema, err := csvReader.DeduceDataTypes(maxRowsToCheckForCSVSchemaDeduction)
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
