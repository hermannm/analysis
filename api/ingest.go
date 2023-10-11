package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"hermannm.dev/analysis/csv"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

// Expects:
//   - query parameter 'table': name of table to create
//   - multipart form field 'tableSchema': JSON-encoded db.TableSchema
//   - multipart form field 'csvFile': CSV file to read data from
func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing 'table' query parameter in request")
		return
	}

	schema, err := getTableSchemaFromRequest(req)
	if err != nil {
		sendClientError(res, err, "")
		return
	}

	csvFile, _, err := req.FormFile("csvFile")
	if err != nil {
		sendClientError(res, err, "failed to get CSV file from request")
		return
	}
	defer csvFile.Close()

	if err := api.db.CreateTable(req.Context(), table, schema); err != nil {
		sendServerError(res, err, "failed to create table from uploaded CSV")
		return
	}

	csvReader, err := csv.NewReader(csvFile, true)
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
//   - multipart form field 'tableSchema': JSON-encoded db.TableSchema
//   - multipart form field 'csvFile': CSV file to read data from
func (api AnalysisAPI) UpdateTableWithCSV(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing query parameter 'table'")
		return
	}

	schema, err := getTableSchemaFromRequest(req)
	if err != nil {
		sendClientError(res, err, "")
		return
	}

	csvFile, _, err := req.FormFile("csvFile")
	if err != nil {
		sendClientError(res, err, "failed to get file upload from request")
		return
	}
	defer csvFile.Close()

	csvReader, err := csv.NewReader(csvFile, true)
	if err != nil {
		sendServerError(res, nil, "failed to read uploaded CSV file")
		return
	}

	if err := api.db.UpdateTableData(req.Context(), table, schema, csvReader); err != nil {
		sendServerError(res, err, "failed to update table with uploaded CSV")
		return
	}
}

func getTableSchemaFromRequest(req *http.Request) (db.TableSchema, error) {
	var schema db.TableSchema

	schemaInput := req.FormValue("tableSchema")
	if schemaInput == "" {
		return db.TableSchema{}, errors.New("missing 'tableSchema' field in request")
	}
	if err := json.Unmarshal([]byte(schemaInput), &schema); err != nil {
		return db.TableSchema{}, wrap.Error(err, "failed to parse table schema from request")
	}
	if errs := schema.Validate(); len(errs) > 0 {
		return db.TableSchema{}, wrap.Errors("invalid table schema", errs...)
	}

	return schema, nil
}
