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
//   - multipart form field 'tableSchema': JSON-encoded db.TableSchema
//   - multipart form field 'csvFile': CSV file to read data from
func (api AnalysisAPI) CreateTableFromCSV(res http.ResponseWriter, req *http.Request) {
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

	if err := api.db.CreateTable(req.Context(), schema); err != nil {
		sendServerError(res, err, "failed to create table from uploaded CSV")
		return
	}

	if err := api.db.StoreTableSchema(req.Context(), schema); err != nil {
		_, dropErr := api.db.DropTable(req.Context(), schema.TableName)
		if dropErr == nil {
			sendServerError(res, err, "failed to store table schema")
			return
		} else {
			sendServerError(res, wrap.Errors(
				"failed to store table schema AND failed to clean up invalid created table afterwards",
				err,
				dropErr,
			), "")
			return
		}
	}

	csvReader, err := csv.NewReader(csvFile, true)
	if err != nil {
		sendServerError(res, err, "failed to read uploaded CSV file")
		return
	}

	if err := api.db.InsertTableData(req.Context(), schema, csvReader); err != nil {
		sendServerError(res, err, "failed to insert CSV data after creating table")
		return
	}
}

// Expects:
//   - multipart form field 'tableSchema': JSON-encoded db.TableSchema
//   - multipart form field 'csvFile': CSV file to read data from
func (api AnalysisAPI) InsertDataFromCSV(res http.ResponseWriter, req *http.Request) {
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

	if err := api.db.InsertTableData(req.Context(), schema, csvReader); err != nil {
		sendServerError(res, err, "failed to insert data from uploaded CSV")
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
	if err := schema.Validate(); err != nil {
		return db.TableSchema{}, wrap.Error(err, "invalid table schema")
	}

	return schema, nil
}
