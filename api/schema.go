package api

import "net/http"

// Expects:
//   - query parameter 'table': name of table to create
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
