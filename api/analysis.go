package api

import (
	"encoding/json"
	"net/http"

	"hermannm.dev/analysis/db"
)

// Expects:
//   - query parameter 'table': name of table to analyze data for
//   - body: JSON-encoded db.AnalysisQuery
//
// Returns:
//   - JSON-encoded db.AnalysisResult
func (api AnalysisAPI) RunAnalysisQuery(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing 'table' query parameter in request")
		return
	}

	var analysis db.AnalysisQuery
	if err := json.NewDecoder(req.Body).Decode(&analysis); err != nil {
		sendClientError(res, err, "failed to parse analysis query from request body")
		return
	}

	analysisResult, err := api.db.RunAnalysisQuery(req.Context(), analysis, table)
	if err != nil {
		sendServerError(res, err, "failed to run analysis query")
		return
	}

	sendJSON(res, analysisResult)
}
