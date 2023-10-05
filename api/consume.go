package api

import (
	"encoding/json"
	"net/http"

	"hermannm.dev/analysis/db"
)

func (api AnalysisAPI) Query(res http.ResponseWriter, req *http.Request) {
	table := req.URL.Query().Get("table")
	if table == "" {
		sendClientError(res, nil, "missing 'table' query parameter in request")
		return
	}

	var query db.Query
	if err := json.NewDecoder(req.Body).Decode(&query); err != nil {
		sendClientError(res, err, "failed to parse query from request body")
		return
	}

	queryResult, err := api.db.Query(req.Context(), query, table)
	if err != nil {
		sendServerError(res, err, "failed to run query")
		return
	}

	sendJSON(res, queryResult)
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
