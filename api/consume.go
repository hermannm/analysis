package api

import (
	"encoding/json"
	"net/http"
)

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
