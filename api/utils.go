package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"hermannm.dev/analysis/log"
	"hermannm.dev/wrap"
)

func sendError(res http.ResponseWriter, err error, message string, statusCode int) {
	if err == nil {
		err = errors.New(message)
	}

	if message != "" {
		err = wrap.Error(err, message)
	}

	log.Error(err, "")
	http.Error(res, err.Error(), statusCode)
}

func sendClientError(res http.ResponseWriter, err error, message string) {
	sendError(res, err, message, http.StatusBadRequest)
}

func sendServerError(res http.ResponseWriter, err error, message string) {
	sendError(res, err, message, http.StatusInternalServerError)
}

func sendJSON(res http.ResponseWriter, value any) {
	res.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(res).Encode(value); err != nil {
		sendError(res, err, "failed to serialize response", http.StatusInternalServerError)
	}
}
