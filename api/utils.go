package api

import (
	"encoding/json"
	"log"
	"net/http"

	"hermannm.dev/wrap"
)

func sendError(res http.ResponseWriter, err error, message string, statusCode int) {
	if err != nil {
		if message == "" {
			message = err.Error()
		} else {
			message = wrap.Error(err, message).Error()
		}
	}

	log.Println(message)
	http.Error(res, message, statusCode)
}

func sendClientError(res http.ResponseWriter, err error, message string) {
	sendError(res, err, message, http.StatusBadRequest)
}

func sendServerError(res http.ResponseWriter, err error, message string) {
	sendError(res, err, message, http.StatusInternalServerError)
}

func sendJSON(res http.ResponseWriter, value any) {
	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(res).Encode(value); err != nil {
		sendError(res, err, "failed to serialize response", http.StatusInternalServerError)
	}
}
