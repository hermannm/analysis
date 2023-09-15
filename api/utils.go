package api

import (
	"encoding/json"
	"log"
	"net/http"

	"hermannm.dev/wrap"
)

func sendError(message string, statusCode int, err error, res http.ResponseWriter) {
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

func sendJSON(value any, res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	res.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(res).Encode(value); err != nil {
		sendError("failed to serialize response", http.StatusInternalServerError, err, res)
	}
}
