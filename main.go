package main

import (
	"log"
	"net/http"

	"hermannm.dev/analysis/api"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func main() {
	log.Println("Loading environment variables...")
	config, err := config.ReadFromEnv()
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to read config from env"))
	}

	log.Println("Connecting to ClickHouse...")
	db, err := db.NewAnalysisDatabase(config)
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to initialize database"))
	}

	analysisAPI := api.NewAnalysisAPI(db, http.DefaultServeMux, config)

	log.Printf("Listening on port %s...", config.API.Port)
	if err := analysisAPI.ListenAndServe(); err != nil {
		log.Fatalln(wrap.Error(err, "server stopped"))
	}
}
