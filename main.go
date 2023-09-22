package main

import (
	"os"

	"hermannm.dev/analysis/api"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/db"
	"hermannm.dev/analysis/log"
)

func main() {
	log.Info("Loading environment variables...")
	config, err := config.ReadFromEnv()
	if err != nil {
		log.Error(err, "failed to read config from env")
		os.Exit(1)
	}

	log.Info("Connecting to ClickHouse...")
	db, err := db.NewAnalysisDatabase(config)
	if err != nil {
		log.Error(err, "failed to initialize database")
		os.Exit(1)
	}

	analysisAPI := api.NewAnalysisAPI(db, config)

	log.Infof("Listening on port %s...", config.API.Port)
	if err := analysisAPI.ListenAndServe(); err != nil {
		log.Error(err, "server stopped")
		os.Exit(1)
	}
}
