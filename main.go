package main

import (
	"fmt"
	"net/http"
	"os"

	"hermannm.dev/analysis/api"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/db"
	"hermannm.dev/analysis/db/clickhouse"
	"hermannm.dev/analysis/db/elasticsearch"
	"hermannm.dev/analysis/log"
)

func main() {
	log.Info("Loading environment variables...")
	conf, err := config.ReadFromEnv()
	if err != nil {
		log.Error(err, "failed to read config from env")
		os.Exit(1)
	}

	var db db.AnalysisDB
	switch conf.DB {
	case config.DBClickHouse:
		log.Info("Connecting to ClickHouse...")
		db, err = clickhouse.NewClickHouseDB(conf)
	case config.DBElasticsearch:
		log.Info("Connecting to Elasticsearch...")
		db, err = elasticsearch.NewElasticsearchDB(conf)
	default:
		err = fmt.Errorf("unrecognized database '%s' from config", conf.DB)
	}
	if err != nil {
		log.Error(err, "failed to initialize database")
		os.Exit(1)
	}

	analysisAPI := api.NewAnalysisAPI(db, http.DefaultServeMux, conf)

	log.Infof("Listening on port %s...", conf.API.Port)
	if err := analysisAPI.ListenAndServe(); err != nil {
		log.Error(err, "server stopped")
		os.Exit(1)
	}
}
