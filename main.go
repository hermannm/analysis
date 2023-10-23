package main

import (
	"context"
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
	log.Initialize()

	log.Info("loading environment variables...")
	conf, err := config.ReadFromEnv()
	if err != nil {
		log.Error(err, "failed to read config from env")
		os.Exit(1)
	}

	var db db.AnalysisDB
	switch conf.DB {
	case config.DBClickHouse:
		log.Info("connecting to ClickHouse...")
		db, err = clickhouse.NewClickHouseDB(conf)
	case config.DBElasticsearch:
		log.Info("connecting to Elasticsearch...")
		db, err = elasticsearch.NewElasticsearchDB(conf)
	default:
		err = fmt.Errorf("unrecognized database '%s' from config", conf.DB)
	}
	if err != nil {
		log.Error(err, "failed to initialize database")
		os.Exit(1)
	}

	if conf.DropTableOnStartup != "" && !conf.IsProduction {
		dropTableAndSchema(db, conf.DropTableOnStartup)
	}

	if err := db.CreateStoredSchemasTable(context.Background()); err != nil {
		log.Error(err, "failed to create table for storing schemas")
		os.Exit(1)
	}

	analysisAPI := api.NewAnalysisAPI(db, http.DefaultServeMux, conf)

	log.Infof("listening on port %s...", conf.API.Port)
	if err := analysisAPI.ListenAndServe(); err != nil {
		log.Error(err, "server stopped")
		os.Exit(1)
	}
}

func dropTableAndSchema(db db.AnalysisDB, table string) {
	ctx := context.Background()

	alreadyDropped, err := db.DropTable(ctx, table)
	if err != nil {
		log.Errorf(
			err,
			"failed to drop table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)",
			table,
		)
		return
	}

	if !alreadyDropped {
		log.Infof("dropped table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)", table)

		if err := db.DeleteTableSchema(ctx, table); err != nil {
			log.Errorf(err, "failed to delete schema for dropped table '%s'", table)
		}
	}
}
