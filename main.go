package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"hermannm.dev/analysis/api"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/db"
	"hermannm.dev/analysis/db/clickhouse"
	"hermannm.dev/analysis/db/elasticsearch"
	"hermannm.dev/devlog"
	"hermannm.dev/devlog/log"
	"hermannm.dev/wrap"
)

func main() {
	logger := slog.New(devlog.NewHandler(os.Stdout, &devlog.Options{Level: slog.LevelDebug}))
	slog.SetDefault(logger)

	log.Info("loading environment variables...")
	conf, err := config.ReadFromEnv()
	if err != nil {
		log.Error(err, "failed to read config from env")
		os.Exit(1)
	}

	db, err := initializeDatabase(conf)
	if err != nil {
		log.Error(err, "failed to initialize database")
		os.Exit(1)
	}

	api := api.NewAnalysisAPI(db, http.DefaultServeMux, conf)

	log.Infof("listening on port %s...", conf.API.Port)
	if err := api.ListenAndServe(); err != nil {
		log.Error(err, "server stopped")
		os.Exit(1)
	}
}

func initializeDatabase(conf config.Config) (db.AnalysisDB, error) {
	var db db.AnalysisDB
	var err error

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
		return nil, err
	}

	if conf.DropTableOnStartup != "" && !conf.IsProduction {
		dropTableAndSchema(db, conf.DropTableOnStartup)
	}

	if err := db.CreateStoredSchemasTable(context.Background()); err != nil {
		return nil, wrap.Error(err, "failed to create table for storing schemas")
	}

	return db, nil
}

func dropTableAndSchema(db db.AnalysisDB, table string) {
	ctx := context.Background()

	alreadyDropped, err := db.DropTable(ctx, table)
	if err != nil {
		log.WarnErrorf(
			err,
			"failed to drop table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)",
			table,
		)
		return
	}

	if !alreadyDropped {
		log.Infof("dropped table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)", table)

		if err := db.DeleteTableSchema(ctx, table); err != nil {
			log.WarnErrorf(err, "failed to delete schema for dropped table '%s'", table)
		}
	}
}
