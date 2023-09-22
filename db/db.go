package db

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/log"
	"hermannm.dev/wrap"
)

type AnalysisDatabase struct {
	conn driver.Conn
}

func NewAnalysisDatabase(config config.Config) (AnalysisDatabase, error) {
	// Options docs: https://clickhouse.com/docs/en/integrations/go#connection-settings
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.ClickHouse.Address},
		Auth: clickhouse.Auth{
			Database: config.ClickHouse.DatabaseName,
			Username: config.ClickHouse.Username,
			Password: config.ClickHouse.Password,
		},
		Debug: config.ClickHouse.Debug,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
	})
	if err != nil {
		return AnalysisDatabase{}, wrap.Error(err, "failed to connect to ClickHouse")
	}

	if err := conn.Ping(context.Background()); err != nil {
		return AnalysisDatabase{}, wrap.Error(err, "failed to ping ClickHouse connection")
	}

	db := AnalysisDatabase{conn: conn}

	tableToDrop := config.ClickHouse.DropTableOnStartup
	if tableToDrop != "" && !config.IsProduction {
		tableAlreadyDropped, err := db.dropTable(context.Background(), tableToDrop)
		if err != nil {
			log.Errorf(
				err,
				"failed to drop table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)",
				tableToDrop,
			)
		} else if !tableAlreadyDropped {
			log.Infof("Dropped table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)", tableToDrop)
		}
	}

	return db, nil
}
