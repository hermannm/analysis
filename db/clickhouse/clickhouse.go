package clickhouse

import (
	"context"
	"fmt"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/log"
	"hermannm.dev/wrap"
)

// Implements db.AnalysisDB for ClickHouse
type ClickHouseDB struct {
	conn clickhousedriver.Conn
}

func NewClickHouseDB(config config.Config) (ClickHouseDB, error) {
	// Options docs: https://clickhouse.com/docs/en/integrations/go#connection-settings
	conn, err := clickhousego.Open(&clickhousego.Options{
		Addr: []string{config.ClickHouse.Address},
		Auth: clickhousego.Auth{
			Database: config.ClickHouse.DatabaseName,
			Username: config.ClickHouse.Username,
			Password: config.ClickHouse.Password,
		},
		Debug: config.ClickHouse.Debug,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
		Compression: &clickhousego.Compression{Method: clickhousego.CompressionLZ4},
	})
	if err != nil {
		return ClickHouseDB{}, wrap.Error(err, "failed to connect to ClickHouse")
	}

	if err := conn.Ping(context.Background()); err != nil {
		return ClickHouseDB{}, wrap.Error(err, "failed to ping ClickHouse connection")
	}

	clickhouse := ClickHouseDB{conn: conn}

	tableToDrop := config.ClickHouse.DropTableOnStartup
	if tableToDrop != "" && !config.IsProduction {
		tableAlreadyDropped, err := clickhouse.dropTable(context.Background(), tableToDrop)
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

	return clickhouse, nil
}

func (clickhouse ClickHouseDB) dropTable(
	ctx context.Context,
	table string,
) (tableAlreadyDropped bool, err error) {
	if err := ValidateIdentifier(table); err != nil {
		return false, wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("DROP TABLE ")
	builder.WriteIdentifier(table)

	// See https://github.com/ClickHouse/ClickHouse/blob/bd387f6d2c30f67f2822244c0648f2169adab4d3/src/Common/ErrorCodes.cpp#L66
	const clickhouseUnknownTableErrorCode = 60

	if err := clickhouse.conn.Exec(ctx, builder.String()); err != nil {
		clickHouseErr, isClickHouseErr := err.(*clickhouseproto.Exception)
		if isClickHouseErr && clickHouseErr.Code == clickhouseUnknownTableErrorCode {
			return true, nil
		}

		return false, wrap.Error(err, "drop table query failed")
	}

	return false, nil
}