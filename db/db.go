package db

import (
	"context"
	"fmt"
	"io"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/wrap"
)

type AnalysisDatabase struct {
	conn driver.Conn
}

type ClickHouseConfig struct {
	Address      string
	DatabaseName string
	Username     string
	Password     string
	Debug        bool
}

func NewAnalysisDatabase(config ClickHouseConfig) (AnalysisDatabase, error) {
	// Options docs: https://clickhouse.com/docs/en/integrations/go#connection-settings
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address},
		Auth: clickhouse.Auth{
			Database: config.DatabaseName,
			Username: config.Username,
			Password: config.Password,
		},
		Debug: config.Debug,
		Debugf: func(format string, v ...any) {
			fmt.Printf(format+"\n", v...)
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
	if err != nil {
		return AnalysisDatabase{}, wrap.Error(err, "failed to connect to ClickHouse")
	}

	if err := conn.Ping(context.Background()); err != nil {
		return AnalysisDatabase{}, wrap.Error(err, "failed to ping ClickHouse connection")
	}

	return AnalysisDatabase{conn: conn}, nil
}

func (db AnalysisDatabase) CreateTableSchemaFromCSV(
	ctx context.Context, table string, file io.Reader,
) error {
	return nil
}

func (db AnalysisDatabase) UpdateTableDataWithCSV(
	ctx context.Context, table string, file io.Reader,
) error {
	return nil
}