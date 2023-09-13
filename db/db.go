package db

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/wrap"
)

type AnalysisDatabase struct {
	clickhouse driver.Conn
}

type ClickHouseConfig struct {
	Address  string
	Database string
	Username string
	Password string
}

func NewAnalysisDatabase(config ClickHouseConfig) (AnalysisDatabase, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{config.Address},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Debugf: func(format string, v ...any) {
			fmt.Printf(format, v...)
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true, // Must be changed before any potential prod deployment
		},
	})
	if err != nil {
		return AnalysisDatabase{}, wrap.Error(err, "failed to connect to ClickHouse")
	}

	if err := conn.Ping(context.Background()); err != nil {
		return AnalysisDatabase{}, wrap.Error(err, "failed to ping ClickHouse connection")
	}

	return AnalysisDatabase{clickhouse: conn}, nil
}
