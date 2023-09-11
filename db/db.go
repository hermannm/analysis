package db

import (
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

func NewAnalysisDatabase(env ClickHouseConfig) (AnalysisDatabase, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{env.Address},
		Auth: clickhouse.Auth{
			Database: env.Database,
			Username: env.Username,
			Password: env.Password,
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

	return AnalysisDatabase{clickhouse: conn}, nil
}
