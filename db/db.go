package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/analysis/column"
	"hermannm.dev/analysis/csv"
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

func (db AnalysisDatabase) CreateTableSchema(
	ctx context.Context, tableName string, columns []column.Column,
) error {
	var query strings.Builder

	query.WriteString("CREATE TABLE ")
	if err := writeIdentifier(&query, tableName); err != nil {
		return wrap.Error(err, "invalid table name")
	}
	query.WriteString(" (`id` UUID, ")

	for i, column := range columns {
		dataType, err := columnTypeToClickHouse(column.DataType)
		if err != nil {
			return wrap.Errorf(
				err, "failed to get ClickHouse data type for column '%s'", column.Name,
			)
		}

		if err := writeIdentifier(&query, column.Name); err != nil {
			return wrap.Error(err, "invalid column name")
		}
		query.WriteRune(' ')
		query.WriteString(dataType)

		if column.Optional {
			query.WriteString(" NULL")
		}

		if i != len(columns)-1 {
			query.WriteString(", ")
		}
	}
	query.WriteRune(')')
	query.WriteString(" ENGINE = MergeTree()")
	query.WriteString(" PRIMARY KEY (id)")

	fmt.Println(query.String())

	if err := db.conn.Exec(ctx, query.String()); err != nil {
		return wrap.Error(err, "create table query failed")
	}

	return nil
}

func (db AnalysisDatabase) UpdateTableWithCSV(
	ctx context.Context, table string, csvReader *csv.Reader,
) error {
	// Skips header row, as we are only interested in data fields here
	if _, err := csvReader.ReadHeaderRow(); err != nil {
		return wrap.Error(err, "failed to skip CSV header row")
	}

	return nil
}
