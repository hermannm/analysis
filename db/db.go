package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"hermannm.dev/analysis/datatypes"
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
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
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
	ctx context.Context,
	tableName string,
	schema datatypes.Schema,
) error {
	var query strings.Builder

	query.WriteString("CREATE TABLE ")
	if err := writeIdentifier(&query, tableName); err != nil {
		return wrap.Error(err, "invalid table name")
	}
	query.WriteString(" (`id` UUID, ")

	for i, column := range schema.Columns {
		dataType, err := columnTypeToClickHouse(column.DataType)
		if err != nil {
			return wrap.Errorf(
				err,
				"failed to get ClickHouse data type for column '%s'",
				column.Name,
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

		if i != len(schema.Columns)-1 {
			query.WriteString(", ")
		}
	}
	query.WriteRune(')')
	query.WriteString(" ENGINE = MergeTree()")
	query.WriteString(" PRIMARY KEY (id)")

	if err := db.conn.Exec(ctx, query.String()); err != nil {
		return wrap.Error(err, "create table query failed")
	}

	return nil
}

const BatchInsertSize = 1000

type DataSource interface {
	ReadRow() (row []string, rowNumber int, done bool, err error)
}

func (db AnalysisDatabase) UpdateTableData(
	ctx context.Context,
	tableName string,
	schema datatypes.Schema,
	data DataSource,
) error {
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	if err := writeIdentifier(&query, tableName); err != nil {
		return wrap.Error(err, "invalid table name")
	}
	queryString := query.String()

	fieldsPerRow := len(schema.Columns) + 1 // +1 for id field

	allRowsSent := false
	for !allRowsSent {
		batch, err := db.conn.PrepareBatch(ctx, queryString)
		if err != nil {
			return wrap.Error(err, "failed to prepare batch data insert")
		}

		for i := 0; i < BatchInsertSize; i++ {
			rawRow, rowNumber, done, err := data.ReadRow()
			if done {
				allRowsSent = true
				break
			}
			if err != nil {
				return wrap.Error(err, "failed to read row")
			}

			convertedRow := make([]any, 0, fieldsPerRow)

			id, err := uuid.NewUUID()
			if err != nil {
				return wrap.Errorf(err, "failed to generate unique ID for row %d", rowNumber)
			}
			convertedRow = append(convertedRow, id.String())

			convertedRow, err = schema.ConvertAndAppendRow(convertedRow, rawRow)
			if err != nil {
				return wrap.Errorf(
					err,
					"failed to convert row %d to data types expected by schema",
					rowNumber,
				)
			}

			if err := batch.Append(convertedRow...); err != nil {
				return wrap.Errorf(err, "failed to add row %d to batch insert", rowNumber)
			}
		}

		if err := batch.Send(); err != nil {
			return wrap.Error(err, "failed to send batch insert")
		}
	}

	return nil
}
