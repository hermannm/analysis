package db

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/google/uuid"
	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/datatypes"
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
			log.Println(
				wrap.Errorf(
					err,
					"failed to drop table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)",
					tableToDrop,
				),
			)
		} else if !tableAlreadyDropped {
			log.Printf("Dropped table '%s' (from DEBUG_DROP_TABLE_ON_STARTUP in env)", tableToDrop)
		}
	}

	return db, nil
}

func (db AnalysisDatabase) CreateTableSchema(
	ctx context.Context,
	table string,
	schema datatypes.Schema,
) error {
	var query strings.Builder

	query.WriteString("CREATE TABLE ")
	if err := writeIdentifier(&query, table); err != nil {
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

// ClickHouse recommends keeping batch inserts between 10,000 and 100,000 rows:
// https://clickhouse.com/docs/en/cloud/bestpractices/bulk-inserts
const BatchInsertSize = 10000

type DataSource interface {
	ReadRow() (row []string, rowNumber int, done bool, err error)
}

func (db AnalysisDatabase) UpdateTableData(
	ctx context.Context,
	table string,
	schema datatypes.Schema,
	data DataSource,
) error {
	var query strings.Builder
	query.WriteString("INSERT INTO ")
	if err := writeIdentifier(&query, table); err != nil {
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

type Aggregate struct {
	Column string `ch:"analysis_group_column" json:"column"`
	Sum    int64  `ch:"analysis_aggregate"    json:"sum"`
}

func (db AnalysisDatabase) Aggregate(
	ctx context.Context,
	tableName string,
	groupColumn string,
	aggregationColumn string,
	limit int,
) (aggregates []Aggregate, err error) {
	var query strings.Builder
	query.WriteString("SELECT ")

	if err := writeIdentifier(&query, groupColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for group-by clause")
	}
	query.WriteString(" AS analysis_group_column, ")

	query.WriteString("SUM(")
	if err := writeIdentifier(&query, aggregationColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for aggregate-by clause")
	}
	query.WriteString(") AS analysis_aggregate ")

	query.WriteString("FROM ")
	if err := writeIdentifier(&query, tableName); err != nil {
		return nil, wrap.Error(err, "invalid table name")
	}

	query.WriteString(" GROUP BY ")
	if err := writeIdentifier(&query, groupColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for group-by clause")
	}

	query.WriteString(" ORDER BY analysis_aggregate DESC ")
	query.WriteString("LIMIT ")
	query.WriteString(strconv.Itoa(limit))

	if err := db.conn.Select(ctx, &aggregates, query.String()); err != nil {
		return nil, wrap.Error(err, "aggregation query failed")
	}

	return aggregates, nil
}

func (db AnalysisDatabase) dropTable(
	ctx context.Context,
	tableName string,
) (tableAlreadyDropped bool, err error) {
	var query strings.Builder
	query.WriteString("DROP TABLE ")
	if err := writeIdentifier(&query, tableName); err != nil {
		return false, wrap.Error(err, "invalid table name")
	}

	// See https://github.com/ClickHouse/ClickHouse/blob/bd387f6d2c30f67f2822244c0648f2169adab4d3/src/Common/ErrorCodes.cpp#L66
	const clickhouseUnknownTableErrorCode = 60

	if err := db.conn.Exec(ctx, query.String()); err != nil {
		clickHouseErr, isClickHouseErr := err.(*clickhouseproto.Exception)
		if isClickHouseErr && clickHouseErr.Code == clickhouseUnknownTableErrorCode {
			return true, nil
		}

		return false, wrap.Error(err, "drop table query failed")
	}

	return false, nil
}
