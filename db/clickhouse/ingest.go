package clickhouse

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (db ClickHouseDB) CreateTableSchema(
	ctx context.Context,
	table string,
	schema db.Schema,
) error {
	var query strings.Builder

	query.WriteString("CREATE TABLE ")
	if err := writeIdentifier(&query, table); err != nil {
		return wrap.Error(err, "invalid table name")
	}
	query.WriteString(" (`id` UUID, ")

	for i, column := range schema.Columns {
		dataType, err := translateDataTypeToClickHouse(column.DataType)
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

func (db ClickHouseDB) UpdateTableData(
	ctx context.Context,
	table string,
	schema db.Schema,
	data db.DataSource,
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
