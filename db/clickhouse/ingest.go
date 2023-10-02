package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) CreateTable(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	var builder strings.Builder

	builder.WriteString("CREATE TABLE ")
	if err := writeIdentifier(&builder, table); err != nil {
		return wrap.Error(err, "invalid table name")
	}
	builder.WriteString(" (`id` UUID, ")

	for i, column := range schema.Columns {
		if err := writeIdentifier(&builder, column.Name); err != nil {
			return wrap.Error(err, "invalid column name")
		}
		builder.WriteRune(' ')

		dataType, ok := clickhouseDataTypes.GetName(column.DataType)
		if !ok {
			return fmt.Errorf("invalid data type '%v' in column '%s'", column.DataType, column.Name)
		}
		builder.WriteString(dataType)

		if column.Optional {
			builder.WriteString(" NULL")
		}

		if i != len(schema.Columns)-1 {
			builder.WriteString(", ")
		}
	}
	builder.WriteRune(')')
	builder.WriteString(" ENGINE = MergeTree()")
	builder.WriteString(" PRIMARY KEY (id)")

	if err := clickhouse.conn.Exec(ctx, builder.String()); err != nil {
		return wrap.Error(err, "create table query failed")
	}

	return nil
}

// ClickHouse recommends keeping batch inserts between 10,000 and 100,000 rows:
// https://clickhouse.com/docs/en/cloud/bestpractices/bulk-inserts
const BatchInsertSize = 10000

func (clickhouse ClickHouseDB) UpdateTableData(
	ctx context.Context,
	table string,
	schema db.TableSchema,
	data db.DataSource,
) error {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	if err := writeIdentifier(&builder, table); err != nil {
		return wrap.Error(err, "invalid table name")
	}
	queryString := builder.String()

	fieldsPerRow := len(schema.Columns) + 1 // +1 for id field

	allRowsSent := false
	for !allRowsSent {
		batch, err := clickhouse.conn.PrepareBatch(ctx, queryString)
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
					"failed to convert row %d to data types expected by table schema",
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
