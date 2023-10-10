package clickhouse

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) CreateTable(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	if err := ValidateIdentifier(table); err != nil {
		return wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("CREATE TABLE ")
	builder.WriteIdentifier(table)
	builder.WriteString(" (`id` UUID, ")

	for i, column := range schema.Columns {
		if err := ValidateIdentifier(column.Name); err != nil {
			return wrap.Error(err, "invalid column name")
		}
		builder.WriteIdentifier(column.Name)
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

	if err := clickhouse.storeTableSchema(ctx, table, schema); err != nil {
		_, dropErr := clickhouse.dropTable(ctx, table)
		if dropErr == nil {
			return wrap.Error(err, "failed to store table schema")
		} else {
			return wrap.Errors(
				"failed to store table schema AND failed to clean up invalid created table afterwards",
				err,
				dropErr,
			)
		}
	}

	return nil
}

func (clickhouse ClickHouseDB) storeTableSchema(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	if errs := schema.Validate(); len(errs) != 0 {
		return wrap.Errors("invalid schema", errs...)
	}

	var builder QueryBuilder
	builder.WriteString("INSERT INTO ")
	builder.WriteIdentifier(schemasTable)
	builder.WriteString(" VALUES (?, ?)")

	formattedColumns := make([]string, len(schema.Columns))
	for i, column := range schema.Columns {
		formattedColumns[i] = fmt.Sprintf(
			"('%s', %d, %t)",
			replaceStringQuotes(column.Name),
			column.DataType,
			column.Optional,
		)
	}

	return clickhouse.conn.AsyncInsert(ctx, builder.String(), true, table, formattedColumns)
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
	if err := ValidateIdentifier(table); err != nil {
		return wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("INSERT INTO ")
	builder.WriteIdentifier(table)
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
