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

	var query QueryBuilder
	query.WriteString("CREATE TABLE ")
	query.WriteIdentifier(table)
	query.WriteString(" (`id` UUID, ")

	for i, column := range schema.Columns {
		if err := ValidateIdentifier(column.Name); err != nil {
			return wrap.Error(err, "invalid column name")
		}
		query.WriteIdentifier(column.Name)
		query.WriteByte(' ')

		dataType, ok := clickhouseDataTypes.GetName(column.DataType)
		if !ok {
			return fmt.Errorf("invalid data type '%v' in column '%s'", column.DataType, column.Name)
		}
		query.WriteString(dataType)

		if column.Optional {
			query.WriteString(" NULL")
		}

		if i != len(schema.Columns)-1 {
			query.WriteString(", ")
		}
	}

	query.WriteByte(')')
	query.WriteString(" ENGINE = MergeTree()")
	query.WriteString(" PRIMARY KEY (id)")

	if err := clickhouse.conn.Exec(ctx, query.String()); err != nil {
		return wrap.Errorf(err, "ClickHouse table creation query failed for table '%s'", table)
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
	if err := ValidateIdentifier(table); err != nil {
		return wrap.Error(err, "invalid table name")
	}

	var query QueryBuilder
	query.WriteString("INSERT INTO ")
	query.WriteIdentifier(table)
	queryString := query.String()

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
