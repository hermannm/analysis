package clickhouse

import (
	"context"

	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

const (
	schemaTable           = "analysis_schemas"
	schemaName            = "name"
	schemaColumnNames     = "column_names"
	schemaColumnDataTypes = "column_data_types"
	schemaColumnOptionals = "column_optionals"
)

func (clickhouse ClickHouseDB) createSchemaTable(ctx context.Context) error {
	var builder QueryBuilder
	builder.WriteString("CREATE TABLE IF NOT EXISTS ")
	builder.WriteIdentifier(schemaTable)
	builder.WriteString(" (")

	builder.WriteIdentifier(schemaName)
	builder.WriteString(" String, ")

	builder.WriteIdentifier(schemaColumnNames)
	builder.WriteString(" Array(String), ")

	builder.WriteIdentifier(schemaColumnDataTypes)
	builder.WriteString(" Array(UInt8), ")

	builder.WriteIdentifier(schemaColumnOptionals)
	builder.WriteString(" Array(Bool))")

	builder.WriteString(" ENGINE = MergeTree()")
	builder.WriteString(" PRIMARY KEY (name)")

	return clickhouse.conn.Exec(ctx, builder.String())
}

func (clickhouse ClickHouseDB) GetTableSchema(
	ctx context.Context,
	table string,
) (schema db.TableSchema, err error) {
	if err = ValidateIdentifier(table); err != nil {
		return db.TableSchema{}, wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("SELECT ")
	builder.WriteIdentifier(schemaColumnNames)
	builder.WriteString(", ")
	builder.WriteIdentifier(schemaColumnDataTypes)
	builder.WriteString(", ")
	builder.WriteIdentifier(schemaColumnOptionals)
	builder.WriteString(" FROM ")
	builder.WriteIdentifier(schemaTable)
	builder.WriteString(" WHERE (")
	builder.WriteIdentifier(schemaName)
	builder.WriteString(" = ?)")

	result := clickhouse.conn.QueryRow(ctx, builder.String(), table)
	if err := result.Err(); err != nil {
		return db.TableSchema{}, wrap.Error(err, "table schema query failed")
	}

	names, dataTypes, optionals := makeSchemaColumnArrays(0)
	if err := result.Scan(&names, &dataTypes, &optionals); err != nil {
		return db.TableSchema{}, wrap.Error(err, "failed to parse table schema from database")
	}
	if len(names) != len(dataTypes) || len(names) != len(optionals) {
		return db.TableSchema{}, wrap.Error(err, "database result arrays had inconsistent lengths")
	}

	schema = db.TableSchema{Columns: make([]db.Column, len(names))}
	for i := range names {
		schema.Columns[i] = db.Column{
			Name:     names[i],
			DataType: db.DataType(dataTypes[i]),
			Optional: optionals[i],
		}
	}
	if errs := schema.Validate(); len(errs) != 0 {
		return db.TableSchema{}, wrap.Errors("database returned invalid table schema", errs...)
	}

	return schema, nil
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
	builder.WriteIdentifier(schemaTable)
	builder.WriteString(" VALUES (?, ?, ?, ?)")

	names, dataTypes, optionals := makeSchemaColumnArrays(len(schema.Columns))
	for i, column := range schema.Columns {
		names[i] = column.Name
		dataTypes[i] = uint8(column.DataType)
		optionals[i] = column.Optional
	}

	shouldWaitForResult := true
	return clickhouse.conn.AsyncInsert(
		ctx,
		builder.String(),
		shouldWaitForResult,
		table,
		names,
		dataTypes,
		optionals,
	)
}

func makeSchemaColumnArrays(
	length int,
) (columnNames []string, dataTypes []uint8, optionals []bool) {
	if length == 0 {
		return nil, nil, nil
	} else {
		return make([]string, length), make([]uint8, length), make([]bool, length)
	}
}
