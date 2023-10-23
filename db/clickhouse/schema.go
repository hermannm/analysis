package clickhouse

import (
	"context"

	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) createSchemaTable(ctx context.Context) error {
	var builder QueryBuilder
	builder.WriteString("CREATE TABLE IF NOT EXISTS ")
	builder.WriteIdentifier(db.StoredSchemasTable)
	builder.WriteString(" (")

	builder.WriteIdentifier(db.StoredSchemaColumnNames)
	builder.WriteString(" String, ")

	builder.WriteIdentifier(db.StoredSchemaColumnNames)
	builder.WriteString(" Array(String), ")

	builder.WriteIdentifier(db.StoredSchemaColumnDataTypes)
	builder.WriteString(" Array(UInt8), ")

	builder.WriteIdentifier(db.StoredSchemaColumnOptionals)
	builder.WriteString(" Array(Bool))")

	builder.WriteString(" ENGINE = MergeTree()")
	builder.WriteString(" PRIMARY KEY (name)")

	return clickhouse.conn.Exec(ctx, builder.String())
}

func (clickhouse ClickHouseDB) GetTableSchema(
	ctx context.Context,
	table string,
) (db.TableSchema, error) {
	if err := ValidateIdentifier(table); err != nil {
		return db.TableSchema{}, wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("SELECT ")
	builder.WriteIdentifier(db.StoredSchemaColumnNames)
	builder.WriteString(", ")
	builder.WriteIdentifier(db.StoredSchemaColumnDataTypes)
	builder.WriteString(", ")
	builder.WriteIdentifier(db.StoredSchemaColumnOptionals)
	builder.WriteString(" FROM ")
	builder.WriteIdentifier(db.StoredSchemasTable)
	builder.WriteString(" WHERE (")
	builder.WriteIdentifier(db.StoredSchemaName)
	builder.WriteString(" = ?)")

	result := clickhouse.conn.QueryRow(ctx, builder.String(), table)
	if err := result.Err(); err != nil {
		return db.TableSchema{}, wrap.Error(err, "table schema query failed")
	}

	var storedSchema db.StoredTableSchema
	if err := result.Scan(
		&storedSchema.ColumnNames,
		&storedSchema.DataTypes,
		&storedSchema.Optionals,
	); err != nil {
		return db.TableSchema{}, wrap.Error(err, "failed to parse table schema from database")
	}

	schema, err := storedSchema.ToSchema()
	if err != nil {
		return db.TableSchema{}, wrap.Error(err, "failed to parse stored table schema")
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
	builder.WriteIdentifier(db.StoredSchemasTable)
	builder.WriteString(" VALUES (?, ?, ?, ?)")

	storedSchema := schema.ToStored()

	shouldWaitForResult := true
	return clickhouse.conn.AsyncInsert(
		ctx,
		builder.String(),
		shouldWaitForResult,
		table,
		storedSchema.ColumnNames,
		storedSchema.DataTypes,
		storedSchema.Optionals,
	)
}
