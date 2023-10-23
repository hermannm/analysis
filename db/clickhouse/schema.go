package clickhouse

import (
	"context"

	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) CreateStoredSchemasTable(ctx context.Context) error {
	var query QueryBuilder
	query.WriteString("CREATE TABLE IF NOT EXISTS ")
	query.WriteIdentifier(db.StoredSchemasTable)
	query.WriteString(" (")

	query.WriteIdentifier(db.StoredSchemaName)
	query.WriteString(" String, ")

	query.WriteIdentifier(db.StoredSchemaColumnNames)
	query.WriteString(" Array(String), ")

	query.WriteIdentifier(db.StoredSchemaColumnDataTypes)
	query.WriteString(" Array(Int8), ")

	query.WriteIdentifier(db.StoredSchemaColumnOptionals)
	query.WriteString(" Array(Bool))")

	query.WriteString(" ENGINE = MergeTree()")
	query.WriteString(" PRIMARY KEY (")
	query.WriteIdentifier(db.StoredSchemaName)
	query.WriteByte(')')

	return clickhouse.conn.Exec(ctx, query.String())
}

func (clickhouse ClickHouseDB) StoreTableSchema(
	ctx context.Context,
	table string,
	schema db.TableSchema,
) error {
	if errs := schema.Validate(); len(errs) != 0 {
		return wrap.Errors("invalid schema", errs...)
	}

	var query QueryBuilder
	query.WriteString("INSERT INTO ")
	query.WriteIdentifier(db.StoredSchemasTable)
	query.WriteString(" VALUES (?, ?, ?, ?)")

	storedSchema := schema.ToStored()

	shouldWaitForResult := true
	return clickhouse.conn.AsyncInsert(
		ctx,
		query.String(),
		shouldWaitForResult,
		table,
		storedSchema.ColumnNames,
		storedSchema.DataTypes,
		storedSchema.Optionals,
	)
}

func (clickhouse ClickHouseDB) GetTableSchema(
	ctx context.Context,
	table string,
) (db.TableSchema, error) {
	if err := ValidateIdentifier(table); err != nil {
		return db.TableSchema{}, wrap.Error(err, "invalid table name")
	}

	var query QueryBuilder
	query.WriteString("SELECT ")
	query.WriteIdentifier(db.StoredSchemaColumnNames)
	query.WriteString(", ")
	query.WriteIdentifier(db.StoredSchemaColumnDataTypes)
	query.WriteString(", ")
	query.WriteIdentifier(db.StoredSchemaColumnOptionals)
	query.WriteString(" FROM ")
	query.WriteIdentifier(db.StoredSchemasTable)
	query.WriteString(" WHERE (")
	query.WriteIdentifier(db.StoredSchemaName)
	query.WriteString(" = ?)")

	result := clickhouse.conn.QueryRow(ctx, query.String(), table)
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

func (clickhouse ClickHouseDB) DeleteTableSchema(
	ctx context.Context,
	table string,
) error {
	var query QueryBuilder
	query.WriteString("DELETE FROM ")
	query.WriteIdentifier(db.StoredSchemasTable)
	query.WriteString(" WHERE (")
	query.WriteIdentifier(db.StoredSchemaName)
	query.WriteString(" = ?)")

	if err := clickhouse.conn.Exec(ctx, query.String(), table); err != nil {
		return wrap.Error(err, "delete table schema query failed")
	}

	return nil
}
