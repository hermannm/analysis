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

	if err := clickhouse.conn.Exec(ctx, query.String()); err != nil {
		return wrap.Error(err, "ClickHouse table creation query failed")
	}

	return nil
}

func (clickhouse ClickHouseDB) StoreTableSchema(
	ctx context.Context,
	schema db.TableSchema,
) error {
	if err := schema.Validate(); err != nil {
		return wrap.Error(err, "invalid table schema")
	}

	var query QueryBuilder
	query.WriteString("INSERT INTO ")
	query.WriteIdentifier(db.StoredSchemasTable)
	query.WriteString(" VALUES (?, ?, ?, ?)")

	storedSchema := schema.ToStored()

	shouldWaitForResult := true
	if err := clickhouse.conn.AsyncInsert(
		ctx,
		query.String(),
		shouldWaitForResult,
		storedSchema.TableName,
		storedSchema.ColumnNames,
		storedSchema.DataTypes,
		storedSchema.Optionals,
	); err != nil {
		return wrap.Error(err, "ClickHouse schema insertion query failed")
	}

	return nil
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
	query.WriteIdentifier(db.StoredSchemaName)
	query.WriteString(", ")
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
		return db.TableSchema{}, wrap.Error(err, "ClickHouse schema fetching query failed")
	}

	var storedSchema db.StoredTableSchema
	if err := result.Scan(
		&storedSchema.TableName,
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
		return wrap.Error(err, "ClickHouse schema deletion query failed")
	}

	return nil
}
