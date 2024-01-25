package clickhouse

import (
	"context"

	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) CreateStoredSchemasTable(ctx context.Context) error {
	var query QueryBuilder
	query.WriteString("CREATE TABLE IF NOT EXISTS ")

	// Ignores errors on these identifiers, as we know they are valid
	query.WriteQuotedIdentifier(db.StoredSchemasTable)
	query.WriteString(" (")

	query.WriteQuotedIdentifier(db.StoredSchemaName)
	query.WriteString(" String, ")

	query.WriteQuotedIdentifier(db.StoredSchemaColumnNames)
	query.WriteString(" Array(String), ")

	query.WriteQuotedIdentifier(db.StoredSchemaColumnDataTypes)
	query.WriteString(" Array(Int8), ")

	query.WriteQuotedIdentifier(db.StoredSchemaColumnOptionals)
	query.WriteString(" Array(Bool))")

	query.WriteString(" ENGINE = MergeTree()")
	query.WriteString(" PRIMARY KEY (")
	query.WriteQuotedIdentifier(db.StoredSchemaName)
	query.WriteByte(')')

	if err := clickhouse.conn.Exec(query.WithParameters(ctx), query.String()); err != nil {
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

	storedSchema := schema.ToStored()

	var query QueryBuilder
	query.WriteString("INSERT INTO ")
	// Ignores error, as this is a safe internal identifier
	query.WriteQuotedIdentifier(db.StoredSchemasTable)
	query.WriteString(" VALUES (?, ?, ?, ?)")

	if err := clickhouse.conn.Exec(
		query.WithParameters(ctx),
		query.String(),
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
	var query QueryBuilder
	query.WriteString("SELECT ")
	query.AddIdentifier(db.StoredSchemaName)
	query.WriteString(", ")
	query.AddIdentifier(db.StoredSchemaColumnNames)
	query.WriteString(", ")
	query.AddIdentifier(db.StoredSchemaColumnDataTypes)
	query.WriteString(", ")
	query.AddIdentifier(db.StoredSchemaColumnOptionals)
	query.WriteString(" FROM ")
	query.AddIdentifier(db.StoredSchemasTable)
	query.WriteString(" WHERE (")
	query.AddIdentifier(db.StoredSchemaName)
	query.WriteString(" = ")
	query.AddStringParameter(table)
	query.WriteByte(')')

	result := clickhouse.conn.QueryRow(query.WithParameters(ctx), query.String())
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
	query.AddIdentifier(db.StoredSchemasTable)
	query.WriteString(" WHERE (")
	query.AddIdentifier(db.StoredSchemaName)
	query.WriteString(" = ")
	query.AddStringParameter(table)
	query.WriteByte(')')

	if err := clickhouse.conn.Exec(query.WithParameters(ctx), query.String()); err != nil {
		return wrap.Error(err, "ClickHouse schema deletion query failed")
	}

	return nil
}
