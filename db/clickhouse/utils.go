package clickhouse

import (
	"context"
	"fmt"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func columnTypeToClickHouse(columnType db.DataType) (string, error) {
	// See https://clickhouse.com/docs/en/sql-reference/data-types
	switch columnType {
	case db.DataTypeInt:
		return "Int64", nil
	case db.DataTypeFloat:
		return "Float64", nil
	case db.DataTypeTimestamp:
		return "DateTime64(3)", nil
	case db.DataTypeUUID:
		return "UUID", nil
	case db.DataTypeText:
		return "String", nil
	}

	return "", fmt.Errorf("unrecognized column type '%s'", columnType)
}

func writeIdentifier(writer *strings.Builder, identifier string) error {
	if !strings.ContainsRune(identifier, '`') {
		writer.WriteRune('`')
		writer.WriteString(identifier)
		writer.WriteRune('`')
		return nil
	}

	if !strings.ContainsRune(identifier, '"') {
		writer.WriteRune('"')
		writer.WriteString(identifier)
		writer.WriteRune('"')
		return nil
	}

	return fmt.Errorf(
		"'%s' contains both \" and `, which is incompatible with database",
		identifier,
	)
}

func (db ClickHouseDB) dropTable(
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
