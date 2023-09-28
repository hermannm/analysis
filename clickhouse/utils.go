package clickhouse

import (
	"context"
	"fmt"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"hermannm.dev/analysis/datatypes"
	"hermannm.dev/wrap"
)

func translateDataTypeToClickHouse(dataType datatypes.DataType) (string, error) {
	// See https://clickhouse.com/docs/en/sql-reference/data-types
	switch dataType {
	case datatypes.DataTypeInt:
		return "Int64", nil
	case datatypes.DataTypeFloat:
		return "Float64", nil
	case datatypes.DataTypeTimestamp:
		return "DateTime64(3)", nil
	case datatypes.DataTypeUUID:
		return "UUID", nil
	case datatypes.DataTypeText:
		return "String", nil
	}

	return "", fmt.Errorf("unrecognized data type '%s'", dataType)
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
