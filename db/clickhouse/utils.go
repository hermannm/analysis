package clickhouse

import (
	"context"
	"fmt"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"hermannm.dev/wrap"
)

func writeIdentifier(builder *strings.Builder, identifier string) error {
	if !strings.ContainsRune(identifier, '`') {
		builder.WriteRune('`')
		builder.WriteString(identifier)
		builder.WriteRune('`')
		return nil
	}

	if !strings.ContainsRune(identifier, '"') {
		builder.WriteRune('"')
		builder.WriteString(identifier)
		builder.WriteRune('"')
		return nil
	}

	return fmt.Errorf(
		"'%s' contains both \" and `, which is incompatible with database",
		identifier,
	)
}

func (clickhouse ClickHouseDB) dropTable(
	ctx context.Context,
	tableName string,
) (tableAlreadyDropped bool, err error) {
	var builder strings.Builder
	builder.WriteString("DROP TABLE ")
	if err := writeIdentifier(&builder, tableName); err != nil {
		return false, wrap.Error(err, "invalid table name")
	}

	// See https://github.com/ClickHouse/ClickHouse/blob/bd387f6d2c30f67f2822244c0648f2169adab4d3/src/Common/ErrorCodes.cpp#L66
	const clickhouseUnknownTableErrorCode = 60

	if err := clickhouse.conn.Exec(ctx, builder.String()); err != nil {
		clickHouseErr, isClickHouseErr := err.(*clickhouseproto.Exception)
		if isClickHouseErr && clickHouseErr.Code == clickhouseUnknownTableErrorCode {
			return true, nil
		}

		return false, wrap.Error(err, "drop table query failed")
	}

	return false, nil
}
