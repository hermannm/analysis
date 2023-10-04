package clickhouse

import (
	"context"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) dropTable(
	ctx context.Context,
	table string,
) (tableAlreadyDropped bool, err error) {
	if err := ValidateIdentifier(table); err != nil {
		return false, wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("DROP TABLE ")
	builder.WriteIdentifier(table)

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
