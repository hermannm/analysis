package clickhouse

import (
	"context"
	"fmt"
	"strings"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"hermannm.dev/wrap"
)

func escapeIdentifier(identifier string) (escaped string, err error) {
	if !strings.ContainsRune(identifier, '`') {
		return fmt.Sprintf("`%s`", identifier), nil
	} else if !strings.ContainsRune(identifier, '"') {
		return fmt.Sprintf(`"%s"`, identifier), nil
	} else {
		return "", fmt.Errorf(
			"'%s' contains both \" and `, which is incompatible with database",
			identifier,
		)
	}
}

func escapeIdentifiers(identifiers ...*string) error {
	for _, identifier := range identifiers {
		escaped, err := escapeIdentifier(*identifier)
		if err != nil {
			return err
		}

		*identifier = escaped
	}

	return nil
}

func (clickhouse ClickHouseDB) dropTable(
	ctx context.Context,
	tableName string,
) (tableAlreadyDropped bool, err error) {
	tableName, err = escapeIdentifier(tableName)
	if err != nil {
		return false, wrap.Error(err, "invalid table name")
	}

	var builder strings.Builder
	builder.WriteString("DROP TABLE ")
	builder.WriteString(tableName)

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
