package db

import (
	"fmt"
	"strings"

	"hermannm.dev/analysis/column"
)

func columnTypeToClickHouse(columnType column.DataType) (string, error) {
	// See https://clickhouse.com/docs/en/sql-reference/data-types
	switch columnType {
	case column.DataTypeInt:
		return "Int64", nil
	case column.DataTypeFloat:
		return "Float64", nil
	case column.DataTypeTimestamp:
		return "DateTime64", nil
	case column.DataTypeUUID:
		return "UUID", nil
	case column.DataTypeString:
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
		"'%s' contains both \" and `, which is incompatible with database", identifier,
	)
}
