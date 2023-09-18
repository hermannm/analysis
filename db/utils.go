package db

import (
	"fmt"
	"strings"

	"hermannm.dev/analysis/datatypes"
)

func columnTypeToClickHouse(columnType datatypes.DataType) (string, error) {
	// See https://clickhouse.com/docs/en/sql-reference/data-types
	switch columnType {
	case datatypes.DataTypeInt:
		return "Int64", nil
	case datatypes.DataTypeFloat:
		return "Float64", nil
	case datatypes.DataTypeTimestamp:
		return "DateTime64(3)", nil
	case datatypes.DataTypeUUID:
		return "UUID", nil
	case datatypes.DataTypeString:
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
