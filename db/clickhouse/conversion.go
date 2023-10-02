package clickhouse

import (
	"hermannm.dev/analysis/db"
	"hermannm.dev/enumnames"
)

// See https://clickhouse.com/docs/en/sql-reference/data-types
var clickhouseDataTypes = enumnames.NewMap(map[db.DataType]string{
	db.DataTypeInt:       "Int64",
	db.DataTypeFloat:     "Float64",
	db.DataTypeTimestamp: "DateTime64(3)",
	db.DataTypeUUID:      "UUID",
	db.DataTypeText:      "String",
})

// See https://clickhouse.com/docs/en/sql-reference/statements/select/order-by
var clickhouseSortOrders = enumnames.NewMap(map[db.SortOrder]string{
	db.SortOrderAscending:  "ASC",
	db.SortOrderDescending: "DESC",
})
