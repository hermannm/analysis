package clickhouse

import (
	"hermannm.dev/analysis/db"
	"hermannm.dev/enumnames"
)

// ClickHouse data types.
// See https://clickhouse.com/docs/en/sql-reference/data-types
const (
	typeInt64      = "Int64"
	typeFloat64    = "Float64"
	typeDateTime   = "DateTime64(3)"
	typeUUID       = "UUID"
	typeString     = "String"
	typeIdentifier = "Identifier"
)

var clickhouseDataTypes = enumnames.NewMap(map[db.DataType]string{
	db.DataTypeInt:      typeInt64,
	db.DataTypeFloat:    typeFloat64,
	db.DataTypeDateTime: typeDateTime,
	db.DataTypeUUID:     typeUUID,
	db.DataTypeText:     typeString,
})

// See https://clickhouse.com/docs/en/sql-reference/statements/select/order-by
var clickhouseSortOrders = enumnames.NewMap(map[db.SortOrder]string{
	db.SortOrderAscending:  "ASC",
	db.SortOrderDescending: "DESC",
})

// See https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference
var clickhouseAggregationKinds = enumnames.NewMap(map[db.AggregationKind]string{
	db.AggregationSum:     "sum",
	db.AggregationAverage: "avg",
	db.AggregationMin:     "min",
	db.AggregationMax:     "max",
	db.AggregationCount:   "count",
})
