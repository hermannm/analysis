package clickhouse

import (
	"hermannm.dev/analysis/db"
	"hermannm.dev/enumnames"
)

// See https://clickhouse.com/docs/en/sql-reference/data-types
var clickhouseDataTypes = enumnames.NewMap(map[db.DataType]string{
	db.DataTypeInt:      "Int64",
	db.DataTypeFloat:    "Float64",
	db.DataTypeDateTime: "DateTime64(3)",
	db.DataTypeUUID:     "UUID",
	db.DataTypeText:     "String",
})

// See https://clickhouse.com/docs/en/sql-reference/statements/select/order-by
var clickhouseSortOrders = enumnames.NewMap(map[db.SortOrder]string{
	db.SortOrderAscending:  "ASC",
	db.SortOrderDescending: "DESC",
})

var clickhouseAggregationKinds = enumnames.NewMap(map[db.AggregationKind]string{
	db.AggregationSum:     "sum",
	db.AggregationAverage: "avg",
	db.AggregationMin:     "min",
	db.AggregationMax:     "max",
	db.AggregationCount:   "count",
})
