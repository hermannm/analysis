package clickhouse

import (
	"context"
	"errors"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/analysis/db"
	"hermannm.dev/analysis/log"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) Query(
	ctx context.Context,
	query db.Query,
	table string,
) (db.QueryResult, error) {
	queryString, err := buildQueryString(query, table)
	if err != nil {
		return db.QueryResult{}, wrap.Error(err, "failed to parse query")
	}

	log.Infof("Generated query:\n%s", queryString)

	results, err := clickhouse.conn.Query(ctx, queryString)
	if err != nil {
		return db.QueryResult{}, wrap.Error(err, "failed to execute query on database")
	}

	parsedResults, err := parseQueryResult(results, query)
	if err != nil {
		return db.QueryResult{}, wrap.Error(err, "failed to parse query results")
	}

	return parsedResults, nil
}

func buildQueryString(query db.Query, table string) (string, error) {
	if query.ColumnSplit.Limit == 0 || query.RowSplit.Limit == 0 {
		return "", errors.New("column/row split limit cannot be 0")
	}

	switch query.ValueAggregation.BaseColumnDataType {
	case db.DataTypeInt, db.DataTypeFloat:
	default:
		return "", fmt.Errorf(
			"value aggregation can only be done on INTEGER or FLOAT columns, not %v",
			query.ValueAggregation.BaseColumnDataType,
		)
	}

	if err := ValidateIdentifiers(
		table,
		query.ColumnSplit.BaseColumnName,
		query.RowSplit.BaseColumnName,
		query.ValueAggregation.BaseColumnName,
	); err != nil {
		return "", wrap.Error(err, "invalid identifier in query")
	}

	var builder QueryBuilder
	builder.WriteString("SELECT ")
	builder.WriteSplit(query.ColumnSplit)
	builder.WriteString(" AS column_split, ")
	builder.WriteSplit(query.RowSplit)
	builder.WriteString(" AS row_split, ")

	if err := builder.WriteAggregation(query.ValueAggregation); err != nil {
		return "", err
	}
	builder.WriteString(" AS value_aggregation ")

	builder.WriteString("FROM ")
	builder.WriteIdentifier(table)

	// WHERE clause to get the top K rows by totals
	builder.WriteString(" WHERE row_split IN (SELECT ")
	builder.WriteIdentifier(query.RowSplit.BaseColumnName)
	builder.WriteString(" FROM ")
	builder.WriteIdentifier(table)
	builder.WriteString(" GROUP BY ")
	builder.WriteIdentifier(query.RowSplit.BaseColumnName)
	builder.WriteString(" ORDER BY ")
	if err := builder.WriteAggregation(query.ValueAggregation); err != nil {
		return "", err
	}
	builder.WriteString(" DESC")
	builder.WriteString(" LIMIT ")
	builder.WriteInt(query.RowSplit.Limit)
	builder.WriteRune(')')

	builder.WriteString(" GROUP BY column_split, row_split")

	builder.WriteString(" ORDER BY column_split ")
	sortOrder, ok := clickhouseSortOrders.GetName(query.ColumnSplit.SortOrder)
	if !ok {
		return "", errors.New("invalid sort order for column split")
	}
	builder.WriteString(sortOrder)

	builder.WriteString(", row_split ")
	sortOrder, ok = clickhouseSortOrders.GetName(query.ColumnSplit.SortOrder)
	if !ok {
		return "", errors.New("invalid sort order for row split")
	}
	builder.WriteString(sortOrder)

	builder.WriteString(" LIMIT ")
	builder.WriteInt(query.ColumnSplit.Limit * query.RowSplit.Limit)

	return builder.String(), nil
}

func parseQueryResult(results driver.Rows, query db.Query) (db.QueryResult, error) {
	queryResult := db.NewQueryResult(query)

	for results.Next() {
		resultHandle, err := queryResult.NewResultHandle()
		if err != nil {
			return db.QueryResult{}, wrap.Error(err, "failed to initialize result handle")
		}

		if err := results.Scan(
			resultHandle.ColumnValue.Pointer(),
			resultHandle.RowValue.Pointer(),
			resultHandle.ValueAggregation.Pointer(),
		); err != nil {
			return db.QueryResult{}, wrap.Error(err, "failed to scan result row")
		}

		if err := queryResult.ParseResult(resultHandle); err != nil {
			return db.QueryResult{}, wrap.Error(err, "failed to parse results from database")
		}
	}

	queryResult.TruncateColumns()
	return queryResult, nil
}

func (clickhouse ClickHouseDB) GetTableSchema(
	ctx context.Context,
	table string,
) (schema db.TableSchema, err error) {
	if err = ValidateIdentifier(table); err != nil {
		return db.TableSchema{}, wrap.Error(err, "invalid table name")
	}

	var builder QueryBuilder
	builder.WriteString("SELECT ")
	builder.WriteIdentifier(schemasTableSchemaColumn)
	builder.WriteString(" FROM ")
	builder.WriteIdentifier(schemasTable)
	builder.WriteString(" WHERE (")
	builder.WriteIdentifier(schemasTableNameColumn)
	builder.WriteString(" = ?)")

	result := clickhouse.conn.QueryRow(ctx, builder.String(), table)
	if err := result.Err(); err != nil {
		return db.TableSchema{}, wrap.Error(err, "table schema query failed")
	}

	if err := result.Scan(&schema.Columns); err != nil {
		return db.TableSchema{}, wrap.Error(err, "failed to parse table schema from database")
	}

	if errs := schema.Validate(); len(errs) != 0 {
		return db.TableSchema{}, wrap.Errors("database returned invalid table schema", errs...)
	}

	return schema, nil
}
