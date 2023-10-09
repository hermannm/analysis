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

func (clickhouse ClickHouseDB) Aggregate(
	ctx context.Context,
	table string,
	groupColumn string,
	aggregationColumn string,
	limit int,
) (aggregates []db.Aggregate, err error) {
	if err := ValidateIdentifiers(
		table,
		groupColumn,
		aggregationColumn,
	); err != nil {
		return nil, wrap.Error(err, "invalid table/column name in aggregate query")
	}

	var builder QueryBuilder
	builder.WriteString("SELECT ")
	builder.WriteIdentifier(groupColumn)
	builder.WriteString(" AS analysis_group_column, ")

	builder.WriteString("SUM(")
	builder.WriteIdentifier(aggregationColumn)
	builder.WriteString(") AS analysis_aggregate ")

	builder.WriteString("FROM ")
	builder.WriteIdentifier(table)

	builder.WriteString(" GROUP BY ")
	builder.WriteIdentifier(groupColumn)

	builder.WriteString(" ORDER BY analysis_aggregate DESC ")
	builder.WriteString("LIMIT ")
	builder.WriteInt(limit)

	if err := clickhouse.conn.Select(ctx, &aggregates, builder.String()); err != nil {
		return nil, wrap.Error(err, "aggregation query failed")
	}

	return aggregates, nil
}
