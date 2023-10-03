package clickhouse

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) Query(
	ctx context.Context,
	query db.Query,
	table string,
	schema db.TableSchema,
) (db.QueryResult, error) {
	if err := escapeIdentifiers(
		&table,
		&query.ColumnSplit.ColumnName,
		&query.RowSplit.ColumnName,
		&query.ValueAggregation.ColumnName,
	); err != nil {
		return db.QueryResult{}, wrap.Error(err, "invalid identifier in query")
	}

	var builder strings.Builder
	builder.WriteString("SELECT ")
	builder.WriteString(query.ColumnSplit.ColumnName)
	builder.WriteString(" AS column_split, ")
	builder.WriteString(query.RowSplit.ColumnName)
	builder.WriteString(" AS row_split, ")

	aggregation, ok := clickhouseAggregations.GetName(query.ValueAggregation.Aggregation)
	if !ok {
		return db.QueryResult{}, errors.New(
			"invalid aggregation type for value aggregation in query",
		)
	}
	builder.WriteString(aggregation)

	builder.WriteRune('(')
	builder.WriteString(query.ValueAggregation.ColumnName)
	builder.WriteString(") AS value_aggregation ")

	builder.WriteString("FROM ")
	builder.WriteString(table)

	builder.WriteString(" GROUP BY ")
	builder.WriteString(query.ColumnSplit.ColumnName)
	builder.WriteString(", ")
	builder.WriteString(query.RowSplit.ColumnName)

	_, err := clickhouse.conn.Query(ctx, builder.String())
	if err != nil {
		return db.QueryResult{}, wrap.Error(err, "ClickHouse query failed")
	}

	return db.QueryResult{}, errors.New("not implemented")
}

func (clickhouse ClickHouseDB) Aggregate(
	ctx context.Context,
	tableName string,
	groupColumn string,
	aggregationColumn string,
	limit int,
) (aggregates []db.Aggregate, err error) {
	if err := escapeIdentifiers(
		&tableName,
		&groupColumn,
		&aggregationColumn,
	); err != nil {
		return nil, wrap.Error(err, "invalid identifier in aggregate query")
	}

	var builder strings.Builder
	builder.WriteString("SELECT ")
	builder.WriteString(groupColumn)
	builder.WriteString(" AS analysis_group_column, ")

	builder.WriteString("SUM(")
	builder.WriteString(aggregationColumn)
	builder.WriteString(") AS analysis_aggregate ")

	builder.WriteString("FROM ")
	builder.WriteString(tableName)

	builder.WriteString(" GROUP BY ")
	builder.WriteString(groupColumn)

	builder.WriteString(" ORDER BY analysis_aggregate DESC ")
	builder.WriteString("LIMIT ")
	builder.WriteString(strconv.Itoa(limit))

	if err := clickhouse.conn.Select(ctx, &aggregates, builder.String()); err != nil {
		return nil, wrap.Error(err, "aggregation query failed")
	}

	return aggregates, nil
}
