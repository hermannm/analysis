package clickhouse

import (
	"context"
	"errors"

	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) Query(
	ctx context.Context,
	query db.Query,
	table string,
	schema db.TableSchema,
) (db.QueryResult, error) {
	if err := ValidateIdentifiers(
		table,
		query.ColumnSplit.BaseColumnName,
		query.RowSplit.BaseColumnName,
		query.ValueAggregation.BaseColumnName,
	); err != nil {
		return db.QueryResult{}, wrap.Error(err, "invalid identifier in query")
	}

	var builder QueryBuilder
	builder.WriteString("SELECT ")
	builder.WriteSplit(query.ColumnSplit)
	builder.WriteString(" AS column_split, ")
	builder.WriteSplit(query.RowSplit)
	builder.WriteString(" AS row_split, ")

	aggregation, ok := clickhouseAggregations.GetName(query.ValueAggregation.Aggregation)
	if !ok {
		return db.QueryResult{}, errors.New(
			"invalid aggregation type for value aggregation in query",
		)
	}
	builder.WriteString(aggregation)

	builder.WriteRune('(')
	builder.WriteIdentifier(query.ValueAggregation.BaseColumnName)
	builder.WriteString(") AS value_aggregation ")

	builder.WriteString("FROM ")
	builder.WriteIdentifier(table)

	builder.WriteString(" GROUP BY column_split, row_split")

	builder.WriteString(" SORT BY column_split ")
	sortOrder, ok := clickhouseSortOrders.GetName(query.ColumnSplit.SortOrder)
	if !ok {
		return db.QueryResult{}, errors.New("invalid sort order for column split")
	}
	builder.WriteString(sortOrder)

	builder.WriteString(", row_split ")
	sortOrder, ok = clickhouseSortOrders.GetName(query.ColumnSplit.SortOrder)
	if !ok {
		return db.QueryResult{}, errors.New("invalid sort order for row split")
	}
	builder.WriteString(sortOrder)

	builder.WriteString(" LIMIT ")
	builder.WriteInt(query.ColumnSplit.Limit * query.RowSplit.Limit)

	_, err := clickhouse.conn.Query(ctx, builder.String())
	if err != nil {
		return db.QueryResult{}, wrap.Error(err, "ClickHouse query failed")
	}

	return db.QueryResult{}, errors.New("not implemented")
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
