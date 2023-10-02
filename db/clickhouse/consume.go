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
	return db.QueryResult{}, errors.New("not implemented")
}

func (clickhouse ClickHouseDB) Aggregate(
	ctx context.Context,
	tableName string,
	groupColumn string,
	aggregationColumn string,
	limit int,
) (aggregates []db.Aggregate, err error) {
	var builder strings.Builder
	builder.WriteString("SELECT ")

	if err := writeIdentifier(&builder, groupColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for group-by clause")
	}
	builder.WriteString(" AS analysis_group_column, ")

	builder.WriteString("SUM(")
	if err := writeIdentifier(&builder, aggregationColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for aggregate-by clause")
	}
	builder.WriteString(") AS analysis_aggregate ")

	builder.WriteString("FROM ")
	if err := writeIdentifier(&builder, tableName); err != nil {
		return nil, wrap.Error(err, "invalid table name")
	}

	builder.WriteString(" GROUP BY ")
	if err := writeIdentifier(&builder, groupColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for group-by clause")
	}

	builder.WriteString(" ORDER BY analysis_aggregate DESC ")
	builder.WriteString("LIMIT ")
	builder.WriteString(strconv.Itoa(limit))

	if err := clickhouse.conn.Select(ctx, &aggregates, builder.String()); err != nil {
		return nil, wrap.Error(err, "aggregation query failed")
	}

	return aggregates, nil
}
