package db

import (
	"context"
	"strconv"
	"strings"

	"hermannm.dev/wrap"
)

type Aggregate struct {
	Column string `ch:"analysis_group_column" json:"column"`
	Sum    int64  `ch:"analysis_aggregate"    json:"sum"`
}

func (db AnalysisDatabase) Aggregate(
	ctx context.Context,
	tableName string,
	groupColumn string,
	aggregationColumn string,
	limit int,
) (aggregates []Aggregate, err error) {
	var query strings.Builder
	query.WriteString("SELECT ")

	if err := writeIdentifier(&query, groupColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for group-by clause")
	}
	query.WriteString(" AS analysis_group_column, ")

	query.WriteString("SUM(")
	if err := writeIdentifier(&query, aggregationColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for aggregate-by clause")
	}
	query.WriteString(") AS analysis_aggregate ")

	query.WriteString("FROM ")
	if err := writeIdentifier(&query, tableName); err != nil {
		return nil, wrap.Error(err, "invalid table name")
	}

	query.WriteString(" GROUP BY ")
	if err := writeIdentifier(&query, groupColumn); err != nil {
		return nil, wrap.Error(err, "invalid column name for group-by clause")
	}

	query.WriteString(" ORDER BY analysis_aggregate DESC ")
	query.WriteString("LIMIT ")
	query.WriteString(strconv.Itoa(limit))

	if err := db.conn.Select(ctx, &aggregates, query.String()); err != nil {
		return nil, wrap.Error(err, "aggregation query failed")
	}

	return aggregates, nil
}
