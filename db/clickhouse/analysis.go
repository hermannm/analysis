package clickhouse

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/analysis/db"
	"hermannm.dev/devlog/log"
	"hermannm.dev/wrap"
)

func (clickhouse ClickHouseDB) RunAnalysisQuery(
	ctx context.Context,
	analysis db.AnalysisQuery,
	table string,
) (db.AnalysisResult, error) {
	queryString, err := buildAnalysisQueryString(analysis, table)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "failed to parse query")
	}

	log.Debug("generated clickhouse query", slog.String("query", queryString))

	rows, err := clickhouse.conn.Query(ctx, queryString)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "failed to execute query against ClickHouse")
	}

	analysisResult, err := parseAnalysisResultRows(rows, analysis)
	if err != nil {
		return db.AnalysisResult{}, wrap.Error(err, "failed to parse query result")
	}

	return analysisResult, nil
}

func buildAnalysisQueryString(analysis db.AnalysisQuery, table string) (string, error) {
	if analysis.ColumnSplit.Limit == 0 || analysis.RowSplit.Limit == 0 {
		return "", errors.New("column/row split limit cannot be 0")
	}

	if err := ValidateIdentifiers(
		table,
		analysis.ColumnSplit.FieldName,
		analysis.RowSplit.FieldName,
		analysis.Aggregation.FieldName,
	); err != nil {
		return "", wrap.Error(err, "invalid identifier in query")
	}

	var query QueryBuilder
	query.WriteString("SELECT ")

	if err := query.WriteSplit(analysis.ColumnSplit); err != nil {
		return "", wrap.Error(err, "failed to parse query column split")
	}
	query.WriteString(" AS column_split, ")

	if err := query.WriteSplit(analysis.RowSplit); err != nil {
		return "", wrap.Error(err, "failed to parse query row split")
	}
	query.WriteString(" AS row_split, ")

	if err := query.WriteAggregation(analysis.Aggregation); err != nil {
		return "", err
	}
	query.WriteString(" AS value_aggregation ")

	query.WriteString("FROM ")
	query.WriteIdentifier(table)

	// WHERE clause to get the top K rows by totals
	query.WriteString(" WHERE row_split IN (SELECT ")
	query.WriteIdentifier(analysis.RowSplit.FieldName)
	query.WriteString(" FROM ")
	query.WriteIdentifier(table)
	query.WriteString(" GROUP BY ")
	query.WriteIdentifier(analysis.RowSplit.FieldName)
	query.WriteString(" ORDER BY ")
	if err := query.WriteAggregation(analysis.Aggregation); err != nil {
		return "", err
	}
	query.WriteString(" DESC")
	query.WriteString(" LIMIT ")
	query.WriteInt(analysis.RowSplit.Limit)
	query.WriteByte(')')

	query.WriteString(" GROUP BY column_split, row_split")

	query.WriteString(" ORDER BY column_split ")
	sortOrder, ok := clickhouseSortOrders.GetName(analysis.ColumnSplit.SortOrder)
	if !ok {
		return "", errors.New("invalid sort order for column split")
	}
	query.WriteString(sortOrder)

	query.WriteString(", row_split ")
	sortOrder, ok = clickhouseSortOrders.GetName(analysis.ColumnSplit.SortOrder)
	if !ok {
		return "", errors.New("invalid sort order for row split")
	}
	query.WriteString(sortOrder)

	query.WriteString(" LIMIT ")
	query.WriteInt(analysis.ColumnSplit.Limit * analysis.RowSplit.Limit)

	return query.String(), nil
}

func parseAnalysisResultRows(
	rows driver.Rows,
	analysis db.AnalysisQuery,
) (db.AnalysisResult, error) {
	analysisResult := db.NewAnalysisQueryResult(analysis)

	for rows.Next() {
		resultHandle, err := analysisResult.NewResultHandle()
		if err != nil {
			return db.AnalysisResult{}, wrap.Error(err, "failed to initialize result handle")
		}

		if err := rows.Scan(
			resultHandle.Column.Pointer(),
			resultHandle.Row.Pointer(),
			resultHandle.Aggregation.Pointer(),
		); err != nil {
			return db.AnalysisResult{}, wrap.Error(err, "failed to scan result row")
		}

		if err := analysisResult.ParseResultHandle(resultHandle); err != nil {
			return db.AnalysisResult{}, wrap.Error(
				err,
				"failed to parse result from database",
			)
		}
	}

	analysisResult.FillEmptyAggregations()
	return analysisResult, nil
}
