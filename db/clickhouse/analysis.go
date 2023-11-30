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
		return "", wrap.Error(err, "invalid table/field name in query")
	}

	var query QueryBuilder
	query.WriteString(
		"SELECT row_split, column_split, aggregation, aggregation_total FROM ",
	)

	// First: Group aggregations by both row and column split
	query.WriteString("(SELECT ")
	if err := query.WriteSplit(analysis.RowSplit); err != nil {
		return "", wrap.Error(err, "failed to parse query row split")
	}
	query.WriteString(" AS row_split, ")
	if err := query.WriteSplit(analysis.ColumnSplit); err != nil {
		return "", wrap.Error(err, "failed to parse query column split")
	}
	query.WriteString(" AS column_split, ")
	if err := query.WriteAggregation(analysis.Aggregation); err != nil {
		return "", err
	}
	query.WriteString(" AS aggregation")
	query.WriteString(" FROM ")
	query.WriteIdentifier(table)
	query.WriteString(" GROUP BY column_split, row_split")
	query.WriteString(") AS splits")

	// We want to join the two SELECTs on the value of row_split, so we can sort by totals
	query.WriteString(" INNER JOIN ")

	// Second: Group aggregations by row split only, to get the totals for each row split
	query.WriteString("(SELECT ")
	query.WriteSplit(analysis.RowSplit) // Error checked in previous SELECT
	query.WriteString(" AS row_split, ")
	query.WriteAggregation(analysis.Aggregation) // Error checked in previous SELECT
	query.WriteString(" AS aggregation_total")
	query.WriteString(" FROM ")
	query.WriteIdentifier(table)
	query.WriteString(" GROUP BY row_split")
	query.WriteString(" ORDER BY aggregation_total ")
	if ok := query.WriteSortOrder(analysis.RowSplit.SortOrder); !ok {
		return "", errors.New("invalid sort order for row split")
	}
	query.WriteString(" LIMIT ")
	query.WriteInt(analysis.RowSplit.Limit)
	query.WriteString(") AS totals")

	query.WriteString(" ON splits.row_split = totals.row_split")

	query.WriteString(" ORDER BY aggregation_total ")
	query.WriteSortOrder(analysis.RowSplit.SortOrder) // Checked for ok above
	query.WriteString(", column_split ")
	if ok := query.WriteSortOrder(analysis.ColumnSplit.SortOrder); !ok {
		return "", errors.New("invalid sort order for column split")
	}

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
			resultHandle.Row.Pointer(),
			resultHandle.Column.Pointer(),
			resultHandle.Aggregation.Pointer(),
			resultHandle.Total.Pointer(),
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
