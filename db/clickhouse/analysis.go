package clickhouse

import (
	"context"
	"errors"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/analysis/db"
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
	if analysis.RowSplit.Limit == 0 || analysis.ColumnSplit.Limit == 0 {
		return "", errors.New("column/row split limit cannot be 0")
	}

	if err := ValidateIdentifiers(
		table,
		analysis.RowSplit.FieldName,
		analysis.ColumnSplit.FieldName,
		analysis.Aggregation.FieldName,
	); err != nil {
		return "", wrap.Error(err, "invalid table/field name in query")
	}

	var query QueryBuilder
	query.WriteString("SELECT ")

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
	query.WriteString(" AS aggregation ")

	query.WriteString("FROM ")
	query.WriteIdentifier(table)

	// WHERE clause to get the top N rows by aggregation totals
	query.WriteString(" WHERE row_split IN (SELECT ")
	query.WriteIdentifier(analysis.RowSplit.FieldName)
	query.WriteString(" FROM ")
	query.WriteIdentifier(table)
	query.WriteString(" GROUP BY ")
	query.WriteIdentifier(analysis.RowSplit.FieldName)
	query.WriteString(" ORDER BY ")
	query.WriteAggregation(analysis.Aggregation) // err checked above
	query.WriteString(" DESC")
	query.WriteString(" LIMIT ")
	query.WriteInt(analysis.RowSplit.Limit)
	query.WriteByte(')')

	query.WriteString(" GROUP BY column_split, row_split")

	return query.String(), nil
}

func parseAnalysisResultRows(
	rows driver.Rows,
	analysis db.AnalysisQuery,
) (db.AnalysisResult, error) {
	analysisResult := db.NewAnalysisQueryResult(analysis)

	for rows.Next() {
		handle, err := analysisResult.NewResultHandle()
		if err != nil {
			return db.AnalysisResult{}, wrap.Error(err, "failed to initialize result handle")
		}

		if err := rows.Scan(
			handle.Row.Pointer(),
			handle.Column.Pointer(),
			handle.Aggregation.Pointer(),
		); err != nil {
			return db.AnalysisResult{}, wrap.Error(err, "failed to scan clickhouse result row")
		}

		if err := analysisResult.ParseResultHandle(handle); err != nil {
			return db.AnalysisResult{}, err
		}
	}

	if err := analysisResult.Finalize(); err != nil {
		return db.AnalysisResult{}, err
	}

	return analysisResult, nil
}
