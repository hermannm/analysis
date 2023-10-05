package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"hermannm.dev/analysis/db"
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

	aggregation, ok := clickhouseAggregations.GetName(query.ValueAggregation.Aggregation)
	if !ok {
		return "", errors.New("invalid aggregation type for value aggregation in query")
	}
	builder.WriteString(aggregation)

	builder.WriteRune('(')
	builder.WriteIdentifier(query.ValueAggregation.BaseColumnName)
	builder.WriteString(") AS value_aggregation ")

	builder.WriteString("FROM ")
	builder.WriteIdentifier(table)

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
	queryResult := db.QueryResult{
		ValueAggregationDataType: query.ValueAggregation.BaseColumnDataType,
		Rows:                     make([]db.RowResult, query.RowSplit.Limit),
		RowsMeta:                 query.RowSplit.SplitMetadata,
		Columns:                  make([]db.ColumnResult, query.ColumnSplit.Limit),
		ColumnsMeta:              query.ColumnSplit.SplitMetadata,
	}

	for i, row := range queryResult.Rows {
		list, err := db.NewDynamicList(
			query.ValueAggregation.BaseColumnDataType,
			query.ColumnSplit.Limit,
		)
		if err != nil {
			return db.QueryResult{}, wrap.Error(
				err,
				"failed to initialize query result values list",
			)
		}

		row.Values = list
		queryResult.Rows[i] = row
	}

	rowResultIndex := 0
	columnResultIndex := 0

	for results.Next() {
		columnValue := getEmptyFieldResultPointer(queryResult.ColumnsMeta.BaseColumnDataType)
		rowValue := getEmptyFieldResultPointer(queryResult.RowsMeta.BaseColumnDataType)
		valueAggregation := getEmptyFieldResultPointer(queryResult.ValueAggregationDataType)
		if columnValue == nil || rowValue == nil || valueAggregation == nil {
			return db.QueryResult{}, errors.New(
				"unhandled data types in query result initialization",
			)
		}

		if err := results.Scan(columnValue, rowValue, valueAggregation); err != nil {
			return db.QueryResult{}, wrap.Error(err, "failed to scan result row")
		}

		columnResult := queryResult.Columns[columnResultIndex]
		rowResult := queryResult.Rows[rowResultIndex]

		if err := useResult(columnValue, queryResult.ColumnsMeta.BaseColumnDataType, func(value any) error {
			if columnResultIndex == 0 {
				columnResult.BaseColumnValue = value
			} else if value != columnResult.BaseColumnValue {
				columnResultIndex++
				columnResult = queryResult.Columns[columnResultIndex]
				columnResult.BaseColumnValue = value
			}
			return nil
		}); err != nil {
			return db.QueryResult{}, err
		}

		if err := useResult(rowValue, queryResult.RowsMeta.BaseColumnDataType, func(value any) error {
			if rowResultIndex == 0 {
				rowResult.BaseColumnValue = value
			} else if value != rowResult.BaseColumnValue {
				rowResultIndex++
				rowResult = queryResult.Rows[rowResultIndex]
				rowResult.BaseColumnValue = value
			}
			return nil
		}); err != nil {
			return db.QueryResult{}, err
		}

		if err := useResult(valueAggregation, queryResult.ValueAggregationDataType, func(value any) error {
			if ok := rowResult.Values.Append(value); !ok {
				return fmt.Errorf(
					"failed to convert field with data type %v",
					queryResult.ValueAggregationDataType,
				)
			}
			return nil
		}); err != nil {
			return db.QueryResult{}, err
		}

		queryResult.Columns[columnResultIndex] = columnResult
		queryResult.Rows[rowResultIndex] = rowResult
	}

	return queryResult, nil
}

// Returns nil for unhandled data types.
func getEmptyFieldResultPointer(dataType db.DataType) any {
	switch dataType {
	case db.DataTypeText:
		var value string
		return &value
	case db.DataTypeInt:
		var value int64
		return &value
	case db.DataTypeFloat:
		var value float64
		return &value
	case db.DataTypeTimestamp:
		var value time.Time
		return &value
	case db.DataTypeUUID:
		var value string
		return &value
	default:
		return nil
	}
}

func useResult(resultPointer any, dataType db.DataType, useFunc func(value any) error) error {
	switch dataType {
	case db.DataTypeText:
		if ptr, ok := resultPointer.(*string); ok {
			return useFunc(*ptr)
		}
	case db.DataTypeInt:
		if ptr, ok := resultPointer.(*int64); ok {
			return useFunc(*ptr)
		}
	case db.DataTypeFloat:
		if ptr, ok := resultPointer.(*float64); ok {
			return useFunc(*ptr)
		}
	case db.DataTypeTimestamp:
		if ptr, ok := resultPointer.(*time.Time); ok {
			return useFunc(*ptr)
		}
	case db.DataTypeUUID:
		if ptr, ok := resultPointer.(*string); ok {
			return useFunc(*ptr)
		}
	default:
		return fmt.Errorf("unrecognized data type %v", dataType)
	}

	return fmt.Errorf("failed to convert field value to data type %v", dataType)
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
