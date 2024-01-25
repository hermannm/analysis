package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"hermannm.dev/analysis/db"
)

type QueryBuilder struct {
	strings.Builder
	parameters clickhouse.Parameters
}

func (query *QueryBuilder) WithParameters(ctx context.Context) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithParameters(query.parameters))
}

func (query *QueryBuilder) AddParameter(value string, parameterType string) {
	// Goes from p0, p1, p2 etc. to ensure unique parameter names
	name := "p" + strconv.Itoa(len(query.parameters))

	// Docs on query parameter syntax:
	// https://clickhouse.com/docs/en/sql-reference/syntax#defining-and-using-query-parameters
	query.WriteByte('{')
	query.WriteString(name)
	query.WriteByte(':')
	query.WriteString(parameterType)
	query.WriteByte('}')

	if query.parameters == nil {
		query.parameters = make(clickhouse.Parameters)
	}
	query.parameters[name] = value
}

func (query *QueryBuilder) AddIntParameter(i int) {
	query.AddParameter(strconv.Itoa(i), typeInt64)
}

func (query *QueryBuilder) AddFloatParameter(f float64) {
	query.AddParameter(strconv.FormatFloat(f, 'f', -1, 64), typeFloat64)
}

func (query *QueryBuilder) AddStringParameter(s string) {
	query.AddParameter(s, typeString)
}

func (query *QueryBuilder) AddIdentifier(identifier string) {
	query.AddParameter(identifier, typeIdentifier)
}

// The query parameter syntax used by AddIdentifier is not available for all queries, such as
// for column names in CREATE TABLE statements. In those cases, we need to quote the provided
// identifier in either ` ` or " " (see https://clickhouse.com/docs/en/sql-reference/syntax#identifiers).
// But to prevent SQL injections, we then first need to ensure that the provided identifier does not
// contain the quote character - if the identifier contains both ` and ", an error is returned.
func (query *QueryBuilder) WriteQuotedIdentifier(identifier string) error {
	if !strings.ContainsRune(identifier, '`') {
		query.WriteByte('`')
		query.WriteString(identifier)
		query.WriteByte('`')
		return nil
	}

	if !strings.ContainsRune(identifier, '"') {
		query.WriteByte('"')
		query.WriteString(identifier)
		query.WriteByte('"')
		return nil
	}

	return fmt.Errorf(
		"'%s' contains both ` and \", making it an invalid database identifier",
		identifier,
	)
}

func (query *QueryBuilder) WriteSortOrder(sortOrder db.SortOrder) (ok bool) {
	if sortOrder, ok := clickhouseSortOrders.GetName(sortOrder); ok {
		query.WriteString(sortOrder)
		return true
	} else {
		return false
	}
}

func (query *QueryBuilder) WriteAggregation(aggregation db.Aggregation) error {
	if err := aggregation.DataType.IsValidForAggregation(); err != nil {
		return err
	}

	kind, ok := clickhouseAggregationKinds.GetName(aggregation.Kind)
	if !ok {
		return errors.New("aggregation kind in query was not recognized")
	}
	query.WriteString(kind)

	query.WriteByte('(')
	query.AddIdentifier(aggregation.FieldName)
	query.WriteByte(')')
	return nil
}

func (query *QueryBuilder) WriteSplit(split db.Split) error {
	switch split.DataType {
	case db.DataTypeInt:
		if split.IntegerInterval != 0 {
			// https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions#floorx-n
			query.WriteString("(floor(")
			query.AddIdentifier(split.FieldName)
			query.WriteString(" / ")
			query.AddIntParameter(split.IntegerInterval)
			query.WriteString(") * ")
			query.AddIntParameter(split.IntegerInterval)
			query.WriteByte(')')
			return nil
		}
	case db.DataTypeFloat:
		if split.FloatInterval != 0 {
			// https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions#floorx-n
			query.WriteString("(floor(")
			query.AddIdentifier(split.FieldName)
			query.WriteString(" / ")
			query.AddFloatParameter(split.FloatInterval)
			query.WriteString(") * ")
			query.AddFloatParameter(split.FloatInterval)
			query.WriteByte(')')
			return nil
		}
	case db.DataTypeDateTime:
		if !split.DateInterval.IsNone() {
			// https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofyear
			switch split.DateInterval {
			case db.DateIntervalYear:
				query.WriteString("toStartOfYear(")
			case db.DateIntervalQuarter:
				query.WriteString("toStartOfQuarter(")
			case db.DateIntervalMonth:
				query.WriteString("toStartOfMonth(")
			case db.DateIntervalWeek:
				query.WriteString("toStartOfWeek(")
			case db.DateIntervalDay:
				query.WriteString("toStartOfDay(")
			default:
				return fmt.Errorf("unrecognized date interval type '%v'", split.DateInterval)
			}

			query.AddIdentifier(split.FieldName)

			if split.DateInterval == db.DateIntervalWeek {
				// Setting mode so that week starts on Mondays
				// https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#toweek
				query.WriteString(", 1)")
			} else {
				query.WriteByte(')')
			}
			return nil
		}
	}

	// If we get here, no interval was specified
	query.AddIdentifier(split.FieldName)
	return nil
}
