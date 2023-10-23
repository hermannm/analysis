package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"hermannm.dev/analysis/db"
)

type QueryBuilder struct {
	strings.Builder
}

func (builder *QueryBuilder) WriteInt(i int) {
	builder.WriteString(strconv.Itoa(i))
}

func (builder *QueryBuilder) WriteFloat(f float64) {
	builder.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
}

// Must only be called after calling ValidateIdentifier/ValidateIdentifiers on the given identifier.
func (builder *QueryBuilder) WriteIdentifier(identifier string) {
	builder.WriteByte('`')
	builder.WriteString(identifier)
	builder.WriteByte('`')
}

func (builder *QueryBuilder) WriteAggregation(valueAggregation db.ValueAggregation) error {
	aggregation, ok := clickhouseAggregations.GetName(valueAggregation.Aggregation)
	if !ok {
		return errors.New("invalid aggregation type for value aggregation in query")
	}
	builder.WriteString(aggregation)

	builder.WriteByte('(')
	builder.WriteIdentifier(valueAggregation.BaseColumnName)
	builder.WriteByte(')')
	return nil
}

func (builder *QueryBuilder) WriteSplit(split db.Split) error {
	switch split.BaseColumnDataType {
	case db.DataTypeInt:
		if split.IntegerInterval != 0 {
			// https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions#floorx-n
			builder.WriteString("(floor(")
			builder.WriteIdentifier(split.BaseColumnName)
			builder.WriteString(" / ")
			builder.WriteInt(split.IntegerInterval)
			builder.WriteString(") * ")
			builder.WriteInt(split.IntegerInterval)
			builder.WriteByte(')')
			return nil
		}
	case db.DataTypeFloat:
		if split.FloatInterval != 0 {
			// https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions#floorx-n
			builder.WriteString("(floor(")
			builder.WriteIdentifier(split.BaseColumnName)
			builder.WriteString(" / ")
			builder.WriteFloat(split.FloatInterval)
			builder.WriteString(") * ")
			builder.WriteFloat(split.FloatInterval)
			builder.WriteByte(')')
			return nil
		}
	case db.DataTypeTimestamp:
		if split.DateInterval != nil {
			dateInterval := *split.DateInterval

			// https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofyear
			switch dateInterval {
			case db.DateIntervalYear:
				builder.WriteString("toStartOfYear(")
			case db.DateIntervalQuarter:
				builder.WriteString("toStartOfQuarter(")
			case db.DateIntervalMonth:
				builder.WriteString("toStartOfMonth(")
			case db.DateIntervalWeek:
				builder.WriteString("toStartOfWeek(")
			case db.DateIntervalDay:
				builder.WriteString("toStartOfDay(")
			default:
				return fmt.Errorf("unrecognized date interval type '%v'", dateInterval)
			}

			builder.WriteIdentifier(split.BaseColumnName)

			if dateInterval == db.DateIntervalWeek {
				// Setting mode so that week starts on Mondays
				// https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#toweek
				builder.WriteString(", 1)")
			} else {
				builder.WriteByte(')')
			}
			return nil
		}
	}

	// If we get here, no interval was specified
	builder.WriteIdentifier(split.BaseColumnName)
	return nil
}

func ValidateIdentifier(identifier string) error {
	if strings.ContainsRune(identifier, '`') {
		return fmt.Errorf("'%s' contains `, which is incompatible with database", identifier)
	}

	return nil
}

func ValidateIdentifiers(identifiers ...string) error {
	for _, identifier := range identifiers {
		if err := ValidateIdentifier(identifier); err != nil {
			return err
		}
	}

	return nil
}
