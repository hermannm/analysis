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

func (query *QueryBuilder) WriteInt(i int) {
	query.WriteString(strconv.Itoa(i))
}

func (query *QueryBuilder) WriteFloat(f float64) {
	query.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
}

// Must only be called after calling ValidateIdentifier/ValidateIdentifiers on the given identifier.
func (query *QueryBuilder) WriteIdentifier(identifier string) {
	query.WriteByte('`')
	query.WriteString(identifier)
	query.WriteByte('`')
}

func (query *QueryBuilder) WriteAggregation(aggregation db.Aggregation) error {
	if err := aggregation.BaseColumnDataType.IsValidForAggregation(); err != nil {
		return err
	}

	kind, ok := clickhouseAggregationKinds.GetName(aggregation.Kind)
	if !ok {
		return errors.New("aggregation kind in query was not recognized")
	}
	query.WriteString(kind)

	query.WriteByte('(')
	query.WriteIdentifier(aggregation.BaseColumnName)
	query.WriteByte(')')
	return nil
}

func (query *QueryBuilder) WriteSplit(split db.Split) error {
	switch split.BaseColumnDataType {
	case db.DataTypeInt:
		if split.IntegerInterval != 0 {
			// https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions#floorx-n
			query.WriteString("(floor(")
			query.WriteIdentifier(split.BaseColumnName)
			query.WriteString(" / ")
			query.WriteInt(split.IntegerInterval)
			query.WriteString(") * ")
			query.WriteInt(split.IntegerInterval)
			query.WriteByte(')')
			return nil
		}
	case db.DataTypeFloat:
		if split.FloatInterval != 0 {
			// https://clickhouse.com/docs/en/sql-reference/functions/rounding-functions#floorx-n
			query.WriteString("(floor(")
			query.WriteIdentifier(split.BaseColumnName)
			query.WriteString(" / ")
			query.WriteFloat(split.FloatInterval)
			query.WriteString(") * ")
			query.WriteFloat(split.FloatInterval)
			query.WriteByte(')')
			return nil
		}
	case db.DataTypeTimestamp:
		if split.DateInterval != nil {
			dateInterval := *split.DateInterval

			// https://clickhouse.com/docs/en/sql-reference/functions/date-time-functions#tostartofyear
			switch dateInterval {
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
				return fmt.Errorf("unrecognized date interval type '%v'", dateInterval)
			}

			query.WriteIdentifier(split.BaseColumnName)

			if dateInterval == db.DateIntervalWeek {
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
	query.WriteIdentifier(split.BaseColumnName)
	return nil
}

func ValidateIdentifier(identifier string) error {
	if identifier == "" {
		return errors.New("received blank identifier")
	}

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
