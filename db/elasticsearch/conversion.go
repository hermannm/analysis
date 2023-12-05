package elasticsearch

import (
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/calendarinterval"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types/enums/sortorder"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func schemaToElasticMappings(schema db.TableSchema) (*types.TypeMapping, error) {
	mappings := new(types.TypeMapping)
	mappings.Properties = make(map[string]types.Property, len(schema.Columns))

	for _, column := range schema.Columns {
		property, err := dataTypeToElasticProperty(column.DataType)
		if err != nil {
			return nil, wrap.Errorf(
				err,
				"failed to convert data type to Elasticsearch property for column '%s'",
				column.Name,
			)
		}

		mappings.Properties[column.Name] = property
	}

	return mappings, nil
}

func dataTypeToElasticProperty(dataType db.DataType) (types.Property, error) {
	switch dataType {
	case db.DataTypeText:
		return types.NewKeywordProperty(), nil
	case db.DataTypeInt:
		return types.NewIntegerNumberProperty(), nil
	case db.DataTypeFloat:
		return types.NewFloatNumberProperty(), nil
	case db.DataTypeDateTime:
		return types.NewDateProperty(), nil
	case db.DataTypeUUID:
		return types.NewKeywordProperty(), nil
	default:
		return nil, fmt.Errorf("unrecognized data type '%v'", dataType)
	}
}

func sortOrderToElastic(sortOrder db.SortOrder) (elasticSortOrder sortorder.SortOrder, ok bool) {
	switch sortOrder {
	case db.SortOrderAscending:
		return sortorder.Asc, true
	case db.SortOrderDescending:
		return sortorder.Desc, true
	default:
		return sortorder.SortOrder{}, false
	}
}

func dateIntervalToElastic(
	dateInterval db.DateInterval,
) (elasticDateInterval calendarinterval.CalendarInterval, ok bool) {
	switch dateInterval {
	case db.DateIntervalYear:
		return calendarinterval.Year, true
	case db.DateIntervalQuarter:
		return calendarinterval.Quarter, true
	case db.DateIntervalMonth:
		return calendarinterval.Month, true
	case db.DateIntervalWeek:
		return calendarinterval.Week, true
	case db.DateIntervalDay:
		return calendarinterval.Day, true
	default:
		return calendarinterval.CalendarInterval{}, false
	}
}
