package elasticsearch

import (
	"fmt"

	elastictypes "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func schemaToElasticMappings(schema db.TableSchema) (*elastictypes.TypeMapping, error) {
	mappings := new(elastictypes.TypeMapping)
	mappings.Properties = make(map[string]elastictypes.Property, len(schema.Columns))

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

func dataTypeToElasticProperty(dataType db.DataType) (elastictypes.Property, error) {
	switch dataType {
	case db.DataTypeText:
		return elastictypes.NewTextProperty(), nil
	case db.DataTypeInt:
		return elastictypes.NewIntegerNumberProperty(), nil
	case db.DataTypeFloat:
		return elastictypes.NewFloatNumberProperty(), nil
	case db.DataTypeTimestamp:
		return elastictypes.NewDateProperty(), nil
	case db.DataTypeUUID:
		return elastictypes.NewTextProperty(), nil
	default:
		return nil, fmt.Errorf("unrecognized data type '%v'", dataType)
	}
}
