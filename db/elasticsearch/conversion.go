package elasticsearch

import (
	"errors"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/analysis/db"
)

func schemaToElasticMappings(schema db.TableSchema) (*types.TypeMapping, error) {
	return nil, errors.New("not implemented")
}
