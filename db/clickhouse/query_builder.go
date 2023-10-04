package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
)

type QueryBuilder struct {
	strings.Builder
}

func (builder *QueryBuilder) WriteInt(i int) {
	builder.WriteString(strconv.Itoa(i))
}

// Must only be called after calling ValidateIdentifier/ValidateIdentifiers on the given identifier.
func (builder *QueryBuilder) WriteIdentifier(identifier string) {
	builder.WriteRune('`')
	builder.WriteString(identifier)
	builder.WriteRune('`')
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
