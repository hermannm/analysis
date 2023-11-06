package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"hermannm.dev/wrap"
)

func wrapElasticError(wrapped error, message string) error {
	return wrap.Error(formatElasticError(wrapped), message)
}

func wrapElasticErrorf(wrapped error, format string, args ...any) error {
	return wrap.Errorf(formatElasticError(wrapped), format, args...)
}

func formatElasticError(err error) error {
	elasticErr, ok := err.(*types.ElasticsearchError)
	if !ok {
		return err
	}

	var errMessage string
	if elasticErr.ErrorCause.Reason == nil {
		errMessage = fmt.Sprintf("%s (status %d)", elasticErr.ErrorCause.Type, elasticErr.Status)
	} else {
		errMessage = fmt.Sprintf(
			"%s (%s, status %d)",
			*elasticErr.ErrorCause.Reason,
			elasticErr.ErrorCause.Type,
			elasticErr.Status,
		)
	}

	rootCause := make([]error, len(elasticErr.ErrorCause.RootCause))
	for i, cause := range elasticErr.ErrorCause.RootCause {
		if cause.Reason == nil {
			rootCause[i] = errors.New(cause.Type)
		} else {
			rootCause[i] = fmt.Errorf("%s (%s)", *cause.Reason, cause.Type)
		}
	}

	if len(rootCause) == 0 {
		return errors.New(errMessage)
	} else {
		return wrap.Errors(errMessage, rootCause...)
	}
}

func extractElasticError(err error) error {
	split := strings.SplitN(err.Error(), "]", 2)
	if len(split) != 2 {
		return err
	}

	rawError := split[1]
	elasticErr := new(types.ElasticsearchError)
	if marshalErr := json.Unmarshal([]byte(rawError), elasticErr); marshalErr != nil {
		return err
	}

	return elasticErr
}
