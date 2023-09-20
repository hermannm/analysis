package csv

import (
	"hermannm.dev/analysis/datatypes"
	"hermannm.dev/wrap"
)

func (reader *Reader) DeduceDataSchema(maxRowsToCheck int) (schema datatypes.Schema, err error) {
	// Resets reader position in file before returning, so its data can be read subsequently
	defer func() {
		if resetErr := reader.ResetReadPosition(); resetErr != nil {
			err = wrap.Error(resetErr, "failed to reset CSV file after parsing its column types")
		}
	}()

	columnNames, err := reader.ReadHeaderRow()
	if err != nil {
		return datatypes.Schema{}, wrap.Error(err, "failed to read CSV column names from header row")
	}

	schema = datatypes.NewSchema(columnNames)

	for {
		row, finished, err := reader.ReadRow()
		if finished || reader.CurrentRow() > maxRowsToCheck {
			break
		}
		if err != nil {
			return datatypes.Schema{}, wrap.Errorf(err, "failed to read CSV file")
		}

		if err := schema.DeduceColumnTypesFromRow(row); err != nil {
			return datatypes.Schema{}, wrap.Errorf(
				err, "failed to parse CSV field types from row %d", reader.CurrentRow(),
			)
		}
	}

	if errs := schema.Validate(); len(errs) > 0 {
		return datatypes.Schema{}, wrap.Errors(
			"failed to deduce data types for all given CSV columns", errs...,
		)
	}

	return schema, nil
}
