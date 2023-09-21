package csv

import (
	"hermannm.dev/analysis/datatypes"
	"hermannm.dev/wrap"
)

func (reader *Reader) DeduceDataSchema(maxRowsToCheck int) (schema datatypes.Schema, err error) {
	// Sets reader position to just after header row before returning, so its data can be read
	// subsequently
	defer func() {
		if resetErr := reader.ResetReadPosition(); resetErr != nil {
			err = wrap.Error(resetErr, "failed to reset CSV file after deducing data schema")
			return
		}
		if _, readErr := reader.ReadHeaderRow(); readErr != nil {
			err = wrap.Error(err, "failed to skip CSV header row after deducing data schema")
		}
	}()

	columnNames, err := reader.ReadHeaderRow()
	if err != nil {
		return datatypes.Schema{}, wrap.Error(
			err,
			"failed to read CSV column names from header row",
		)
	}

	schema = datatypes.NewSchema(columnNames)

	for {
		row, rowNumber, done, err := reader.ReadRow()
		if done || rowNumber > maxRowsToCheck {
			break
		}
		if err != nil {
			return datatypes.Schema{}, wrap.Errorf(err, "failed to read CSV file")
		}

		if err := schema.DeduceColumnTypesFromRow(row); err != nil {
			return datatypes.Schema{}, wrap.Errorf(
				err,
				"failed to parse CSV field types from row %d",
				rowNumber,
			)
		}
	}

	if errs := schema.Validate(); len(errs) > 0 {
		return datatypes.Schema{}, wrap.Errors(
			"failed to deduce data types for all given CSV columns",
			errs...,
		)
	}

	return schema, nil
}
