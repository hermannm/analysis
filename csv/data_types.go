package csv

import (
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (reader *Reader) DeduceDataTypes(maxRowsToCheck int) (schema db.Schema, err error) {
	columnNames, err := reader.ReadHeaderRow()
	if err != nil {
		return db.Schema{}, wrap.Error(
			err,
			"failed to read CSV column names from header row",
		)
	}

	schema = db.NewSchema(columnNames)

	for {
		row, rowNumber, done, err := reader.ReadRow()
		if done || rowNumber > maxRowsToCheck {
			break
		}
		if err != nil {
			return db.Schema{}, wrap.Errorf(err, "failed to read CSV file")
		}

		if err := schema.DeduceDataTypesFromRow(row); err != nil {
			return db.Schema{}, wrap.Errorf(
				err,
				"failed to parse CSV data types from row %d",
				rowNumber,
			)
		}
	}

	if errs := schema.Validate(); len(errs) > 0 {
		return db.Schema{}, wrap.Errors(
			"failed to deduce data types for all given CSV columns",
			errs...,
		)
	}

	return schema, nil
}
