package csv

import (
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func (reader *Reader) DeduceTableSchema(maxRowsToCheck int) (schema db.TableSchema, err error) {
	columnNames, err := reader.ReadHeaderRow()
	if err != nil {
		return db.TableSchema{}, wrap.Error(
			err,
			"failed to read CSV column names from header row",
		)
	}

	schema = db.NewTableSchema(columnNames)

	for {
		row, rowNumber, done, err := reader.ReadRow()
		if done || rowNumber > maxRowsToCheck {
			break
		}
		if err != nil {
			return db.TableSchema{}, wrap.Errorf(err, "failed to read CSV file")
		}

		if err := schema.DeduceDataTypesFromRow(row); err != nil {
			return db.TableSchema{}, wrap.Errorf(
				err,
				"failed to parse CSV data types from row %d",
				rowNumber,
			)
		}
	}

	if errs := schema.ValidateColumns(); len(errs) > 0 {
		return db.TableSchema{}, wrap.Errors(
			"failed to deduce data types for all given CSV columns",
			errs...,
		)
	}

	return schema, nil
}
