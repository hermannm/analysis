package csvparsing

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"hermannm.dev/wrap"
)

type ColumnType string

const (
	ColumnTypeString    ColumnType = "String"
	ColumnTypeInt       ColumnType = "Integer"
	ColumnTypeFloat     ColumnType = "Float"
	ColumnTypeTimestamp ColumnType = "Timestamp"
)

type Column struct {
	Name     string
	Type     ColumnType
	Optional bool
}

func DeduceColumnTypes(
	csvFile io.ReadSeeker, fieldDelimiter rune, maxRowsToCheck int,
) (columns []Column, err error) {
	// Resets reader position in file before returning, so its data can be read subsequently
	defer func() {
		if _, seekErr := csvFile.Seek(0, io.SeekStart); seekErr != nil {
			err = wrap.Error(err, "failed to reset CSV file after parsing its column types")
		}
	}()

	parser := newColumnTypeParser(csvFile, fieldDelimiter, maxRowsToCheck)

	if err := parser.parseColumnNames(); err != nil {
		return nil, wrap.Error(err, "failed to parse CSV column names")
	}

	for row, finished, err := parser.readRow(); !finished; {
		if err != nil {
			return nil, wrap.Errorf(err, "failed to read row %d of CSV file", parser.currentRow)
		}

		if err := parser.deduceTypesFromRow(row); err != nil {
			return nil, wrap.Errorf(
				err, "failed to parse CSV field types from row %d", parser.currentRow,
			)
		}
	}

	if errs := parser.checkUndeducedColumnTypes(); len(errs) > 0 {
		return nil, wrap.Errors("failed to deduce data types for these CSV columns", errs...)
	}

	return parser.columns, nil
}

type typeParser struct {
	reader         *csv.Reader
	columns        []Column
	currentRow     int
	maxRowsToCheck int
}

func newColumnTypeParser(csvFile io.ReadSeeker, fieldDelimiter rune, maxRowsToCheck int) typeParser {
	reader := csv.NewReader(csvFile)
	reader.ReuseRecord = true
	reader.Comma = fieldDelimiter
	return typeParser{reader: reader, columns: nil, currentRow: 0, maxRowsToCheck: maxRowsToCheck}
}

func (parser *typeParser) readRow() (row []string, finished bool, err error) {
	parser.currentRow++
	if parser.currentRow > parser.maxRowsToCheck {
		return nil, true, nil
	}

	row, err = parser.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true, nil
		} else {
			return nil, false, err
		}
	}

	return row, false, nil
}

func (parser *typeParser) parseColumnNames() error {
	headers, finished, err := parser.readRow()
	if parser.currentRow != 1 {
		return errors.New("tried to read column names after first row")
	}
	if finished {
		return errors.New("csv file ended before getting to parse column names")
	}
	if err != nil {
		return wrap.Error(err, "failed to read CSV header row")
	}

	parser.columns = make([]Column, 0, len(headers))
	for _, header := range headers {
		parser.columns = append(parser.columns, Column{
			Name: header, Type: "", Optional: false,
		})
	}

	return nil
}

func (parser *typeParser) deduceTypesFromRow(row []string) error {
	if parser.columns == nil {
		return errors.New("tried to deduce column types before parsing column names")
	}

	for i, field := range row {
		if i >= len(parser.columns) {
			return errors.New("row contains more fields than there are columns")
		}

		column := parser.columns[i]

		deducedType, isBlank := deduceColumnTypeFromField(field)
		if isBlank {
			column.Optional = true
		} else if column.Type == "" {
			column.Type = deducedType
		} else if column.Type != deducedType {
			return fmt.Errorf(
				"found incompatible data types '%s' and '%s' in column '%s'",
				column.Type, deducedType, column.Name,
			)
		}

		parser.columns[i] = column
	}

	return nil
}

func deduceColumnTypeFromField(field string) (deducedType ColumnType, isBlank bool) {
	if field == "" {
		return "", true
	}

	if _, err := strconv.ParseInt(field, 10, 64); err == nil {
		return ColumnTypeInt, false
	}

	if _, err := strconv.ParseFloat(field, 64); err == nil {
		return ColumnTypeFloat, false
	}

	if _, err := time.Parse(time.RFC3339, field); err == nil {
		return ColumnTypeTimestamp, false
	}

	return ColumnTypeString, false
}

func (parser typeParser) checkUndeducedColumnTypes() []error {
	var undeducedColumns []error

	for i, column := range parser.columns {
		if column.Type == "" {
			undeducedColumns = append(
				undeducedColumns, fmt.Errorf("'%s' (column %d)", column.Name, i),
			)
		}
	}

	return undeducedColumns
}
