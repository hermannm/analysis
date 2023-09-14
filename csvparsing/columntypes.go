package csvparsing

import (
	"encoding/csv"
	"errors"
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

func ParseCSVColumnTypes(csvFile io.ReadSeeker, fieldDelimiter rune) ([]Column, error) {
	parser := newCSVColumnTypeParser(csvFile, fieldDelimiter)

	if err := parser.parseColumnNames(); err != nil {
		return nil, wrap.Error(err, "failed to parse CSV column names")
	}

	for !parser.done() {
		if err := parser.parseTypesFromRow(); err != nil {
			return nil, wrap.Errorf(
				err, "failed to parse CSV field types from row %d", parser.currentRow,
			)
		}
	}

	// Resets reader position in file, so its data can be read subsequently
	if _, err := csvFile.Seek(0, io.SeekStart); err != nil {
		return nil, wrap.Error(err, "failed to reset CSV file after parsing its column types")
	}

	return parser.columns, nil
}

type typeParser struct {
	reader     *csv.Reader
	columns    []Column
	currentRow int
}

func newCSVColumnTypeParser(csvFile io.ReadSeeker, fieldDelimiter rune) typeParser {
	reader := csv.NewReader(csvFile)
	reader.ReuseRecord = true
	reader.Comma = fieldDelimiter
	return typeParser{reader: reader, columns: nil, currentRow: 1}
}

func (parser *typeParser) parseColumnNames() error {
	if parser.currentRow != 1 {
		return errors.New("tried to read column names after first row")
	}

	headers, err := parser.reader.Read()
	if err != nil {
		return wrap.Error(err, "failed to read first row")
	}

	parser.columns = make([]Column, 0, len(headers))
	for _, header := range headers {
		parser.columns = append(parser.columns, Column{
			Name: header, Type: "",
		})
	}

	return nil
}

func (parser *typeParser) parseTypesFromRow() error {
	parser.currentRow++

	row, err := parser.reader.Read()
	if err != nil {
		return wrap.Error(err, "failed to read row")
	}

	for i, field := range row {
		if i >= len(parser.columns) {
			return errors.New("row contains more fields than there are columns")
		}

		if parser.columns[i].Type != "" {
			continue
		}

		if columnType, deduced := deduceColumnTypeFromField(field); deduced {
			parser.columns[i].Type = columnType
		}
	}

	return nil
}

func deduceColumnTypeFromField(field string) (columnType ColumnType, deduced bool) {
	if field == "" {
		return "", false
	}

	if _, err := strconv.ParseInt(field, 10, 64); err == nil {
		return ColumnTypeInt, true
	}

	if _, err := strconv.ParseFloat(field, 64); err == nil {
		return ColumnTypeFloat, true
	}

	if _, err := time.Parse(time.RFC3339, field); err == nil {
		return ColumnTypeTimestamp, true
	}

	return ColumnTypeString, true
}

func (parser *typeParser) done() bool {
	for _, column := range parser.columns {
		if column.Type == "" {
			return false
		}
	}
	return true
}
