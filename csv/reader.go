package csv

import (
	"encoding/csv"
	"errors"
	"io"

	"hermannm.dev/wrap"
)

type Reader struct {
	inner      *csv.Reader
	file       io.ReadSeeker
	currentRow int
}

func NewReader(csvFile io.ReadSeeker, skipHeaderRow bool) (*Reader, error) {
	delimiter, err := DeduceFieldDelimiter(csvFile, 20, DefaultDelimitersToCheck)
	if err != nil {
		return nil, err
	}

	reader := &Reader{inner: newInnerReader(csvFile, delimiter), file: csvFile, currentRow: 0}

	if skipHeaderRow {
		if _, err := reader.ReadHeaderRow(); err != nil {
			return nil, wrap.Error(err, "failed to skip CSV header row")
		}
	}

	return reader, nil
}

func newInnerReader(csvFile io.ReadSeeker, delimiter rune) *csv.Reader {
	reader := csv.NewReader(csvFile)
	reader.ReuseRecord = true
	reader.Comma = delimiter
	return reader
}

// Implements db.DataSource
func (reader *Reader) ReadRow() (row []string, rowNumber int, done bool, err error) {
	reader.currentRow++

	row, err = reader.inner.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, true, nil
		} else {
			return nil, 0, false, err
		}
	}

	return row, reader.currentRow, false, nil
}

func (reader *Reader) ReadHeaderRow() (row []string, err error) {
	row, rowNumber, done, err := reader.ReadRow()
	if rowNumber != 1 {
		return nil, errors.New("tried to read header row after reading previous rows")
	}
	if done {
		return nil, errors.New("csv file ended before header row")
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (reader *Reader) ResetReadPosition(skipHeaderRow bool) error {
	if _, err := reader.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader.currentRow = 0
	reader.inner = newInnerReader(reader.file, reader.inner.Comma)

	if skipHeaderRow {
		if _, err := reader.ReadHeaderRow(); err != nil {
			return wrap.Error(err, "failed to skip CSV header row")
		}
	}

	return nil
}
