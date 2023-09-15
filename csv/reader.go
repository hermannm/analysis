package csv

import (
	"encoding/csv"
	"errors"
	"io"
)

type Reader struct {
	inner      *csv.Reader
	file       io.ReadSeeker
	currentRow int
}

func NewReader(csvFile io.ReadSeeker) (*Reader, error) {
	delimiter, err := DeduceFieldDelimiter(csvFile, 20, DefaultDelimitersToCheck)
	if err != nil {
		return nil, err
	}

	inner := csv.NewReader(csvFile)
	inner.ReuseRecord = true
	inner.Comma = delimiter

	return &Reader{inner: inner, file: csvFile, currentRow: 0}, nil
}

func (reader *Reader) ReadRow() (row []string, finished bool, err error) {
	reader.currentRow++

	row, err = reader.inner.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true, nil
		} else {
			return nil, false, err
		}
	}

	return row, false, nil
}

func (reader *Reader) ReadHeaderRow() (row []string, err error) {
	row, finished, err := reader.ReadRow()
	if reader.CurrentRow() != 1 {
		return nil, errors.New("tried to read header row after reading previous rows")
	}
	if finished {
		return nil, errors.New("csv file ended before header row")
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (reader Reader) CurrentRow() int {
	return reader.currentRow
}

func (reader *Reader) ResetReadPosition() error {
	if _, err := reader.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	reader.currentRow = 0
	return nil
}
