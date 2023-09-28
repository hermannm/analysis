package datatypes

type DataSource interface {
	ReadRow() (row []string, rowNumber int, done bool, err error)
}
