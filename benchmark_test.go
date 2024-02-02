package main

import (
	"context"
	"embed"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"

	"hermannm.dev/analysis/config"
	"hermannm.dev/analysis/csv"
	"hermannm.dev/analysis/db"
	"hermannm.dev/devlog"
	"hermannm.dev/devlog/log"
	"hermannm.dev/wrap"
)

var (
	database db.AnalysisDB

	//go:embed test-data.csv
	testData embed.FS

	testDataColumns = []db.Column{
		{Name: "currency", DataType: db.DataTypeText, Optional: false},
		{Name: "value", DataType: db.DataTypeInt, Optional: false},
		{Name: "date", DataType: db.DataTypeDateTime, Optional: false},
		{Name: "invoiceNumber", DataType: db.DataTypeInt, Optional: false},
		{Name: "responsibleName", DataType: db.DataTypeText, Optional: false},
		{Name: "supplierId", DataType: db.DataTypeUUID, Optional: false},
	}
)

// Sets up logger and database connection before running tests.
func TestMain(m *testing.M) {
	logHandler := devlog.NewHandler(os.Stdout, &devlog.Options{Level: slog.LevelDebug})
	slog.SetDefault(slog.New(logHandler))

	conf, err := config.ReadFromEnv()
	if err != nil {
		log.ErrorCause(err, "failed to read config from env")
		os.Exit(1)
	}

	database, err = initializeDatabase(conf)
	if err != nil {
		log.ErrorCause(err, "failed to initialize database")
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func BenchmarkInsertTableData(b *testing.B) {
	testFile, err := testData.Open("test-data.csv")
	if err != nil {
		b.Fatal(wrap.Error(err, "failed to open test file"))
	}
	defer testFile.Close()

	reader, err := csv.NewReader(testFile.(io.ReadSeeker), true)
	if err != nil {
		b.Fatal(wrap.Error(err, "failed to create reader for CSV test file"))
	}

	schema := newSchema("insert_data_test")
	withTestTable(b, schema, func() {
		for i := 0; i < b.N; i++ {
			if err := database.InsertTableData(context.Background(), schema, reader); err != nil {
				b.Fatal(err)
			}

			b.StopTimer()
			if err := reader.ResetReadPosition(true); err != nil {
				b.Fatal(wrap.Error(err, "failed to reset read position in CSV test file"))
			}
			b.StartTimer()
		}
	})
}

func BenchmarkRunAnalysisQuery(b *testing.B) {
	query := db.AnalysisQuery{
		Aggregation: db.Aggregation{
			Kind:      db.AggregationSum,
			FieldName: "value",
			DataType:  db.DataTypeInt,
		},
		RowSplit: db.Split{
			FieldName: "supplierId",
			DataType:  db.DataTypeUUID,
			SortOrder: db.SortOrderDescending,
			Limit:     10,
		},
		ColumnSplit: db.Split{
			FieldName:    "date",
			DataType:     db.DataTypeDateTime,
			SortOrder:    db.SortOrderAscending,
			Limit:        4,
			DateInterval: db.DateIntervalQuarter,
		},
	}

	schema := newSchema("run_query_test")
	withTestTable(b, schema, func() {
		for i := 0; i < b.N; i++ {
			if _, err := database.RunAnalysisQuery(
				context.Background(),
				query,
				schema.TableName,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCreateTable(b *testing.B) {
	for i := 0; i < b.N; i++ {
		schema := newSchema("create_table_test_" + strconv.Itoa(i))
		if err := database.CreateTable(context.Background(), schema); err != nil {
			b.Fatal(wrap.Errorf(err, "failed to create table no. %d", i))
		}
	}

	b.StopTimer()

	for i := 0; i < b.N; i++ {
		tableName := "create_table_test_" + strconv.Itoa(i)
		if _, err := database.DropTable(context.Background(), tableName); err != nil {
			b.Fatal(wrap.Errorf(err, "failed to clean up table '%s' after test", tableName))
		}
	}
}

func newSchema(name string) db.TableSchema {
	return db.TableSchema{TableName: name, Columns: testDataColumns}
}

func withTestTable(b *testing.B, schema db.TableSchema, testFunc func()) {
	if err := database.CreateTable(context.Background(), schema); err != nil {
		b.Fatal(wrap.Error(err, "failed to create table for data insertion test"))
	}
	defer func() {
		if _, err := database.DropTable(context.Background(), schema.TableName); err != nil {
			b.Fatal(wrap.Error(err, "failed to clean up InsertDataTest table after test"))
		}
	}()

	b.ResetTimer()
	testFunc()
	b.StopTimer()
}
