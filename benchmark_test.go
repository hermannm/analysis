package main

import (
	"context"
	"embed"
	"io"
	"log/slog"
	"os"
	"runtime"
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

	testQuery = db.AnalysisQuery{
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

func BenchmarkIngestion(b *testing.B) {
	schema := newSchema("ingestion_test")
	withTestTable(b, schema, func(reader *csv.Reader) {
		for i := 0; i < b.N; i++ {
			if err := database.IngestData(context.Background(), reader, schema); err != nil {
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

func BenchmarkQuery(b *testing.B) {
	schema := newSchema("query_test")
	withTestTable(b, schema, func(*csv.Reader) {
		for i := 0; i < b.N; i++ {
			if _, err := database.RunAnalysisQuery(
				context.Background(),
				testQuery,
				schema.TableName,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkConcurrentQueries(b *testing.B) {
	const concurrentQueries = 1024

	schema := newSchema("concurrent_queries_test")
	withTestTable(b, schema, func(*csv.Reader) {
		// Divides by GOMAXPROCS, since SetParallelism multiplies its argument by GOMAXPROCS, and we
		// want exactly concurrentQueries number of concurrent queries
		b.SetParallelism(concurrentQueries / runtime.GOMAXPROCS(0))

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := database.RunAnalysisQuery(
					context.Background(),
					testQuery,
					schema.TableName,
				); err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

func BenchmarkCreateTable(b *testing.B) {
	schema := newSchema("create_table_test")

	for i := 0; i < b.N; i++ {
		if err := database.CreateTable(context.Background(), schema); err != nil {
			b.Fatal(wrap.Errorf(err, "failed to create table no. %d", i))
		}

		b.StopTimer()
		if _, err := database.DropTable(context.Background(), schema.TableName); err != nil {
			b.Fatal(wrap.Errorf(err, "failed to clean up table '%s'", schema.TableName))
		}
		b.StartTimer()
	}
}

func newSchema(name string) db.TableSchema {
	return db.TableSchema{TableName: name, Columns: testDataColumns}
}

func withTestTable(b *testing.B, schema db.TableSchema, testFunc func(*csv.Reader)) {
	if err := database.CreateTable(context.Background(), schema); err != nil {
		b.Fatal(wrap.Errorf(err, "failed to create table '%s'", schema.TableName))
	}
	defer func() {
		if _, err := database.DropTable(context.Background(), schema.TableName); err != nil {
			b.Fatal(wrap.Errorf(err, "failed to clean up table '%s' after test", schema.TableName))
		}
	}()

	testFile, err := testData.Open("test-data.csv")
	if err != nil {
		b.Fatal(wrap.Error(err, "failed to open test file"))
	}
	defer testFile.Close()

	reader, err := csv.NewReader(testFile.(io.ReadSeeker), true)
	if err != nil {
		b.Fatal(wrap.Error(err, "failed to create reader for CSV test file"))
	}

	if err := database.IngestData(context.Background(), reader, schema); err != nil {
		b.Fatal(wrap.Errorf(err, "failed to insert test data in table '%s'", schema.TableName))
	}
	if err := reader.ResetReadPosition(true); err != nil {
		b.Fatal(wrap.Error(err, "failed to reset read position in CSV test file"))
	}

	b.ResetTimer()
	testFunc(reader)
	b.StopTimer()
}
