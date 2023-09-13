package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"hermannm.dev/analysis/api"
	"hermannm.dev/analysis/db"
	"hermannm.dev/wrap"
)

func main() {
	config, err := readConfigFromEnv()
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to read config from env"))
	}

	db, err := db.NewAnalysisDatabase(config.clickhouse)
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to initialize database"))
	}

	api.NewAnalysisAPI(db)
}

type Config struct {
	clickhouse db.ClickHouseConfig
}

func readConfigFromEnv() (Config, error) {
	err := godotenv.Load()
	if err != nil {
		return Config{}, wrap.Error(err, "failed to load .env file")
	}

	var config Config
	var missingEnvs []error

	for _, env := range []struct {
		name  string
		field *string
	}{
		{"CLICKHOUSE_ADDRESS", &config.clickhouse.Address},
		{"CLICKHOUSE_DB_NAME", &config.clickhouse.Database},
		{"CLICKHOUSE_USERNAME", &config.clickhouse.Username},
		{"CLICKHOUSE_PASSWORD", &config.clickhouse.Password},
	} {
		if envValue, isSet := os.LookupEnv(env.name); isSet {
			*env.field = envValue
		} else {
			missingEnvs = append(missingEnvs, fmt.Errorf("%s missing", env.name))
		}
	}

	if len(missingEnvs) != 0 {
		return Config{}, wrap.Errors("missing environment variables", missingEnvs...)
	}

	return config, nil
}
