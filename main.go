package main

import (
	"fmt"
	"log"
	"net/http"
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

	db, err := db.NewAnalysisDatabase(config.ClickHouse)
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to initialize database"))
	}

	analysisAPI := api.NewAnalysisAPI(db, http.DefaultServeMux, config.API)

	if err := analysisAPI.ListenAndServe(); err != nil {
		log.Fatalln(wrap.Error(err, "server stopped"))
	}
}

type Config struct {
	API        api.Config
	ClickHouse db.ClickHouseConfig
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
		{"API_PORT", &config.API.Port},
		{"CLICKHOUSE_ADDRESS", &config.ClickHouse.Address},
		{"CLICKHOUSE_DB_NAME", &config.ClickHouse.Database},
		{"CLICKHOUSE_USERNAME", &config.ClickHouse.Username},
		{"CLICKHOUSE_PASSWORD", &config.ClickHouse.Password},
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
