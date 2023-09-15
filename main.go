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
	log.Println("Loading environment variables...")
	config, err := readConfigFromEnv()
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to read config from env"))
	}

	log.Println("Connecting to ClickHouse...")
	db, err := db.NewAnalysisDatabase(config.ClickHouse)
	if err != nil {
		log.Fatalln(wrap.Error(err, "failed to initialize database"))
	}

	analysisAPI := api.NewAnalysisAPI(db, http.DefaultServeMux, config.API)

	log.Printf("Listening on port %s...", config.API.Port)
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
	var errs []error

	for _, env := range []struct {
		name  string
		field *string
	}{
		{"API_PORT", &config.API.Port},
		{"CLICKHOUSE_ADDRESS", &config.ClickHouse.Address},
		{"CLICKHOUSE_DB_NAME", &config.ClickHouse.DatabaseName},
		{"CLICKHOUSE_USERNAME", &config.ClickHouse.Username},
		{"CLICKHOUSE_PASSWORD", &config.ClickHouse.Password},
	} {
		if envValue, isSet := os.LookupEnv(env.name); isSet {
			*env.field = envValue
		} else {
			errs = append(errs, fmt.Errorf("%s missing", env.name))
		}
	}

	for _, env := range []struct {
		name  string
		field *bool
	}{
		{"CLICKHOUSE_DEBUG_ENABLED", &config.ClickHouse.Debug},
	} {
		if envValue, isSet := os.LookupEnv(env.name); isSet {
			switch envValue {
			case "true":
				*env.field = true
			case "false":
				*env.field = false
			default:
				errs = append(
					errs, fmt.Errorf("invalid value for %s (must be 'true'/'false')", env.name),
				)
			}
		} else {
			errs = append(errs, fmt.Errorf("%s missing", env.name))
		}
	}

	if len(errs) != 0 {
		return Config{}, wrap.Errors("invalid environment variables", errs...)
	}

	return config, nil
}
