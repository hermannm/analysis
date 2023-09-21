package config

import (
	"github.com/caarlos0/env/v9"
	"github.com/joho/godotenv"
	"hermannm.dev/wrap"
)

type Config struct {
	IsProduction bool `env:"PRODUCTION"`
	API          API
	ClickHouse   ClickHouse
}

type API struct {
	Port string `env:"API_PORT"`
}

type ClickHouse struct {
	Address            string `env:"CLICKHOUSE_ADDRESS"`
	DatabaseName       string `env:"CLICKHOUSE_DB_NAME"`
	Username           string `env:"CLICKHOUSE_USERNAME"`
	Password           string `env:"CLICKHOUSE_PASSWORD"`
	Debug              bool   `env:"CLICKHOUSE_DEBUG_ENABLED"`
	DropTableOnStartup string `env:"DEBUG_DROP_TABLE_ON_STARTUP" envDefault:""`
}

func ReadFromEnv() (Config, error) {
	if err := godotenv.Load(); err != nil {
		return Config{}, wrap.Error(err, "failed to load .env file")
	}

	var config Config
	if err := env.ParseWithOptions(&config, env.Options{RequiredIfNoDef: true}); err != nil {
		return Config{}, err
	}

	return config, nil
}
