package config

import (
	"fmt"

	"github.com/caarlos0/env/v9"
	"github.com/joho/godotenv"
	"hermannm.dev/wrap"
)

type Config struct {
	BaseConfig
	ClickHouse ClickHouse
}

type BaseConfig struct {
	IsProduction bool        `env:"PRODUCTION"`
	DB           SupportedDB `env:"DATABASE"`
	API          API
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

type SupportedDB string

const (
	DBClickHouse SupportedDB = "clickhouse"
)

func ReadFromEnv() (Config, error) {
	if err := godotenv.Load(); err != nil {
		return Config{}, wrap.Error(err, "failed to load .env file")
	}

	parseOptions := env.Options{RequiredIfNoDef: true}

	var config Config

	if err := env.ParseWithOptions(&config.BaseConfig, parseOptions); err != nil {
		return Config{}, err
	}

	switch config.DB {
	case DBClickHouse:
		if err := env.ParseWithOptions(&config.ClickHouse, parseOptions); err != nil {
			return Config{}, err
		}
	default:
		err := fmt.Errorf("must be one of: '%s'", DBClickHouse)
		return Config{}, wrap.Errorf(err, "unsupported value '%s' for DATABASE in env", config.DB)
	}

	return config, nil
}
