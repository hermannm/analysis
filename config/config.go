package config

import (
	"fmt"

	"github.com/caarlos0/env/v9"
	"github.com/joho/godotenv"
	"hermannm.dev/wrap"
)

type Config struct {
	BaseConfig
	ClickHouse    ClickHouse
	Elasticsearch Elasticsearch
}

type BaseConfig struct {
	Environment        Environment `env:"ENVIRONMENT"`
	API                API
	DB                 SupportedDB `env:"DATABASE"`
	DropTableOnStartup string      `env:"DEBUG_DROP_TABLE_ON_STARTUP" envDefault:""`
}

type API struct {
	Port string `env:"API_PORT"`
}

type ClickHouse struct {
	Address      string `env:"CLICKHOUSE_ADDRESS"`
	DatabaseName string `env:"CLICKHOUSE_DB_NAME"`
	Username     string `env:"CLICKHOUSE_USERNAME"`
	Password     string `env:"CLICKHOUSE_PASSWORD"`
	Debug        bool   `env:"CLICKHOUSE_DEBUG_ENABLED"`
}

type Elasticsearch struct {
	Address string `env:"ELASTICSEARCH_ADDRESS"`
	Debug   bool   `env:"ELASTICSEARCH_DEBUG_ENABLED"`
}

type SupportedDB string

const (
	DBClickHouse    SupportedDB = "clickhouse"
	DBElasticsearch SupportedDB = "elasticsearch"
)

type Environment string

const (
	Production  Environment = "production"
	Development Environment = "development"
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
	case DBElasticsearch:
		if err := env.ParseWithOptions(&config.Elasticsearch, parseOptions); err != nil {
			return Config{}, err
		}
	default:
		err := fmt.Errorf("must be one of: '%s'/'%s'", DBClickHouse, DBElasticsearch)
		return Config{}, wrap.Errorf(err, "unsupported value '%s' for DATABASE in env", config.DB)
	}

	return config, nil
}

// Implements [encoding.TextUnmarshaler], to ensure valid Environment values.
func (environment *Environment) UnmarshalText(text []byte) error {
	value := Environment(text)
	switch value {
	case Production, Development:
		*environment = value
		return nil
	default:
		return fmt.Errorf(
			"invalid ENVIRONMENT value '%s', must be '%s' or '%s'",
			value,
			Production,
			Development,
		)
	}
}
