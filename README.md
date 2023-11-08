# analysis

An API server that lets users upload CSV data to perform analytical queries on it, using either
ClickHouse or Elasticsearch as the backing database. Built as part of hermannm's master's thesis in
Computer Science.

## Project structure

- `api` defines the API endpoints exposed by the service
- `db` defines the `AnalysisDB` interface, allowing us to toggle between database implementations
  while sharing common functionality
  - `clickhouse` implements `AnalysisDB` for [ClickHouse](https://clickhouse.com/docs/en/intro)
  - `elasticsearch` implements `AnalysisDB` for
    [Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/8.10/elasticsearch-intro.html)
- `csv` implements data type and field delimiter deduction for CSV files
- `config` implements configuration parsing from environment variables

Certain files in the `api`, `clickhouse` and `elasticsearch` packages follow a common pattern:

- `analysis.go` handles execution of analytical queries
- `ingestion.go` handles data ingestion, i.e. creating new database tables and inserting data into
  them
- `schema.go` handles storing and fetching of table schemas

## Local setup

1. Create a `.env` file by copying `.env.example`:

   ```
   cp .env.example .env
   ```

   - On Windows:

     ```
     copy .env.example .env
     ```

2. Set the `DATABASE` field in the `.env` file to either `clickhouse` or `elasticsearch`

3. Start the database with [Docker](https://www.docker.com/products/docker-desktop/)

   - For ClickHouse:

     ```
     docker compose up clickhouse
     ```

   - For Elasticsearch:

     ```
     docker compose up elasticsearch
     ```

4. Run the API server with [Go](https://go.dev/):

   ```
   go run .
   ```
