# analysis

An API server that lets users upload CSV data to perform analytical queries on it, using either
ClickHouse or Elasticsearch as the backing database. Built as part of hermannm's master's thesis in
Computer Science.

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
