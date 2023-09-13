# analysis

New implementation of the Analysis service from the Ignite Procurement platform. Built as part of
hermannm's master's thesis in Computer Science.

## Local setup

1. Create a `.env` file by copying `.env.example`:

   ```
   cp .env.example .env
   ```

2. Start ClickHouse with Docker:

   ```
   docker compose up
   ```

3. Run the API server with Go:

   ```
   go run .
   ```
