# SPE – Optional/Persistence Branch

This branch extends the core SPE (Scalable Parallel Executor) system with **persistence features**. The goal is to allow jobs and tasks to survive server restarts, enabling reliable long-running workflows.

---

## Overview

- **Persistence Layer**: Jobs and their task states are periodically saved to disk (or another persistent store).
- **Crash/Restart Recovery**: On server startup, SPE reloads saved jobs and resumes incomplete executions.
- **All Core Features**: Inherits all job submission, planning, execution, and reporting features from the mainline SPE design.

## Key Features

- **Persistent Job State**: Each job’s current state, including task progress and results, is stored outside of volatile memory.
- **Automatic Restore**: After server restart, unfinished jobs are automatically restored and resumed.
- **Fault Tolerance**: Protects against data loss from crashes or planned maintenance.

## Installation

- **Elixir**: ~> 1.18
- **Erlang/OTP**: Compatible with Elixir 1.18

```sh
git clone https://github.com/eduardogalba/spe.git
cd spe
git checkout optional/persistence
mix deps.get
```

## Usage

Start the SPE server as usual:
```sh
iex -S mix
```
or
```elixir
{:ok, _pid} = SPE.start_link(num_workers: 4)
```

### Job Submission

Jobs are submitted as maps:
```elixir
job = %{
  "name" => "example_persistent_job",
  "tasks" => [
    %{"name" => "t1", "exec" => fn -> ... end},
    %{"name" => "t2", "exec" => fn -> ... end, "enables" => ["t1"]}
  ]
}
SPE.submit_job(job)
```

### Persistence Details

- **How it works**: This branch uses Ecto to connect to PostgreSQL and save/restore job states in the database.
- **Connection details**: The connection configuration is found in [`config/config.exs`](config/config.exs), where you can set the username, password, database, and host.
  ```elixir
  config :spe, SPE.Repo,
    username: "postgres",
    password: "postgres",
    database: "spe",
    hostname: "localhost",
    pool_size: 10
  ```
- **Migrations**: Database migrations are located in [`priv/repo/migrations`](priv/repo/migrations). The `jobs` table schema defines how job states are stored.
- **Ecto commands**:
  - Initialize the database:
    ```sh
    mix ecto.create
    ```
  - Apply migrations:
    ```sh
    mix ecto.migrate
    ```
- **Docker Compose**: Optionally, you can use the provided `docker-compose.yml` file to quickly start a ready-to-use PostgreSQL service.
  ```yaml
  version: "3"
  services:
    db:
      image: postgres:16
      environment:
        POSTGRES_USER: postgres
        POSTGRES_PASSWORD: postgres
        POSTGRES_DB: spe
      ports:
        - "5432:5432"
      volumes:
        - pgdata:/var/lib/postgresql/data
  volumes:
    pgdata:
  ```
- **Jobs DML**: The `jobs` table stores each job's state, including name, plan, tasks, results, and timestamps.
  ```sql
  CREATE TABLE jobs (
    name VARCHAR PRIMARY KEY,
    plan JSONB,
    num_workers INTEGER,
    enables JSONB,
    returns JSONB,
    results JSONB,
    tasks JSONB,
    inserted_at TIMESTAMP
  );
  ```
- **On restart**: The server loads persisted job/task states and resumes processing where it left off.

## Testing

To ensure the persistence features work correctly:

- **Unit tests**: Covering Ecto integration and job state management.
- **Integration tests**: End-to-end scenarios from job submission to recovery.
- **Manual tests**: Verify behavior during unexpected shutdowns and restarts.

Run tests with:
```sh
mix test test/spe_test.exs
```

## Documentation

- **Dependencies**: See `mix.exs`
- **Generate Docs**: `mix docs`
- **HexDocs**: (if published) [HexDocs](https://hexdocs.pm/spe)

## Roadmap

- **Advanced Persistence**: Support for database adapters or cloud storage.
- **Distributed Recovery**: Extend persistence across a distributed cluster.
- **More Robust Error Handling**: Detect and resolve conflicting or orphaned job states.

## Logging

All events, including persistence and recovery actions, are logged using Elixir's Logger.
Logging configuration details are also found in [`config/config.exs`](config/config.exs)
```elixir
  config :logger, level: :info
  ```
