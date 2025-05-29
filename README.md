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

- **How it works**: The branch introduces mechanisms (e.g., using ETS, DETS, or file serialization) to write job/task state to disk.
- **On restart**: The server loads persisted job/task states and resumes processing where it left off.

## Testing

```sh
mix test
```
Tests cover:
- State persistence and recovery
- All normal job execution scenarios
- Failure and crash-resilience

## Documentation

- **Dependencies**: See `mix.exs`
- **Generate Docs**: `mix docs`
- **HexDocs**: (if published) [HexDocs](https://hexdocs.pm/spe)

## Roadmap

- **Advanced Persistence**: Support for database adapters or cloud storage.
- **Distributed Recovery**: Extend persistence across a distributed cluster.
- **More Robust Error Handling**: Detect and resolve conflicting or orphaned job states.

See [`docs/backlog.md`](docs/backlog.md) for further planning and user stories.

## Logging

All events, including persistence and recovery actions, are logged using Elixir's Logger.

## Contributing

- Make small, descriptive commits.
- Ensure all persistence and recovery features are fully tested.
- Add your name to the `AUTHORS` file.

## License

[Specify your license here]
