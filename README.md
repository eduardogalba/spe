# SPE

This branch implements the **SPE** main structure. It lays the foundation for submitting, planning, and executing jobs composed of interdependent tasks, using Elixir's concurrency primitives and supervision trees.

## Overview

- **SPE Server Bootstrapping**: Configurable startup, supervision of core components.
- **Job Submission**: Accepts job descriptions, validates their structure and dependencies.
- **Execution Plan Skeleton**: Outlines the calculation of execution plans (using e.g., Kahn's algorithm).
- **Job Management**: Prepares for tracking and managing the lifecycle of submitted jobs.
- **Extensible for Task Execution and Error Handling**: Stubs in place for future implementation.

## Features & Structure

- **SPE Module**: The main entry point and GenServer, responsible for server startup, job submission, and job start requests.
- **JobManager Module**: Receives delegated job-related messages, will manage job state and execution flow.
- **Scheduler Module**: Implements skeleton logic for planning execution order of tasks, e.g., finding tasks with no dependencies.
- **PubSub Integration**: Sets up Phoenix PubSub for future job/task event broadcasting.
- **Validation**: Checks job description shape, uniqueness of task names, and that dependencies are well-formed.
- **Logging**: Uses Elixir's Logger for debugging and status reporting.

## Quick Start

### Prerequisites

- **Elixir**: ~> 1.18
- **Erlang/OTP**: Compatible with Elixir 1.18

### Setup

Clone the repo:
```sh
git clone https://github.com/eduardogalba/spe.git
cd spe
```

Install dependencies:
```sh
mix deps.get
```

### Running

Start the server (in interactive mode):
```sh
iex -S mix
```
Or programmatically:
```elixir
{:ok, _pid} = SPE.start_link(num_workers: 4)
```

### Submitting a Job

A job is a map like:
```elixir
job = %{
  "name" => "example",
  "tasks" => [
    %{"name" => "t1", "exec" => fn -> ... end},
    %{"name" => "t2", "exec" => fn -> ... end, "enables" => {"t1"}}
  ]
}
SPE.submit_job(job)
```

## Development Roadmap

- **Job Lifecycle**: Full management of in-progress, completed, and failed tasks.
- **Task Execution**: Concurrency, worker pool management, and result collection.
- **Robust Error Handling**: Timeouts, crash isolation, dependency-aware skips.
- **PubSub Events**: Real-time job/task status notifications.
- **Testing**: ExUnit-based test suite for APIs and behaviors.

For detailed planning, see [`docs/backlog.md`](docs/backlog.md).

## Logging

The SPE system uses Elixir's `Logger` for all major events. Adjust log level as needed for debugging:
```elixir
Logger.configure(level: :debug)
```

## Contributing

- Follow best practices for Git commits (small, descriptive).
- Ensure tests (to be implemented) pass before submitting PRs.
- See `AUTHORS` for contributor listing.

## License

[Specify your license here]
