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
job = %{"name" => "nisse", "tasks" => [%{"name" => "t0", "enables" => [], "exec" => fn _ -> 1 + 2 end, "timeout" => :infinity}]}

id = case SPE.submit_job(job) do
  {:ok, job_id} -> job_id
  {:error, desc} -> desc
end
```

### Starting a Job
```elixir
SPE.start_job(job_id)
```

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
