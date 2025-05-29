defmodule SuperWorker do
  @moduledoc """
  **SuperWorker** is responsible for managing a set of workers that execute jobs in parallel.
  It uses a Supervisor to manage the workers, ensuring that they are restarted if they fail.
  ## Usage:
  - Start a SuperWorker
  ```elixir
  {:ok, sup_pid} = SuperWorker.start_link(job_pid, num_workers)
  ```
  - The `job_pid` is the PID of the job that the workers will execute.
  - The `num_workers` specifies how many workers should be started for the job.
  ## Notes:
  - The SuperWorker module uses a Supervisor to manage the workers.
  - It defines a one-for-one restart strategy, meaning that if a worker fails, it will be restarted individually.
  - It logs debug information about the worker startup processes.
  ## Dependencies:
  - `Supervisor`: For managing the workers.
  - `Logger`: For logging debug information.
  """
  use Supervisor
  require Logger

  @doc """
  Initializes the Supervisor with the necessary options and child processes.
  This includes setting the strategy to `:one_for_one` and defining the maximum number of restarts and seconds.
  #### Parameters:
  - `info`: A map containing the job PID and the number of workers to be started.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, sup_pid} = SuperWorker.start_link(job_pid, num_workers)
  ```
  """
  @impl Supervisor
  def init(info) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 2,
      max_seconds: 5
    ]

    Logger.debug("[SuperWorker #{inspect(self())}]: Starting SuperWorker...")
    Logger.debug("[SuperWorker #{inspect(self())}]: init: Received info: #{inspect(info)}.")
    num_workers = if info[:num_workers] == :unbound, do: 1, else: info[:num_workers]

    workers =
      for i <- 1..num_workers do
        %{
          id: :"worker_#{i}",
          start: {Worker, :start_link, [%{job: info[:job_pid]}]}
        }
      end

    Supervisor.init(workers, opts)
  end

  @doc """
  Starts the SuperWorker Supervisor with the given job PID and number of workers.
  This function initializes the Supervisor and sets it up to manage the workers for the specified job.
  #### Parameters:
  - `job_pid`: The PID of the job that the workers will execute.
  - `num_workers`: The number of workers to be started for the job.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, sup_pid} = SuperWorker.start_link(job_pid, num_workers)
  ```
  """
  def start_link(job_pid, num_workers) do
    Logger.debug("[SuperWorker #{inspect(self())}]: Received in start_link: #{inspect(job_pid)}")
    info =
      %{
        job_pid: job_pid,
        num_workers: num_workers
      }

    Supervisor.start_link(__MODULE__, info, [])
  end
end
