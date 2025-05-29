defmodule SuperManager do
  @moduledoc """
  **SuperManager** is responsible for managing the lifecycle of jobs in the SPE system.
  It uses a Supervisor to manage the SuperJob processes, which handle the execution of jobs and their associated workers.
  ## Usage:
  - Start the SuperManager
  ```elixir
  {:ok, _pid} = SuperManager.start_link()
  ```
  - Start a job with the given job state and number of workers
  ```elixir
  job_state = %{id: "job_1", ...} # Define the job state as needed
  {:ok, _job_pid} = SuperManager.start_job(job_state, 4)
  ```
  ## Notes:
  - The SuperManager uses a one-for-one restart strategy, meaning that if a job fails, it will be restarted individually.
  - It logs debug information about job starts and the SuperJob processes.
  ## Dependencies:
  - `Supervisor`: For managing the SuperJob processes.
  - `Logger`: For logging debug information.
  """
  use Supervisor
  require Logger

  @doc """
  Initializes the Supervisor with the necessary options.
  This includes setting the strategy to `:one_for_one` and defining the maximum number of restarts and seconds.
  #### Parameters:
  - `_init_arg`: Initialization argument, not used in this case.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, sup_pid} = SuperManager.start_link()
  ```
  """
  @impl Supervisor
  def init(_init_arg) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Supervisor.init([], opts)
  end

  @doc """
  Starts the SuperManager Supervisor.
  This function initializes the Supervisor and sets it up to manage SuperJob processes.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, sup_pid} = SuperManager.start_link()
  ```
  """
  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: SPE.SuperManager)
  end

  @doc """
  Starts a SuperJob with the given job state and number of workers.
  This function initializes the SuperJob process and starts it under the SuperManager Supervisor.
  #### Parameters:
  - `job_state`: A map containing the job state, including the job ID and any other necessary information.
  - `num_workers`: The number of workers to be started for the job.
  #### Returns:
  - `{:ok, pid}`: The PID of the started SuperJob process is returned, indicating that the job has been successfully started.
  #### Example:
  ```elixir
  job_state = %{id: "job_1", ...} # Define the job state as needed
  {:ok, _job_pid} = SuperManager.start_job(job_state, 4)
  ```
  """
  def start_job(job_state, num_workers) do
    superjob =
      %{
        id: job_state[:id],
        start: {SuperJob, :start_link, [job_state, num_workers]},
        restart: :transient
      }

    Logger.debug("[SuperManager #{inspect(self())}]: Starting SuperJob...")
    Supervisor.start_child(SPE.SuperManager, superjob)
  end
end
