defmodule SuperJob do
  @moduledoc """
  **SuperJob** is responsible for managing the lifecycle of a job, including starting the job and its associated workers.
  It uses a Supervisor to manage the job and its workers, ensuring that they are restarted if they fail.
  ## Usage:
  - Start a SuperJob
  ```elixir
  {:ok, sup_pid} = SuperJob.start_link(job_state, num_workers)
  ```
  - The `job_state` should contain the job ID and any other necessary state information.
  - The `num_workers` specifies how many workers should be started for the job.
  ## Notes:
  - The SuperJob module uses a Supervisor to manage the job and its workers.
  - It defines a one-for-one restart strategy, meaning that if a job or worker fails, it will be restarted individually.
  - It logs debug information about the job and worker startup processes.
  ## Dependencies:
  - `Supervisor`: For managing the job and its workers.
  - `Logger`: For logging debug information.
  """
  use Supervisor
  require Logger

  @doc """
  Initializes the Supervisor with the children processes that it will manage.
  The children processes include the job and its associated SuperWorker.
  #### Parameters:
  - `children`: A list of child specifications for the Supervisor, which includes the job and SuperWorker.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  children = [
  %{
    id: :job,
    start: {Job, :start_link, [job_state]},
    restart: :transient
  },
  %{
    id: :super_worker,
    start: {SuperWorker, :start_link, [job_pid, num_workers]},
    restart: :transient
  }
  ]

  {:ok, sup_pid} = SuperJob.init(children)
  ```
  """
  @impl Supervisor
  def init(children) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Logger.debug("[SuperJob #{inspect(self())}]: Definiendo estrategia...")
    Supervisor.init(children, opts)
  end

  @doc """
  Starts a SuperJob with the given job state and number of workers.
  This function initializes the Supervisor and starts the job and its associated SuperWorker.
  #### Parameters:
  - `job_state`: A map containing the job state, including the job ID and any other necessary information.
  - `num_workers`: The number of workers to be started for the job.
  #### Returns:
  - `{:ok, sup_pid}`: If the SuperJob is successfully started, it returns the Supervisor's PID.
  - `{:error, reason}`: If there is an error starting the SuperJob, it returns the error reason.
  #### Example:
  ```elixir
  job_state = %{id: "job_123", other_info: "some_info"}
  {:ok, sup_pid} = SuperJob.start_link(job_state, 4)
  ```
  """
  def start_link(job_state, num_workers) do
    Logger.debug("[SuperJob #{inspect(self())}]: Iniciando...")
    {:ok, sup_pid} = Supervisor.start_link(__MODULE__, [])

    job =
      %{
        id: job_state[:id],
        start: {Job, :start_link, [job_state]},
        restart: :transient
      }

    Logger.debug("[SuperJob #{inspect(self())}]: Iniciando Job...")

    {:ok, job_pid} = Supervisor.start_child(sup_pid, job)

    Logger.debug("[SuperJob #{inspect(self())}]: REsultado: #{inspect(job_pid)}")

    super_worker =
      %{
        id: "sw_" <> inspect(job_state[:id]),
        start: {SuperWorker, :start_link, [job_pid, num_workers]},
        restart: :transient
      }

    Logger.debug("[SuperJob #{inspect(self())}]: Iniciando SuperWorker...")

    result = Supervisor.start_child(sup_pid, super_worker)
    Logger.debug("[SuperJob #{inspect(self())}]: Resultado: #{inspect(result)}...")

    case result do
      {:ok, _} -> {:ok, sup_pid}
      {:error, _} -> result
    end
  end
end
