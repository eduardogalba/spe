defmodule SPE do
  @moduledoc """
  **SPE** is the main module for the SPE (Scalable Parallel Execution) system.
  It is responsible for starting the server, managing job submissions, and handling job starts.
  It uses a GenServer to maintain the state of the system and handle calls related to job management.
  ## Usage:
  - Start the SPE server
  ```elixir
  {:ok, _pid} = SPE.start_link(num_workers: 4)
  ```
  - Submit a job
  ```elixir
  job_desc = %{
    "tasks" => [
      %{"name" => "task1", "enables" => ["task2"]},
      %{"name" => "task2", "enables" => []}
    ]
  }
  {:ok, job_id} = SPE.submit_job(job_desc)
  ```
  - Start the job
  ```elixir
  {:ok, _job_id} = SPE.start_job(job_id)
  ```
  ## Notes:
  - The SPE module uses a `JobManager` to handle job submissions and starts.
  - It maintains a state that includes waiting jobs, which are jobs that are waiting for their plans to be ready before they can start.
  - It logs debug information about job submissions, starts, and other events.
  ## Dependencies:
  - `GenServer`: For managing the state and handling calls.
  - `Logger`: For logging debug information.
  - `JobManager`: A module responsible for managing jobs, including submitting jobs, starting them, and handling their plans.
  - `SuperManager`: A module responsible for managing the overall system and ensuring that the SPE server is running.
  """
  use GenServer
  require Logger

  @doc """
  Initializes the SPE server with the given options and state.
  The options can include the number of workers, which will be passed to the JobManager.
  #### Parameters:
  - `opts`: A keyword list of options, including `:num_workers` to specify the number of workers.
  - `state`: The initial state of the SPE server, which includes a map for waiting jobs.
  #### Returns:
  - `{:ok, state}`: The initial state of the SPE server is returned, ready to handle job submissions and starts.
  #### Example:
  ```elixir
  {:ok, _pid} = SPE.start_link(num_workers: 4)
  ```
  """
  def init({opts, state}) do
    Logger.info("[SPE #{inspect(self())}]: Server starting...")

    case SuperSPE.start_link(opts) do
      {:ok, supv} ->
        ref = Process.monitor(supv)
        {:ok, Map.put(state, :supv, %{ref: ref, pid: supv})}

      {:error, {:already_started, _}} = error ->
        Logger.error("[SPE #{inspect(self())}]: Server is already started.")
        error

      {:error, {:shutdown, reason}} = error ->
        Logger.error(
          "[SPE #{inspect(self())}]: One of the child processes is crashing caused by #{inspect(reason)}"
        )
        error
    end
  end

  @doc """
  Handles synchronous calls to the SPE server.
  It processes job start requests and job submissions.
  #### Parameters:
  - `{:start_job, job_id}`: A tuple containing the job ID to start.
  - `{:submit_job, job_desc}`: A tuple containing the job description to submit.
  #### Returns:
  - `{:reply, response, state}`: The response from the JobManager for the job start or submission, along with the current state.
  #### Example:
  ```elixir
  {:ok, job_id} = SPE.start_job("job_123")
  {:ok, job_id} = SPE.submit_job(%{"tasks" => [%{"name" => "task1", "enables" => []}]})
  ```
  """
  def handle_call({:start_job, job_id}, from, state) do
    case JobManager.start_job(job_id) do
      {:warn, :wait_until_plan} ->
        new_state =
          update_in(
            state[:waiting],
            fn waiting ->
              Map.put(waiting, job_id, from)
            end
          )

        {:noreply, new_state}

      {:ok, _} = ok ->
        {:reply, ok, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:submit_job, job_desc}, _from, state) do
    {:reply, JobManager.submit_job(job_desc), state}
  end

  def handle_cast({:ready_to_start, {job_id, response}}, state) do
    new_state =
      update_in(
        state[:waiting],
        fn clients ->
          Map.delete(clients, job_id)
        end
      )

    case response do
      {:ok, _} -> GenServer.reply(state[:waiting][job_id], {:ok, job_id})
      {:error, _} = error -> GenServer.reply(state[:waiting][job_id], error)
    end

    {:noreply, new_state}
  end

  @doc """
  Handles generic info messages sent to the SPE server.
  It logs the received message and returns the current state without any changes.
  #### Parameters:
  - `msg`: The message received by the SPE server.
  #### Returns:
  - `{:noreply, state}`: The current state of the SPE server is returned without any changes.
  #### Example:
  ```elixir
  SPE.handle_info(:some_info_message, state)
  ```
  """
  def handle_info(msg, state) do
    Logger.debug("Info generico")
    Logger.debug("#{inspect(msg)}")
    {:noreply, state}
  end

  @doc """
  Starts the SPE server with the given options.
  #### Parameters:
  - `opts`: A keyword list of options, including `:num_workers` to specify the number of workers.
  #### Returns:
  - `{:ok, pid}`: The PID of the started SPE server.
  #### Example:
  ```elixir
  {:ok, _pid} = SPE.start_link(num_workers: 4)
  ```
  """
  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      {opts, %{waiting: %{}}},
      name: SPE
    )
  end

  @doc """
  Submits a job to the SPE server with the given job description.
  #### Parameters:
  - `job_desc`: A map containing the job description, including tasks and their dependencies.
  #### Returns:
  - `{:ok, job_id}`: If the job is successfully submitted and planned, it returns the job ID.
  - `{:error, :invalid_description}`: If the job description is invalid, it returns an error.
  #### Example:
  ```elixir
  job_desc = %{
    "tasks" => [
      %{"name" => "task1", "enables" => ["task2"]},
      %{"name" => "task2", "enables" => []}
    ]
  }
  {:ok, job_id} = SPE.submit_job(job_desc)
  ```
  """
  def submit_job(job_desc) do
    if !Validator.valid_job?(job_desc) do
      {:error, :invalid_description}
    else
      GenServer.call(SPE, {:submit_job, job_desc})
    end
  end

  @doc """
  Starts a job with the given job ID.
  #### Parameters:
  - `job_id`: The ID of the job to be started.
  #### Returns:
  - `{:ok, job_id}`: If the job is successfully started, it returns the job ID.
  - `{:error, error}`: If there is an error in starting the job, it returns the error.
  #### Example:
  ```elixir
  {:ok, _job_id} = SPE.start_job("12345")
  ```
  """
  def start_job(job_id) do
    GenServer.call(SPE, {:start_job, job_id})
  end

  @doc """
  Notifies the SPE server that a job is ready to start.
  This function is called when a job plan is ready, and it will notify the waiting clients that the job can now start.
  #### Parameters:
  - `job_id`: The ID of the job that is ready to start.
  - `response`: The response from the JobManager indicating whether the job can start or if there was an error.
  #### Returns:
  - `:ok`: If the job is successfully marked as ready to start.
  #### Example:
  ```elixir
  job_id = "12345"
  response = {:ok, job_id}
  SPE.job_ready(job_id, response)
  ```
  """
  def job_ready(job_id, response) do
    GenServer.cast(SPE, {:ready_to_start, {job_id, response}})
  end

  @doc """
  Terminates the SPE server and stops the Supervisor managing it.
  This function is called when the SPE server is no longer needed, and it will clean up the resources used by the server.
  #### Parameters:
  - `_reason`: The reason for termination, which is not used in this implementation.
  - `state`: The current state of the SPE server, which includes the Supervisor's PID.
  #### Returns:
  - `:ok`: Indicates that the SPE server has been successfully terminated.
  #### Example:
  ```elixir
  SPE.terminate(:normal, state)
  ```
  """
  def terminate(_reason, state) do
    Supervisor.stop(state[:supv][:pid])
    :ok
  end
end
