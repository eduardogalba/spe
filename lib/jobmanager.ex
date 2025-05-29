defmodule JobManager do
  @moduledoc """
  **JobManager** is responsible for managing jobs, including submitting jobs, starting them, and handling their plans.
  It uses a GenServer to maintain state and handle calls related to job management.

  ## Usage:
  - Start the JobManager
  ```elixir
  {:ok, _pid} = JobManager.start_link(num_workers: 4)
  ```
  - Submit a job
  ```elixir
  job_desc = %{
    "tasks" => [
      %{"name" => "task1", "enables" => ["task2"]},
      %{"name" => "task2", "enables" => []}
    ]
  }
  {:ok, job_id} = JobManager.submit_job(job_desc)
  ```
  - Start the job
  ```elixir
  {:ok, _job_id} = JobManager.start_job(job_id)
  ```

  ## Notes:
  - The JobManager uses a planning module (`Planner`) to create a plan for the job based on the job description and the number of workers available.
  - It handles job submissions and starts jobs based on the plans created.
  - The JobManager maintains a state that includes the jobs and their plans, as well as the number of workers available for job execution.
  - It logs debug information about job submissions, plans, and task management.

  ## Dependencies:
  - `GenServer`: For managing the state and handling calls.
  - `Logger`: For logging debug information.
  - `Planner`: A module responsible for planning jobs based on their descriptions and available workers.
  """

  use GenServer
  require Logger

  @doc """
  Initializes the JobManager with a state containing the number of workers, jobs, and waiting jobs.
  The `num_workers` can be set to `:unbound` to allow dynamic worker allocation based on job requirements.
  The `jobs` map will hold the jobs submitted, and `waiting` can be used for jobs that are waiting to be processed.
  #### Parameters:
  - `state`: The initial state of the JobManager, including the number of workers, jobs, and waiting jobs.
  #### Returns:
  - `{:ok, state}`: The initial state of the JobManager is returned, ready to handle job submissions and starts.
  #### Example:
  ```elixir
  {:ok, _pid} = JobManager.start_link(num_workers: 4)
  ```
  """
  def init(state) do
    {:ok, state}
  end

  @doc """
  Handles job submission by receiving a job description, planning the job, and storing it in the state.
  #### Parameters:
  - `{:submit, job_desc}`: A tuple containing the job description to be submitted.
  #### Returns:
  - `{:reply, {:ok, job_id}, new_state}`: If the job is successfully planned and stored, it returns the job ID and the updated state.
  - `{:reply, {:error, error}, state}`: If there is an error in planning the job, it returns the error and the current state.
  #### Example:
  ```elixir
  job_desc = %{
    "tasks" => [
      %{"name" => "task1", "enables" => ["task2"]},
      %{"name" => "task2", "enables" => []}
    ]
  }
  {:ok, job_id} = JobManager.submit_job(job_desc)
  ```
  """
  def handle_call({:submit, job_desc}, _from, state) do
    job_id = "#{inspect(make_ref())}"
    plan = Planner.planning(job_desc, state[:num_workers])

    case plan do
      {:ok, new_plan} ->
        Logger.debug("[JobManager #{inspect(self())}]: Receiving plan for #{inspect(job_id)}...")

        tasks =
          Enum.reduce(
            job_desc["tasks"],
            %{},
            fn task_desc, acc ->
              Map.put(acc, task_desc["name"], task_desc)
            end
          )

        enables =
          Enum.reduce(
            tasks,
            %{},
            fn {task_name, desc}, acc ->
              if desc["enables"] do
                Map.put(acc, task_name, desc["enables"])
              else
                acc
              end
            end
          )

        # Si no hay num_workers definido optamos por el nivel
        # de concurrencia que pueda necesitar mas workers
        # Cambio de estrategia crear un proceso es costoso y tarda bastante
        # Si este maximo es menor que el propuesto por el usuario, se ignora
        maximum_concurrent_tasks = new_plan |> Enum.map(&length/1) |> Enum.max()

        num_workers =
          if state[:num_workers] == :unbound or maximum_concurrent_tasks < state[:num_workers] do
            maximum_concurrent_tasks
          else
            state[:num_workers]
          end

        Logger.debug(
          "[JobManager #{inspect(self())}]: For Job #{inspect(job_id)} will run maximum #{inspect(num_workers)} tasks."
        )

        Logger.debug(
          "[JobManager #{inspect(self())}]: For Job #{inspect(job_id)} the plan is #{inspect(new_plan)}"
        )

        Logger.debug("[JobManager #{inspect(self())}]: Tengo en enables #{inspect(enables)}")

        new_job = %{
          id: job_id,
          plan: new_plan,
          enables: enables,
          num_workers: num_workers,
          tasks: tasks
        }

        new_jobs =
          state[:jobs]
          |> Map.put(job_id, new_job)

        {:reply, {:ok, job_id}, Map.put(state, :jobs, new_jobs)}

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  def handle_call({:start, job_id}, _from, state) do
    if !Map.has_key?(state[:jobs], job_id) do
      ## OJO! No para aqui
      {:reply, {:error, :unregistered_job}, state}
    else
      # Pensandolo bien el JobManager no creo que necesita saber mas la informacion
      # del job, simplemente su pid si este se cae
      # OJO! Aqui se podria comprobar la longitud maxima de las sublistas
      # y si num_workers es unbound se establece esa longitud como num_workers
      job = state[:jobs][job_id]

      case SuperManager.start_job(job, job[:num_workers]) do
        {:ok, _} -> {:reply, {:ok, job_id}, state}
        any -> {:reply, any, state}
      end
    end
  end

  @doc """
  Handles job planning by receiving a job ID and its planned tasks, updating the state with the new plan.
  #### Parameters:
  - `{:planning, {job_id, job_plan}}`: A tuple containing the job ID and its planned tasks.
  #### Returns:
  - `{:noreply, new_state}`: The updated state after planning the job.
  #### Example:
  ```elixir
  job_id = "12345"
  job_plan = [%{"task" => "task1"}, %{"task" => "task2"}]
  JobManager.plan_ready(job_id, job_plan)
  ```
  """
  def handle_info(msg, state) do
    Logger.debug("Info generico")
    Logger.debug("#{inspect(msg)}")
    {:noreply, state}
  end

  @doc """
  Starts the JobManager GenServer with the specified options.
  #### Parameters:
  - `opts`: A keyword list of options, including `:num_workers` to specify the number of workers and `:name` to set the GenServer name.
  #### Returns:
  - `{:ok, pid}`: The PID of the started JobManager GenServer.
  #### Example:
  ```elixir
  {:ok, _pid} = JobManager.start_link(num_workers: 4, name: :job_manager)
  ```
  """
  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      %{num_workers: Keyword.get(opts, :num_workers, :unbound), jobs: %{}, waiting: %{}},
      name: Keyword.get(opts, :name, SPE.JobManager)
    )
  end

  @doc """
  Submits a job to the JobManager with the given job description.
  #### Parameters:
  - `job_desc`: A map containing the job description, including tasks and their dependencies.
  #### Returns:
  - `{:ok, job_id}`: If the job is successfully submitted and planned, it returns the job ID.
  - `{:error, error}`: If there is an error in planning the job, it returns the error.
  #### Example:
  ```elixir
  job_desc = %{
    "tasks" => [
      %{"name" => "task1", "enables" => ["task2"]},
      %{"name" => "task2", "enables" => []}
    ]
  }
  {:ok, job_id} = JobManager.submit_job(job_desc)
  ```
  """
  def submit_job(job_desc) do
    GenServer.call(SPE.JobManager, {:submit, job_desc})
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
  {:ok, _job_id} = JobManager.start_job("12345")
  ```
  """
  def start_job(job_id) do
    GenServer.call(SPE.JobManager, {:start, job_id})
  end

  @doc """
  Notifies the JobManager that a job plan is ready for the given job ID.
  #### Parameters:
  - `job_id`: The ID of the job for which the plan is ready.
  - `job_plan`: The planned tasks for the job.
  #### Returns:
  - `:ok`: If the plan is successfully received and processed.
  #### Example:
  ```elixir
  job_id = "12345"
  job_plan = [%{"task" => "task1"}, %{"task" => "task2"}]
  JobManager.plan_ready(job_id, job_plan)
  ```
  """
  def plan_ready(job_id, job_plan) do
    GenServer.cast(SPE.JobManager, {:planning, {job_id, job_plan}})
  end
end
