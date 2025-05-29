defmodule JobManager do
  use GenServer
  require Logger

  def init(state) do
    {:ok, state}
  end

  def handle_call({:submit, job_desc}, _from, state) do
    job_id = "#{inspect(make_ref())}"

    recovered_job = JobRepository.get_job_state(job_desc["name"])
    case recovered_job do
      {:error, :not_found} ->
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
                end)

            enables =
              Enum.reduce(tasks,
                %{},
                fn  {task_name, desc} , acc ->
                  if (desc["enables"]) do
                    Map.put(acc, task_name, desc["enables"])
                  else
                    acc
                  end
              end)

            # If num_workers is not defined, we choose the concurrency level that may need more workers.
            # Changing strategy: creating a process is expensive and takes time.
            # If this maximum is less than the one proposed by the user, it is ignored.
            maximum_concurrent_tasks = new_plan |> Enum.map(&length/1) |> Enum.max()
            num_workers =
              if state[:num_workers] == :unbound or maximum_concurrent_tasks < state[:num_workers] do
                maximum_concurrent_tasks
              else
                state[:num_workers]
              end

            Logger.debug("[JobManager #{inspect(self())}]: For Job #{inspect(job_id)} will run maximum #{inspect(num_workers)} tasks.")
            Logger.debug("[JobManager #{inspect(self())}]: For Job #{inspect(job_id)} the plan is #{inspect(new_plan)}")

            Logger.debug("[JobManager #{inspect(self())}]: Enables contains #{inspect(enables)}")

            new_job = %{id: job_id, plan: new_plan, enables: enables, num_workers: num_workers, tasks: tasks, name: job_desc["name"]}

            new_jobs =
              state[:jobs]
              |> Map.put(job_id, new_job)

            {:reply, {:ok, job_id}, Map.put(state, :jobs, new_jobs)}

          {:error, error} ->
            {:reply, {:error, error}, state}
        end

      {:ok, job} ->
        Logger.info("Recovering job after server crash")
        # Here it needs to recover what was saved but adjust the plan
        # because there may be tasks that were not completed and will no longer be part of the plan
        completed_tasks = Map.keys(job.results)
        task_desc = Map.drop(job.tasks, completed_tasks)

        plan = Planner.planning_task_description(task_desc, job.num_workers)

        case plan do
          {:ok, new_plan} ->
            Logger.info("A new recovery plan is calculated #{inspect(new_plan)}")
            maximum_concurrent_tasks = new_plan |> Enum.map(&length/1) |> Enum.max()
            num_workers =
              if state[:num_workers] == :unbound or maximum_concurrent_tasks < state[:num_workers] do
                maximum_concurrent_tasks
              else
                state[:num_workers]
              end

            tasks =
              Enum.reduce(
                job_desc["tasks"],
                %{},
                fn task_desc, acc ->
                  Map.put(acc, task_desc["name"], task_desc)
                end)

            new_job =
              job
              |> Map.put(:id, job_id)
              |> Map.put(:num_workers, num_workers)
              |> Map.put(:plan, new_plan)
              |> Map.put(:tasks, tasks)

            new_jobs =
              state[:jobs]
              |> Map.put(job_id, new_job)

            {:reply, {:ok, job_id}, Map.put(state, :jobs, new_jobs)}

          {:error, error} ->
            {:reply, {:error, error}, state}
        end
    end

  end

  def handle_call({:start, job_id}, _from, state) do
    if !Map.has_key?(state[:jobs], job_id) do
      {:reply, {:error, :unregistered_job}, state}
    else
      job = state[:jobs][job_id]
      case SuperManager.start_job(job, job[:num_workers]) do
        {:ok, _} -> {:reply, {:ok, job_id}, state}
        any -> {:reply, any, state}
      end
    end
  end

  def handle_info(msg, state) do
    Logger.debug("Generic info")
    Logger.debug("#{inspect(msg)}")
    {:noreply, state}
  end

  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      %{num_workers: Keyword.get(opts, :num_workers, :unbound), jobs: %{}, waiting: %{}},
      name: Keyword.get(opts, :name, SPE.JobManager)
    )

  end

  def submit_job(job_desc) do
    GenServer.call(SPE.JobManager, {:submit, job_desc})
  end

  def start_job(job_id) do
    GenServer.call(SPE.JobManager, {:start, job_id})
  end

  def plan_ready(job_id, job_plan) do
    GenServer.cast(SPE.JobManager, {:planning, {job_id, job_plan}})
  end
end
