defmodule SPE do
  use GenServer
  require Logger

  def init(state) do
    pubsub = Phoenix.PubSub.child_spec(name: SPE.PubSub)
    manager = %{
      id: :manager,
      start: {JobManager, :start_link, [SPE.JobManager, state[:options]]}
    }


    children = [
      pubsub,
      manager
    ]

    Logger.info("[SPE #{inspect(self())}]: Server starting...")
    case Supervisor.start_link(children, strategy: :one_for_one) do
      {:ok, supv} -> {:ok, Map.put(state, :supv, supv)}
      {:error, {:already_started, _}} = error ->
        Logger.error("[SPE #{inspect(self())}]: Server is already started.")
        error
      {:error, {:shutdown, reason}} ->
        Logger.error("[SPE #{inspect(self())}]: One of the child processes is crashing caused by #{inspect(reason)}")
        reason
    end
  end

  def handle_call(request, _from, state) do
    case request do
      {:submit, job_desc} ->
        if (!valid_job?(job_desc)) do
          {:reply, {:error, :invalid_description}, state}
        else
          job_id = make_ref()
          spawn_link(Scheduler.planning(self(), {job_id, job_desc}))
          {:reply, {:ok, job_id}, state}
        end
      {:start, job_id} ->
        {:reply, GenServer.call(SPE.JobManager, {:start, job_id}), state}
      _ ->
        Logger.error("[SPE #{inspect(self())}]: Request did not match any clause...")
        {:reply, {:error, :invalid_request}, state}
    end
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__, %{options: opts, jobs: %{}}, [name: SPE])
  end

  def submit_job(job_desc) do
    GenServer.cast(SPE, {:submit, job_desc})
  end

  def start_job(job_id) do
    GenServer.cast(SPE, {:start, job_id})
  end

  def valid_job?(job) do
    Logger.info("[SPE #{inspect(self())}]: Validating job...")
    with _ <- Logger.debug("[SPE #{inspect(self())}]: Is the description a map?"),
      true <- is_map(job),
      _ <- Logger.debug("[SPE #{inspect(self())}]: Contains fields: name and tasks?"),
      true <- Map.has_key?(job, "name") and Map.has_key?(job, "tasks"),
      _ <- Logger.debug("[SPE #{inspect(self())}]: Is the name a non-empty String?"),
      true <- is_bitstring(job["name"]) and String.length(job["name"]) > 0,
      _ <- Logger.debug("[SPE #{inspect(self())}]: Is the tasks field a list?"),
      true <- is_list(job["tasks"]),
      _ <- Logger.debug("[SPE #{inspect(self())}]: Validating tasks..."),
      nil <- Enum.find(job["tasks"], &(!valid_task(&1))),
      _ <- Logger.debug("[SPE #{inspect(self())}]: Are the tasks names unique?"),
      true <- unique_task_names?(job["tasks"]),
      _ <- Logger.debug("[SPE #{inspect(self())}]: Are the enables field properly set?"),
      true <- valid_enable_tasks?(job["tasks"]) do
        Logger.info("[SPE #{inspect(self())}]: Job validation passed.")
        true
    else
      _ ->
        Logger.error("[SPE #{inspect(self())}]: Job validation failed.")
        false
    end
  end

  defp unique_task_names?(tasks) do
    Logger.info("[SPE #{inspect(self())}]: Checking for unique task names...")
    names = Enum.map(tasks, & &1["name"])
    result = Enum.uniq(names) == names
    if result do
      Logger.info("[SPE #{inspect(self())}]: All task names are unique.")
    else
      Logger.error("[SPE #{inspect(self())}]: Duplicate task names found.")
    end
    result
  end

  defp valid_enable_tasks?(tasks) do
    Logger.info("[SPE #{inspect(self())}]: Validating 'enables' field for tasks...")
    task_names = Enum.map(tasks, & &1["name"])

    result = Enum.all?(tasks, fn task ->
      case Map.get(task, "enables") do
        nil ->
          Logger.info("[SPE #{inspect(self())}]: Task #{inspect(task["name"])} has no 'enables' field.")
          true
        enables when is_tuple(enables) ->
          valid = Enum.all?(Tuple.to_list(enables), &(&1 in task_names))
          if valid do
            Logger.info("[SPE #{inspect(self())}]: Task #{inspect(task["name"])} has valid 'enables' field.")
          else
            Logger.error("[SPE #{inspect(self())}]: Task #{inspect(task["name"])} has invalid 'enables' field.")
          end
          valid
        _ ->
          Logger.error("[SPE #{inspect(self())}]: Task #{inspect(task["name"])} has an invalid 'enables' field type.")
          false
      end
    end)

    if result do
      Logger.info("[SPE #{inspect(self())}]: All tasks have valid 'enables' fields.")
    else
      Logger.error("[SPE #{inspect(self())}]: Some tasks have invalid 'enables' fields.")
    end

    result
  end

  defp valid_task(task) do
    Logger.info("Validating task: #{inspect(task["name"])}")
    with  _ <- Logger.debug("[SPE #{inspect(self())}]: Is the description a map?"),
        true <- is_map(task),
        _ <- Logger.debug("[SPE #{inspect(self())}]: Contains fields: name and exec?"),
         true <- Map.has_key?(task, "name") and Map.has_key?(task, "exec"),
         _ <- Logger.debug("[SPE #{inspect(self())}]: Is the name a non-empty String?"),
         true <- is_bitstring(task["name"]) and String.length(task["name"]) > 0,
         _ <- Logger.debug("[SPE #{inspect(self())}]: Is exec a function with arity 1?"),
         true <- is_function(task["exec"], 1),
         _ <- Logger.debug("[SPE #{inspect(self())}]: Has it timeout? Is it :infinity or a positive integer?"),
         true <-
           (Map.has_key?(task, "timeout") and
           (
             task["timeout"] == :infinity or
             (is_integer(task["timeout"]) and task["timeout"] > 0)
           )) or
           !Map.has_key?(task, "timeout"),
           _ <- Logger.debug("[SPE #{inspect(self())}]: Has it enables? Is it a list?"),
         true <- (Map.has_key?(task, "enables") and is_list(task["enables"])) or !Map.has_key?(task, "timeout") do
      Logger.info("Task validation passed for: #{inspect(task["name"])}")
      true
    else
      _ ->
        Logger.error("Task validation failed for: #{inspect(task["name"])}")
        false
    end
  end
end
