defmodule SPE do
  use GenServer
  require Logger

  def init(state) do
    pubsub = Phoenix.PubSub.child_spec(name: SPE.PubSub)
    manager = %{
      id: :manager,
      start: {SuperJob, :start_link, [[name: SuperJob]]}
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

  def handle_call(request, {caller, _}, state) do
    case request do
      {:submit, job_desc} ->
        if (!valid_job?(job_desc)) do
          {:reply, {:error, :invalid_description}, state}
        else
          job_id = make_ref()
          new_jobs =
            state[:jobs]
            |> Map.put(job_id, %{desc: job_desc, plan: nil, deps: %{}, num_workers: state[:num_workers]})

          spawn_link(Planner, :planning, [self(), {job_id, job_desc}, state[:num_workers]])
          {:reply, {:ok, job_id}, Map.put(state, :jobs, new_jobs)}
        end
      {:start, job_id} ->
        if (!Map.has_key?(state[:jobs], job_id)) do
          {:reply, {:error, :unregistered_job}, state}
        end
        if (!state[:jobs][job_id][:plan]) do
          Logger.debug("[SPE #{inspect(self())}]: The plan is not ready yet. Saving client pid")
          # Si no esta listo se lo anota y le contesta cuando la tenga
          new_state =
            update_in(
              state[:waiting],
              fn waiting ->
                Map.put(waiting, job_id, caller)
              end
            )
          {:noreply, new_state}
        else
          job = Map.put(state[:jobs][job_id], :id, job_id)
          case SuperJob.start_job(job) do
            {:ok, _} -> {:reply, :ok, state}
            any -> {:reply, any, state}
          end

        end
      _ ->
        Logger.error("[SPE #{inspect(self())}]: Request did not match any clause...")
        {:reply, {:error, :invalid_request}, state}
    end
  end

  def handle_info({:planning, {job_id, job_plan, deps}}, state) do
    Logger.debug("[SPE #{inspect(self())}]: Receiving plan for #{inspect(job_id)}...")
    tasks =
      Enum.into(state[:desc]["tasks"], %{}, fn task_desc ->
        {task_desc["name"], task_desc}
      end)

    new_state =
      update_in(state[:jobs][job_id], fn job ->Map.put(job, :plan, job_plan) end)
      |> Map.put(:deps, deps)
      |> Map.delete(:desc) # La descripcion entera es innecesaria
      |> Map.put(:tasks, tasks)

    # De momento, no se manejan posibles errores
    # Un posible error es querer iniciar un trabajo no registrado
    if (Map.has_key?(state[:waiting], job_id)) do
      Logger.debug("[SPE #{inspect(self())}]: Replying client waiting...")

      new_state =
        update_in(
          new_state[:waiting],
          fn clients ->
            Map.delete(clients, job_id)
          end
        )
      Logger.debug("[SPE #{inspect(self())}]: After replying #{inspect(new_state)}")
      job = Map.put(state[:jobs][job_id], :id, job_id)
      case SuperJob.start_job(job) do
            {:ok, _} -> GenServer.reply(state[:waiting][job_id], :ok)
            any -> GenServer.reply(state[:waiting][job_id], any)
      end
      {:noreply, new_state}
    else
      {:noreply, new_state}
    end

  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def start_link(opts) do
    if (Keyword.has_key?(opts, :num_workers)) do
      GenServer.start_link(
        __MODULE__,
        %{num_workers: Keyword.get(opts, :num_workers), jobs: %{}, waiting: %{}},
        [name: SPE]
      )
    else
      GenServer.start_link(
        __MODULE__,
        %{num_workers: :unbound, jobs: %{}, waiting: %{}},
        [name: SPE]
      )
    end

  end

  def submit_job(job_desc) do
    GenServer.call(SPE, {:submit, job_desc})
  end

  def start_job(job_id) do
    # De momento para los tests
    GenServer.call(SPE, {:start, job_id}, :infinity)
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
