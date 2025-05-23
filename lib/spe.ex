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

  def handle_call(request, from, state) do
    case request do
      {:submit, job_desc} ->
        if (!Validator.valid_job?(job_desc)) do
          {:reply, {:error, :invalid_description}, state}
        else
          job_id = make_ref()
          new_jobs =
            state[:jobs]
            |> Map.put(job_id, %{desc: job_desc, plan: nil, enables: %{}, num_workers: state[:num_workers]})

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
                Map.put(waiting, job_id, from)
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

  def handle_info({:planning, {job_id, job_plan}}, state) do
    Logger.debug("[SPE #{inspect(self())}]: Receiving plan for #{inspect(job_id)}...")
    Logger.debug("[SPE #{inspect(self())}]: Tengo en state #{inspect(state)}")

    tasks =
      Enum.reduce(
        state[:jobs][job_id][:desc]["tasks"],
        %{},
        fn task_desc, acc ->
          Map.put(acc, task_desc["name"], task_desc)
        end)
      Logger.debug("[SPE #{inspect(self())}]: Tengo en tasks #{inspect(tasks)}")

    enables =
      Enum.reduce(tasks,
        %{},
        fn  {task_name, desc} , acc ->
          if (desc["enables"]) do
            Map.put(acc, task_name, Tuple.to_list(desc["enables"]))
          else
            acc
          end
      end)

    Logger.debug("[SPE #{inspect(self())}]: Tengo en enables #{inspect(enables)}")

    new_job =
      Map.put(state[:jobs][job_id], :plan, job_plan)
      |> Map.put(:enables, enables)
      |> Map.delete(:desc) # La descripcion entera es innecesaria
      |> Map.put(:tasks, tasks)

    new_state = update_in(state[:jobs], fn jobs ->Map.put(jobs, job_id, new_job) end)

    Logger.debug("[SPE #{inspect(self())}]: Tengo nuevo state #{inspect(new_state)}")

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
      job = Map.put(new_state[:jobs][job_id], :id, job_id)
      Logger.info("Creando trabajo: #{inspect(job)}")
      case SuperJob.start_job(job) do
            {:ok, _} -> GenServer.reply(state[:waiting][job_id], :ok)
            any -> GenServer.reply(state[:waiting][job_id], any)
      end
      {:noreply, new_state}
    else
      {:noreply, new_state}
    end

  end

  def handle_info(msg, state) do
    Logger.info("Info generico")
    Logger.info("#{inspect(msg)}")
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
    GenServer.call(SPE, {:start, job_id})
  end

end
