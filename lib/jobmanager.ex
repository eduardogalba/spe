defmodule JobManager do
  use GenServer
  require Logger

  def init(state) do
    {:ok, Map.put(state, :waiting, [])}
  end

  def handle_call({:submit, job_desc}, _from, state) do
    job_id = make_ref()

    new_jobs =
      state[:jobs]
      |> Map.put(job_id, %{desc: job_desc, plan: nil, enables: %{}, num_workers: state[:num_workers]})

    spawn_link(Planner, :planning, [job_id, job_desc, state[:num_workers]])
    {:reply, {:ok, job_id}, Map.put(state, :jobs, new_jobs)}
  end

  def handle_call({:start, job_id}, _from, state) do
    if !Map.has_key?(state[:jobs], job_id) do
      {:reply, {:error, :unregistered_job}, state} ## OJO! No para aqui
    end

    if !state[:jobs][job_id][:plan] do
      Logger.debug("[SPE #{inspect(self())}]: The plan is not ready yet. Saving client pid")
      # Si no esta listo se lo anota y le contesta cuando la tenga
      new_waiting = state[:waiting] ++ [job_id]

      new_state = Map.put(state, :waiting, new_waiting)

      {:reply, {:warn, :wait_until_plan},  new_state}
    else
      job = Map.put(state[:jobs][job_id], :id, job_id)
      case SuperJob.start_job(job) do
        {:ok, _} -> {:reply, :ok, state}
        any -> {:reply, any, state}
      end
    end
  end


  def handle_cast({:planning, {job_id, job_plan}}, state) do
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

    if Enum.member?(new_state[:waiting], job_id) do
      Logger.debug("[SPE #{inspect(self())}]: Replying client waiting...")

      new_waiting = List.delete(state[:waiting], job_id)

      Logger.debug("[SPE #{inspect(self())}]: After replying #{inspect(new_state)}")
      job = Map.put(new_state[:jobs][job_id], :id, job_id)
      Logger.info("Creando trabajo: #{inspect(job)}")
      SPE.job_ready(job_id, SuperJob.start_job(job))

      {:noreply, Map.put(state, :waiting, new_waiting)}
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
