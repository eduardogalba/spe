defmodule JobManager do
  use GenServer
  require Logger

  def init(state) do
    {:ok, state}
  end

  def handle_call({:submit, job_desc}, _from, state) do
    job_id = "#{inspect(make_ref())}"
    new_plan = Planner.planning(job_desc, state[:num_workers])

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

    Logger.debug("[JobManager #{inspect(self())}]: For Job #{inspect(job_id)} will run maximum #{inspect(num_workers)} tasks.")
    Logger.debug("[JobManager #{inspect(self())}]: For Job #{inspect(job_id)} the plan is #{inspect(new_plan)}")

    Logger.debug("[JobManager #{inspect(self())}]: Tengo en enables #{inspect(enables)}")

    new_job = %{id: job_id, plan: new_plan, enables: enables, num_workers: num_workers, tasks: tasks}

    new_jobs =
      state[:jobs]
      |> Map.put(job_id, new_job)

    {:reply, {:ok, job_id}, Map.put(state, :jobs, new_jobs)}
  end

  def handle_call({:start, job_id}, _from, state) do
    if !Map.has_key?(state[:jobs], job_id) do
      {:reply, {:error, :unregistered_job}, state} ## OJO! No para aqui
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

  def handle_info(msg, state) do
    Logger.debug("Info generico")
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
