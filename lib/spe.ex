defmodule SPE do
  use GenServer
  require Logger

  def init({opts, state}) do
    # TODO: opts contains num_workers, pass to JobManager
    pubsub = Phoenix.PubSub.child_spec(name: SPE.PubSub)

    super_manager = %{
      id: :super_manager,
      start: {SuperManager, :start_link, []}
    }

    manager_opts = Keyword.put(opts, :name, SPE.JobManager)
    manager = %{
      id: :manager,
      start: {JobManager, :start_link, [manager_opts]}
    }

    children = [
      SPE.Repo,
      pubsub,
      super_manager,
      manager
    ]

    Logger.info("[SPE #{inspect(self())}]: Server starting...")

    case Supervisor.start_link(children, strategy: :one_for_one) do
      {:ok, supv} ->
        ref = Process.monitor(supv)
        {:ok, Map.put(state, :supv, %{ref: ref, pid: supv})}

      {:error, {:already_started, _}} = error ->
        Logger.error("[SPE #{inspect(self())}]: Server is already started.")
        error

      {:error, {:shutdown, reason}} ->
        Logger.error(
          "[SPE #{inspect(self())}]: One of the child processes is crashing caused by #{inspect(reason)}"
        )

        reason
    end
  end

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

  def handle_info(msg, state) do
    Logger.debug("Generic info")
    Logger.debug("#{inspect(msg)}")
    {:noreply, state}
  end

  def start_link(opts) do
    GenServer.start_link(
      __MODULE__,
      {opts, %{waiting: %{}}},
      name: SPE
    )
  end

  def submit_job(job_desc) do
    if (!Validator.valid_job?(job_desc)) do
      {:error, :invalid_description}
    else
      GenServer.call(SPE, {:submit_job, job_desc})
    end
  end

  def start_job(job_id) do
    GenServer.call(SPE, {:start_job, job_id})
  end

  def job_ready(job_id, response) do
    GenServer.cast(SPE, {:ready_to_start, {job_id, response}})
  end

  def terminate(_reason, state) do
    Supervisor.stop(state[:supv][:pid])
    :ok
  end

 end
