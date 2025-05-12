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

  def handle_call(request, from, state) do
    case request do
      {:submit, job_desc} ->
        if (!valid_job?(job_desc)) do
          {:reply, {:error, :invalid_description}, state}
        else
          {:reply, GenServer.call(SPE.JobManager, {:submit, job_desc}), state}
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
end
