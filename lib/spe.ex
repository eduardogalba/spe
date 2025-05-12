defmodule SPE do
  use GenServer

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

    case Supervisor.start_link(children, strategy: :one_for_one) do
      {:ok, supv} -> {:ok, Map.put(state, :supv, supv)}
      {:error, {:already_started, _}} = error ->
        error
      {:error, {:shutdown, reason}} ->
        reason
    end
  end

  def handle_call(request, from, state) do

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
