defmodule SuperManager do
  use Supervisor
  require Logger

  @impl Supervisor
  def init(_init_arg) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Supervisor.init([], opts)
  end

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: SPE.SuperManager)
  end

  def start_job(job_state, num_workers) do

    superjob =
      %{
        id: job_state[:id],
        start: {SuperJob, :start_link, [job_state, num_workers]},
        restart: :transient
      }

    Logger.debug("[SuperManager #{inspect(self())}]: Starting SuperJob...")
    Supervisor.start_child(SPE.SuperManager, superjob)
  end
end
