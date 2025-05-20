defmodule JobManager do
  use DynamicSupervisor

  @impl DynamicSupervisor
  def init(_init_arg) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Supervisor.init([], opts);
  end

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, [], opts)
  end

  def start_job(job_state) do
    child_spec = [
      %{
        id: job_state[:id],
        start: {Job, :start_link, [job_state]},
        restart: :transient,
        type: :worker
      }
    ]

    Supervisor.start_child(__MODULE__, child_spec)
  end



end
