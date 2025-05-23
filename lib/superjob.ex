defmodule SuperJob do
  use Supervisor

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
    Supervisor.start_link(__MODULE__, [], name: SPE.SuperJob)
  end

  def start_job(job_state) do
    child_spec =
      %{
        id: job_state[:id],
        start: {Job, :start_link, [job_state]},
        restart: :transient
      }

    Supervisor.start_child(SPE.SuperJob, child_spec)
  end

end
