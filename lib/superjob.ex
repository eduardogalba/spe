defmodule SuperJob do
  use Supervisor

   @impl Supervisor
  def init(children) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Supervisor.init(children, opts)
  end

  def start_link(job_state, num_workers) do
    job =
      %{
        id: job_state[:id],
        start: {Job, :start_link, [job_state]},
        restart: :transient
      }

    super_task =
      %{
        id: "st_" <> inspect(job_state[:id]),
        start: {SuperTask, :start_link, [[num_workers: num_workers]]},
        restart: :transient
      }

    children = [
      job,
      super_task
    ]

    Supervisor.start_link(__MODULE__, [children], name: SPE.SuperJob)
  end

end
