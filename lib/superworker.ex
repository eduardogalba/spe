defmodule SuperWorker do
  use Supervisor
  require Logger

  @impl Supervisor
  def init(info) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Logger.debug("[SuperWorker #{inspect(self())}]: Iniciando workers...")
    Logger.debug("[SuperWorker #{inspect(self())}]: init: Me llega en info: #{inspect(info)}.")
    num_workers = if (info[:num_workers] == :unbound), do: 1, else: info[:num_workers]

    workers =
        for i <- 1..num_workers do
          %{
            id: :"worker_#{i}",
            start: {Worker, :start_link, [%{job: info[:job_pid]}]}
          }
        end


    Supervisor.init(workers, opts)
  end

  def start_link(job_pid, num_workers) do
    Logger.debug("[SuperWorker #{inspect(self())}]: Me llega en start_link: #{inspect(job_pid)}")
    info =
      %{
        job_pid: job_pid,
        num_workers: num_workers
      }

    Supervisor.start_link(__MODULE__, info, [])
  end

end
