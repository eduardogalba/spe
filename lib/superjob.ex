defmodule SuperJob do
  use Supervisor
  require Logger

   @impl Supervisor
  def init(children) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]
    Logger.debug("[SuperJob #{inspect(self())}]: Definiendo estrategia...")
    Supervisor.init(children, opts)
  end

  def start_link(job_state, num_workers) do
    Logger.debug("[SuperJob #{inspect(self())}]: Iniciando...")
    {:ok, sup_pid} = Supervisor.start_link(__MODULE__, [])

    job =
      %{
        id: job_state[:id],
        start: {Job, :start_link, [job_state]},
        restart: :transient
      }

    Logger.debug("[SuperJob #{inspect(self())}]: Iniciando Job...")

    {:ok, job_pid} = Supervisor.start_child(sup_pid, job)

    Logger.debug("[SuperJob #{inspect(self())}]: REsultado: #{inspect(job_pid)}")

    super_worker =
      %{
        id: "sw_" <> inspect(job_state[:id]),
        start: {SuperWorker, :start_link, [job_pid, num_workers]},
        restart: :transient
      }

    Logger.debug("[SuperJob #{inspect(self())}]: Iniciando SuperWorker...")
    result = Supervisor.start_child(sup_pid, super_worker)

    Logger.debug("[SuperJob #{inspect(self())}]: Resultado: #{inspect(result)}...")

    {:ok, sup_pid}
  end

end
