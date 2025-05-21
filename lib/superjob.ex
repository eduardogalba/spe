defmodule SuperJob do
  use Supervisor

  @impl Supervisor
  def init(info) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    workers =
      if(Map.get(info, :num_workers, :unbound) != :unbound) do
        for i <- 1..Map.get(info, :num_workers, 1) do
          %{
            id: :"worker_#{i}",
            start: {Worker, :start_link, [[name: :"worker_#{i}"]]}
          }
        end
      else
        []
      end

    Supervisor.init(workers, opts)
  end

  def start_link(opts) do
    Supervisor.start_link(
      __MODULE__,
      %{
        num_workers: Keyword.get(opts, :num_workers, :unbound),
        free_workers: :queue.new(),
        processing_jobs: %{}
      },
      name: JobManager
    )
  end

  def start_job(job_state) do
    child_spec =
      %{
        id: job_state[:id],
        start: {Job, :start_link, [job_state]},
        restart: :transient
      }

    Supervisor.start_child(__MODULE__, child_spec)
  end

  def worker_ready() do
    GenServer.call(JobManager, {:ready, {Process.info(self())[:refgistered_name], self()}})
  end

  def task_completed(name, result, worker_name) do
    GenServer.call(JobManager, {:ended_task, name, result, worker_name})
  end

  def handle_call({:ready, {name, pid}}, _, state) do
    :queue.in(%{name: name, pid: pid}, state[:free_workers])
    {:reply, :ok}
  end

  def handle_call({:ended_task, name, result, worker_name}) do
    ## Maneja la tarea
  end
end
