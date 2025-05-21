defmodule Worker do
  use GenServer

  def init(_init_arg) do
    JobManager.worker_ready()
  end

  def start_link(opts) do
    GenServer.start_link(__MODULE__,
      name: Keyword.get(opts, :name, :default),
      name: Keyword.get(opts, :name, :default)
    )

    {:ok, Keyword.get(opts, :name, :default)}
  end

  def send_task(worker_pid, name: name, fn: fun, params: params) do
    GenServer.cast(worker_pid, {:task, name, fun, params})
  end

  def handle_cast({:task, name, fun, params}, _, worker_name) do
    result = apply(fun, params)
    JobManager.task_completed(name, result, worker_name)
    {:noreply, worker_name}
  end
end
