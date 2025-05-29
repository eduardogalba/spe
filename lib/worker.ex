defmodule Worker do
  use GenServer
  require Logger

  def init(state) do
    Logger.debug(("[Worker #{inspect(self())}]: init: Received state #{inspect(state)}"))
    send(state[:job], {:notify_ready, self()})
    Logger.debug(("[Worker #{inspect(self())}]: Entering init"))
    {:ok, state}
  end

  def start_link(state) do
    Logger.debug("[SuperWorker #{inspect(self())}]: Starting Worker...")
    Logger.debug("[SuperWorker #{inspect(self())}]: State is #{inspect(state)}")
    GenServer.start_link(__MODULE__, state)
  end

  def send_task(worker_pid, job_id, name, timeout, fun, params) do
    GenServer.cast(worker_pid, {:task, {job_id, {name, timeout, fun, params}}})
  end

  def handle_cast({:task, {job_id, {name, timeout, fun, params}}}, state) do
    result = apply(job_id, name, timeout, fun, params)
    Job.task_completed(state[:job], {name, result, self()})
    {:noreply, state}
  end

  def handle_call(:are_u_ready, _from, state) do
    {:reply, :ok, state}
  end

  def are_u_ready?(worker_pid) do
    GenServer.call(worker_pid, :are_u_ready)
  end

  def apply(job_id, task_name, timeout, function, args) do
    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_started, task_name}}
    )

    effective_timeout = if !timeout, do: :infinity, else: timeout

    task_fun = fn ->
      try do
        {:result, Kernel.apply(function, [args])}
      rescue
        exception ->
          {:failed, {:crashed, Exception.message(exception)}}
      end
    end

    task = Task.async(task_fun)

    result =
        try do
          Task.await(task, effective_timeout)
        catch
          :exit, {:timeout, _} ->
            {:failed, :timeout}
          :exit, reason ->
            {:failed, reason}
        end

    Logger.debug("[Worker #{inspect(self())}]: Sending to PubSub #{inspect(job_id)} message queue #{inspect(result)}")

    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_terminated, task_name}}
    )

    result
  end

end
