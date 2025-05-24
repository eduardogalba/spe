defmodule Worker do
  use GenServer
  require Logger

  def init(state) do
    Logger.debug(("[Worker #{inspect(self())}]: init: Me llega en state #{inspect(state)}"))
    send(state[:job], {:notify_ready, self()})
    Logger.debug(("[Worker #{inspect(self())}]: Entrando a init"))
    {:ok, state}
  end

  def start_link(state) do
    Logger.debug("[SuperWorker #{inspect(self())}]: Iniciando Worker...")
    Logger.debug("[SuperWorker #{inspect(self())}]: Esto tengo en mi estado #{inspect(state)}")
    GenServer.start_link(__MODULE__, state)
  end

  def send_task(worker_pid, name, timeout, fun, params) do
    GenServer.cast(worker_pid, {:task, name, timeout, fun, params})
  end

  def handle_cast({:task, name, timeout, fun, params}, state) do
    result = apply(state[:job], name, timeout, fun, params)
    Job.task_completed(state[:job], {name, result, self()})
    {:noreply, state}
  end

  def apply(job_id, task_name, timeout, function, args) do
    tick = :erlang.monotonic_time(:millisecond)
    effective_timeout = if !timeout, do: :infinity, else: timeout
    Logger.debug("[Worker #{inspect(self())}]: Task #{inspect(task_name)} Arguments #{inspect(args)}")

    task_fun = fn ->
      try do
        Kernel.apply(function, [args])
      rescue
        exception ->
          Logger.debug("[#{inspect(task_name)}]: Capturada excepción en el hijo: #{inspect(exception)}")
          Logger.error("#{inspect(__STACKTRACE__)}")
          {:failed, {:crashed, Exception.message(exception)}}
      catch
        kind, reason ->
          Logger.debug("[#{inspect(task_name)}]: Capturado catch en el hijo: #{inspect(kind)}, #{inspect(reason)}")
          {:failed, {:crashed, reason}}
      end
    end

    task = Task.async(task_fun)

    Logger.debug("[Worker #{inspect(self())}]: Primera linea de defensa atravesada")

    result =
        try do
          {:result, Task.await(task, effective_timeout)}
        catch
          :exit, {:timeout, _} ->
            Logger.debug("Task.await ha hecho timeout.")
            {:failed, :timeout}
        end


    tac = :erlang.monotonic_time(:millisecond)

    Logger.debug("[Worker #{inspect(self())}]: Sending to PubSub message queue #{inspect(result)}")

    # Comunicación a todos de las tareas terminadas
    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      "#{inspect(job_id)}",
      {:spe, tac - tick, {job_id, :task_terminated, task_name}}
    )

    result
  end

end
