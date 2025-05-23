defmodule SPETask do
  require Logger

  def apply(job_id, task_name, timeout, function, args) do
    tick = :erlang.monotonic_time(:millisecond)
    effective_timeout = if !timeout, do: :infinity, else: timeout
    Logger.debug("[SPETask #{inspect(self())}]: Task #{inspect(task_name)} Arguments #{inspect(args)}")

    task_fun = fn ->
      try do
        Kernel.apply(function, args)
      rescue
        exception ->
          Logger.debug("[#{inspect(task_name)}]: Capturada excepción en el hijo: #{inspect(exception)}")
          {:failed, {:crashed, Exception.message(exception)}}
      catch
        kind, reason ->
          Logger.debug("[#{inspect(task_name)}]: Capturado catch en el hijo: #{inspect(kind)}, #{inspect(reason)}")
          {:failed, {:crashed, reason}}
      end
    end

    task = Task.async(task_fun)

    Logger.debug("[SPETask #{inspect(self())}]: Primera linea de defensa atravesada")

    result =
        case Task.await(task, effective_timeout) do
          nil ->
            Logger.debug("No se ha completado la tarea a tiempo.")
            {:failed, :timeout}
          res = {:failed, _} -> res
          result -> {:result, result}
        end


    tac = :erlang.monotonic_time(:millisecond)

    Logger.debug("[SPETask #{inspect(self())}]: Sending to PubSub message queue #{inspect(result)}")

    # Comunicación a Job de que ha terminado
    Phoenix.PubSub.broadcast(
      SPE.PubSub,
      "#{inspect(job_id)}:reports",
      {:task_terminated, {task_name, result}}
    )

    # Comunicación a todos de las tareas terminadas
    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      "#{inspect(job_id)}",
      {:spe, tac - tick, {job_id, :task_terminated, task_name}}
    )

  end

end
