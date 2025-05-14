defmodule SPETask do
  require Logger

  def apply(job_id, task_name, function, args) do
    tick = :erlang.monotonic_time(:millisecond)
    Logger.debug("[SPETask #{inspect(self())}]: Task #{inspect(task_name)} Arguments #{inspect(args)}")

    result =
      try do
        {:result, Kernel.apply(function, args)}
      rescue
        e ->
          Logger.error("[SPETask #{inspect(self())}]: Failing with exception #{inspect(e)}")
          {:failed, e}
      end

    tac = :erlang.monotonic_time(:millisecond)

    Logger.info("[SPETask #{inspect(self())}]: Sending to PubSub message queue #{inspect(result)}")

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
