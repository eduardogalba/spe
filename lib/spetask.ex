defmodule SPETask do

  def apply(job_id, task_name, function, args) do
    tick = :erlang.monotonic_time(:millisecond)
    IO.puts("Argumentos #{inspect(args)}")
    result =
      try do
        {:result, Kernel.apply(function, args)}
      rescue
        e ->
          IO.puts("Fallando con #{inspect(e)}")
          {:failed, e}
      end

    tac = :erlang.monotonic_time(:millisecond)

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
