defmodule Job do
  use GenServer
  require Logger

  def init(state) do
    # Asumo que plan es correcto y viene como una lista de listas [[]],
    # cada uno de los niveles de tamaño máximo num_workers representa
    # las tareas a ejecutar por instante de tiempo
    Phoenix.PubSub.subscribe(SPE.PubSub, "#{inspect(state[:id])}:reports")
    Logger.info("[Job #{inspect(self())}]: Job starting...")
    case start_tasks(state) do
      :stop -> {:stop, {:error, :empty_task_plan}}
      :not_matched -> {:stop, {:error, :wrong_plan_format}}
      new_state -> {:ok, new_state}
    end

  end

  def start_link(state) do
    # Asumo que me debe llegar por los args, mi id como trabajo, el plan a seguir en una lista,
    # y la descripción del trabajo.

    # Quiero solo las descripciones de tarea como un mapa y no toda la descripción teniendo
    # las tareas como una lista

    tasks =
      Enum.into(state[:desc]["tasks"], %{}, fn task_desc ->
        {task_desc["name"], task_desc}
      end)

    new_state =
      state
      |> Map.delete(:desc)
      |> Map.put(:tasks, tasks)
      |> Map.put(:done, %{})
      |> Map.put(:undone, [])
      |> Map.put(:time_start, :erlang.monotonic_time(:millisecond))

    GenServer.start_link(__MODULE__, new_state, [])
  end

  def handle_info({:task_terminated, {task_name, result}}, state) do
    case result do
      {:failed, _} ->
        Logger.info("[Job #{inspect(self())}]: Handling task failing...")
        new_undone = List.delete(state[:undone], task_name)
        disable_tasks =
          case state[:tasks][task_name]["enables"] do
            nil -> []
            _ -> Tuple.to_list(state[:tasks][task_name]["enables"])
          end

        if disable_tasks == [] do
          new_done = Map.put(state[:done], task_name, result)
          new_state =
            state
            |> Map.put(:undone, new_undone)
            |> Map.put(:done, new_done)

          {:noreply, new_state}
        else

          new_plan =
            Enum.reduce(
              disable_tasks,
              fn d_task ->
                Enum.map(
                  state[:plan],
                  fn next_tasks ->
                    List.delete(next_tasks, d_task)
                  end
                )
              end
            )

          IO.puts("Nuevo plan: #{inspect(new_plan)}")

          new_done =
            Enum.reduce(
              disable_tasks,
              fn d_task ->
                Map.put(state[:done], d_task, :not_run)
              end
            )
            |> Map.put(task_name, result)

          IO.puts("Tareas hechas #{inspect(new_done)}")
          # Actualiza el estado con los nuevos valores
          new_state =
            state
            |> Map.put(:done, new_done)
            |> Map.put(:undone, new_undone)
            |> Map.put(:plan, new_plan)

          {:noreply, new_state}
    end

      {:result, value} ->
        Logger.info("[Job #{inspect(self())}]: Handling task ending...")
        new_undone = List.delete(state[:undone], task_name)
        new_done =
          state[:done]
          |> Map.put(task_name, value)

        new_state =
          state
          |> Map.put(:done, new_done)
          |> Map.put(:undone, new_undone)

        case start_tasks(new_state) do
          :stop ->
            Logger.info("[Job #{inspect(self())}]: Finished all tasks...")
            failed =
              Enum.any?(new_state[:done], fn result ->
                case result do
                  {:failed, _reason} -> true
                  _ -> false
                end
              end)

            status = if failed, do: :failed, else: :suceeded

            Phoenix.PubSub.local_broadcast(
              SPE.PubSub,
              "#{inspect(new_state[:id])}",
              {
                :spe,
                :erlang.monotonic_time(:millisecond) - new_state[:time_start],
                {new_state[:id],:result, {status, new_state[:done]}}}
            )

            {:stop, :normal, new_state}

          :not_matched ->
            {:stop, {:error, :wrong_plan_format}}

          final_state ->
            Logger.info("[Job #{inspect(self())}]: Next state...")
            {:noreply, final_state}
        end

    end
  end

  # Ignoro cualquier otro mensaje
  def handle_info(_, state) do
    {:noreply, state}
  end

  defp start_tasks(state) do
    case state[:plan] do
      [] ->
        Logger.info("[Job #{inspect(self())}]: No more tasks left to run...")
        :stop

      [first_tasks | next_tasks] ->
        Logger.info("[Job #{inspect(self())}]: Starting tasks #{inspect(first_tasks)}")
        job_id = state[:id]
        # Olvidamos Supervisor por ahora, las primeras tareas no requieren args por eso nil
        Enum.each(
          first_tasks,
          fn task ->
            case task do
              [] -> nil
              task_name -> spawn_link(SPETask, :apply, [job_id, task_name, state[:tasks][task_name]["exec"], [state[:done]]])
            end
          end
        )

        state
          |> Map.put(:plan, next_tasks)
          |> Map.put(:undone, first_tasks)

      _ ->
        Logger.error("[Job #{inspect(self())}]: Something is strange in tasks plan")
        :not_matched
      end
  end

end
