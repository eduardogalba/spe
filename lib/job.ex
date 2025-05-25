defmodule Job do
  use GenServer
  require Logger

  def init(state) do
    Logger.info("[Job #{inspect(self())}]: Job starting...")
    {:ok, state}
  end

  def start_link(state) do
    Logger.debug("Iniciando trabajo...")

    new_state =
      state
      |> Map.put(:returns, %{}) # Esto tambien sirve para los argumentos de las tareas
      |> Map.put(:pending_tasks, [])
      |> Map.put(:free_workers, [])
      |> Map.put(:results, %{})
      |> Map.put(:time_start, :erlang.monotonic_time(:millisecond)) # Empieza el cronometro

    GenServer.start_link(__MODULE__, new_state, [])
  end

  def handle_cast({:worker_ready, worker_pid}, state) do
    Logger.info("[Job #{inspect(self())}]: Nuevo worker #{inspect(worker_pid)}...")
    free_workers = state[:free_workers] ++ [worker_pid]
    new_state =
      state
      |> Map.put(:free_workers, free_workers)

    should_we_finish?(new_state)
  end

  def handle_cast({:task_terminated, {task_name, {:result, value}, worker_pid}}, state) do
    Logger.info("[Job #{inspect(self())}]: Handling task ending...")
    new_pending_tasks = List.delete(state[:pending_tasks], task_name)
    Logger.info("[Job #{inspect(self())}]: These are the tasks stil running => #{inspect(new_pending_tasks)}")
    new_returns =
      state[:returns]
      |> Map.put(task_name, value)

    new_results =
      state[:results]
      |> Map.put(task_name, {:result, value})

    new_state =
      state
      |> Map.put(:returns, new_returns)
      |> Map.put(:pending_tasks, new_pending_tasks)
      |> Map.put(:results, new_results)
      |> Map.put(:free_workers, state[:free_workers] ++ [worker_pid])

    if new_pending_tasks == [] and Map.keys(state[:tasks]) do
      Logger.info("[Job #{inspect(self())}]: Let´s continue with the plan...")
      should_we_finish?(new_state)
    else
      {:noreply, new_state}
    end
  end

  def handle_cast({:task_terminated, {task_name, {:failed, reason}, worker_pid}}, state) do
    Logger.info("[Job #{inspect(self())}]: Handling task failing...")
    new_pending_tasks = List.delete(state[:pending_tasks], task_name)
    disable_tasks = Planner.find_dependent_tasks(state[:enables], task_name)
    Logger.info("[Job #{inspect(self())}]: Que falta por hacer #{inspect(new_pending_tasks)}...")
    Logger.info("[Job #{inspect(self())}]: Tareas a deshabilitar #{inspect(disable_tasks)}...")

    new_state =
      if disable_tasks == [] do
        new_returns = Map.put(state[:returns], task_name, {:failed, reason})

        state
        |> Map.put(:pending_tasks, new_pending_tasks)
        |> Map.put(:returns, new_returns)
      else

        new_plan =
          Enum.reduce(disable_tasks, state[:plan],
            fn d_task, acc_plan ->
              Enum.map(acc_plan,
                fn next_tasks ->
                  List.delete(next_tasks, d_task)
                end
          ) end)

        new_plan_cleaned =
          new_plan
          |> Enum.filter(fn sublist -> !Enum.empty?(sublist) end)

        IO.puts("Nuevo plan: #{inspect(new_plan_cleaned)}")

        new_returns =
          Enum.reduce(disable_tasks, state[:returns],
            fn d_task, acc_returns ->
              Map.put(acc_returns, d_task, :not_run)
            end)
          |> Map.put(task_name, {:failed, reason})

        new_results =
          Enum.reduce(disable_tasks, state[:results],
            fn d_task, acc_results ->
              Map.put(acc_results, d_task, :not_run)
            end)
          |> Map.put(task_name, {:failed, reason})

        IO.puts("Tareas hechas #{inspect(new_returns)}")
        # Actualiza el estado con los nuevos valores

        state
        |> Map.put(:returns, new_returns)
        |> Map.put(:pending_tasks, new_pending_tasks)
        |> Map.put(:plan, new_plan_cleaned)
        |> Map.put(:results, new_results)
        |> Map.put(:free_workers, state[:free_workers] ++ [worker_pid])
      end

    should_we_finish?(new_state)
  end

  def handle_info({:notify_ready, worker_pid}, state) do
    Logger.info("[Job #{inspect(self())}]: Nuevo worker #{inspect(worker_pid)}...")
    free_workers = state[:free_workers] ++ [worker_pid]
    new_state =
      state
      |> Map.put(:free_workers, free_workers)

    worker_ready(self(), worker_pid)
    {:noreply, new_state}
  end

  def task_completed(job_id, {task_name, result, worker_pid}) do
    GenServer.cast(job_id, {:task_terminated, {task_name, result, worker_pid}})
  end

  def worker_ready(job_pid, worker_pid) do
    GenServer.cast(job_pid, {:worker_ready, worker_pid})
  end

  defp should_we_finish?(state) do
    case dispatch_tasks(state) do
      :wait ->
        if (length(Map.keys(state[:tasks])) == length(Map.keys(state[:returns]))) do
          Logger.info("[Job #{inspect(self())}]: Finished all tasks...")
          Logger.info("[Job #{inspect(self())}]: Resultados #{inspect(state[:results])}")
          failed =
            state[:results]
            |> Map.values()
            |> Enum.any?(fn result ->
                case result do
                  {:failed, _reason} -> true
                  _ -> false
                end
              end)

          status = if failed, do: :failed, else: :suceeded

          Phoenix.PubSub.local_broadcast(
            SPE.PubSub,
            "#{inspect(state[:id])}",
            {
              :spe,
              :erlang.monotonic_time(:millisecond) - state[:time_start],
              {state[:id],:result, {status, state[:results]}}
            }
          )

          {:stop, :normal, state}
        else
          {:noreply, state}
        end

      new_state ->
        Logger.info("[Job #{inspect(self())}]: Next state...")
        {:noreply, new_state}
    end
  end

  defp dispatch_tasks(state) do
    case state[:plan] do
      [] ->
        Logger.info("[Job #{inspect(self())}]: No more tasks left to run...")
        :wait

      [first_tasks | next_tasks] ->

        Logger.info("[Job #{inspect(self())}]: Esto queda en el plan #{inspect(state[:plan])}")
        free_workers = state[:free_workers]
        {to_assign, remaining_tasks} = Enum.split(first_tasks, length(free_workers))


        # Empareja tareas y workers
        Enum.zip(to_assign, free_workers)
        |> Enum.each(fn {task_name, worker_pid} ->
          Worker.send_task(
            worker_pid,
            task_name,
            state[:tasks][task_name]["timeout"],
            state[:tasks][task_name]["exec"],
            state[:returns]
          )
        end)
        Logger.info("[Job #{inspect(self())}]: Starting tasks #{inspect(to_assign)}")


        # Los workers que quedan libres después de asignar
        new_free_workers = Enum.drop(free_workers, length(to_assign))

        # Construye el nuevo plan: si quedan tareas sin asignar, las dejas al principio
        new_plan =
          if remaining_tasks == [] do
            next_tasks
          else
            [remaining_tasks | next_tasks]
          end

        Logger.info("[Job #{inspect(self())}]: Tareas restantes #{inspect(remaining_tasks)}")
        Logger.info("[Job #{inspect(self())}]: Nuevo plan #{inspect(new_plan)}")

        state
          |> Map.put(:plan, new_plan)
          |> Map.put(:pending_tasks, state[:pending_tasks] ++ to_assign)
          |> Map.put(:free_workers, new_free_workers)

      end
  end

end
