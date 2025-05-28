defmodule Job do
  use GenServer
  require Logger

  def init(state) do
    Logger.debug("[Job #{inspect(self())}]: Job starting...")
    {:ok, state}
  end

  def start_link(state) do
    Logger.debug("Iniciando trabajo...")

    new_state =
      state
      |> Map.put(:returns, Map.get(state, :returns, %{})) # Esto tambien sirve para los argumentos de las tareas
      |> Map.put(:ongoing_tasks, [])
      |> Map.put(:free_workers, [])
      |> Map.put(:results, Map.get(state, :results, %{}))
      |> Map.put(:time_start, :erlang.monotonic_time(:millisecond)) # Empieza el cronometro
      |> Map.put(:busy_workers, %{})

    GenServer.start_link(__MODULE__, new_state, [])
  end

  def handle_cast({:worker_ready, worker_pid}, state) do
    Logger.debug("[Job #{inspect(self())}]: Nuevo worker #{inspect(worker_pid)}...")
    free_workers = state[:free_workers] ++ [worker_pid]
    # Si se cae, me llegara una notificacion
    Process.monitor(worker_pid)
    Worker.are_u_ready?(worker_pid)
    new_state =
      state
      |> Map.put(:free_workers, free_workers)

    should_we_finish?(new_state)
  end

  def handle_cast({:task_terminated, {task_name, {:result, value}, worker_pid}}, state) do
    Logger.debug("[Job #{inspect(self())}]: Handling task #{inspect(task_name)} ending...")
    new_ongoing_tasks = List.delete(state[:ongoing_tasks], task_name)
    #Logger.debug("[Job #{inspect(self())}]: These are the tasks stil running => #{inspect(new_ongoing_tasks)}")
    Logger.debug("[Job #{inspect(self())}]: Task Name: #{inspect(task_name)} Result: #{inspect(value)}")
    new_returns =
      state[:returns]
      |> Map.put(task_name, value)

    new_results =
      state[:results]
      |> Map.put(task_name, {:result, value})

    new_state =
      state
      |> Map.put(:returns, new_returns)
      |> Map.put(:ongoing_tasks, new_ongoing_tasks)
      |> Map.put(:results, new_results)
      |> Map.put(:free_workers, state[:free_workers] ++ [worker_pid])
      |> Map.put(:busy_workers, Map.delete(state[:busy_workers], worker_pid))

    if new_ongoing_tasks == [] and Map.keys(state[:tasks]) do
      #Logger.debug("[Job #{inspect(self())}]: Next plan floor = > #{inspect(state[:plan])}")
      should_we_finish?(new_state)
    else
      {:noreply, new_state}
    end
  end

  def handle_cast({:task_terminated, {task_name, {:failed, reason}, worker_pid}}, state) do
    Logger.debug("[Job #{inspect(self())}]: Handling task #{inspect(task_name)} failing...")
    new_ongoing_tasks = List.delete(state[:ongoing_tasks], task_name)
    disable_tasks = Planner.find_dependent_tasks(state[:enables], task_name)
    new_worker_pid =
      case worker_pid do
        {:fallen_worker, pid} -> pid
        pid -> pid
      end
    new_free_workers =
      case worker_pid do
        {:fallen_worker, _pid} -> []
        pid -> [pid]
      end
    Logger.debug("[Job #{inspect(self())}]: Que falta por hacer #{inspect(new_ongoing_tasks)}...")
    Logger.debug("[Job #{inspect(self())}]: Tareas a deshabilitar #{inspect(disable_tasks)}...")

    Logger.debug("[Job #{inspect(self())}]: Results => #{inspect(state[:results])}")
    Logger.debug("[Job #{inspect(self())}]: Returns => #{inspect(state[:returns])}")

    new_state =
      if disable_tasks == [] do
        new_returns = Map.put(state[:returns], task_name, {:failed, reason})
        new_results = Map.put(state[:results], task_name, {:failed, reason})

        state
        |> Map.put(:ongoing_tasks, new_ongoing_tasks)
        |> Map.put(:returns, new_returns)
        |> Map.put(:results, new_results)
        |> Map.put(:busy_workers, Map.delete(state[:busy_workers], new_worker_pid))
        |> Map.put(:free_workers, state[:free_workers] ++ new_free_workers)

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

        Logger.debug("Nuevo plan: #{inspect(new_plan_cleaned)}")

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

        Logger.debug("Tareas hechas #{inspect(new_returns)}")
        # Actualiza el estado con los nuevos valores

        state
        |> Map.put(:returns, new_returns)
        |> Map.put(:ongoing_tasks, new_ongoing_tasks)
        |> Map.put(:plan, new_plan_cleaned)
        |> Map.put(:results, new_results)
        |> Map.put(:free_workers, state[:free_workers] ++ new_free_workers)
        |> Map.put(:busy_workers, Map.delete(state[:busy_workers], new_worker_pid))

      end

    should_we_finish?(new_state)
  end

  def handle_info({:notify_ready, worker_pid}, state) do
    Logger.debug("[Job #{inspect(self())}]: Adding new worker #{inspect(worker_pid)}...")
    worker_ready(self(), worker_pid)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do

    if reason != :normal do
      Logger.debug("[Job #{inspect(self())}]: Handling Worker #{inspect(pid)} failing because of #{inspect(reason)}")
      task_name = Map.get(state[:busy_workers], pid)
      Logger.debug("[Job #{inspect(self())}]: Previous task being done #{inspect(task_name)}")

      task_completed(self(), {task_name, {:failed, reason}, {:fallen_worker, pid}})
      new_free_workers = List.delete(state[:free_workers], pid)
      new_state =
        state
        |> Map.put(:free_workers, new_free_workers)

      {:noreply, new_state}
    else
      {:noreply, state}
    end

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
        if (length(Map.keys(state[:tasks])) == length(Map.keys(state[:results]))) do
          Logger.info("[Job #{inspect(self())}]: Finished all tasks...")
          Logger.debug("[Job #{inspect(self())}]: Results #{inspect(state[:results])}")
          failed =
            state[:results]
            |> Map.values()
            |> Enum.any?(fn result ->
                case result do
                  {:failed, _reason} -> true
                  _ -> false
                end
              end)

          status = if failed, do: :failed, else: :succeeded
          Logger.debug("[Job #{inspect(self())}]: Sending to PubSub message queue #{inspect(state[:id])}")

          Phoenix.PubSub.local_broadcast(
            SPE.PubSub,
            state[:id],
            {
              :spe,
              :erlang.monotonic_time(:millisecond) - state[:time_start],
              {state[:id], :result, {status, state[:results]}}
            }
          )

          JobRepository.delete_job(state[:name])

          {:stop, :normal, state}
        else
          JobRepository.save_job_state(state)
          {:noreply, state}
        end

      new_state ->
        Logger.debug("[Job #{inspect(self())}]: Job is not finished yet. Conitnuing..")
        JobRepository.save_job_state(state)
        {:noreply, new_state}
    end
  end

  defp dispatch_tasks(state) do
    case state[:plan] do
      [] ->
        Logger.debug("[Job #{inspect(self())}]: No more tasks left to run...")
        :wait

      [first_tasks | next_tasks] ->

        Logger.debug("[Job #{inspect(self())}]: This is the sequence of tasks #{inspect(state[:plan])}")
        free_workers = state[:free_workers]
        {to_assign, remaining_tasks} = Enum.split(first_tasks, length(free_workers))

        Logger.debug("[Job #{inspect(self())}]: The are these workers #{inspect(free_workers)} available")
        busy_workers =
          Enum.zip(to_assign, free_workers)
          |> Enum.reduce(%{}, fn {task_name, worker_pid}, acc ->
            Map.put(acc, worker_pid, task_name)
          end)

        # Empareja tareas y workers
        Enum.zip(to_assign, free_workers)
        |> Enum.each(fn {task_name, worker_pid} ->
          Logger.debug("[Job #{inspect(self())}]: Assigning task #{inspect(task_name)} to worker #{inspect(worker_pid)}")
          Worker.send_task(
            worker_pid,
            state[:id],
            task_name,
            state[:tasks][task_name]["timeout"],
            state[:tasks][task_name]["exec"],
            state[:returns]
          )
        end)
        Logger.debug("[Job #{inspect(self())}]: Starting tasks #{inspect(to_assign)}")


        # Los workers que quedan libres después de asignar
        new_free_workers = Enum.drop(free_workers, length(to_assign))

        # Construye el nuevo plan: si quedan tareas sin asignar, las dejas al principio
        new_plan =
          if remaining_tasks == [] do
            next_tasks
          else
            [remaining_tasks | next_tasks]
          end

        Logger.debug("[Job #{inspect(self())}]: Remaining tasks after dispatching #{inspect(remaining_tasks)}")
        Logger.debug("[Job #{inspect(self())}]: New sequence of tasks #{inspect(new_plan)}")

        state
          |> Map.put(:plan, new_plan)
          |> Map.put(:ongoing_tasks, state[:ongoing_tasks] ++ to_assign)
          |> Map.put(:free_workers, new_free_workers)
          |> Map.put(:busy_workers, busy_workers)

      end
  end

end
