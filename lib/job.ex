defmodule Job do
  @moduledoc """
  **Job** module is responsible for managing the execution of tasks
  in a distributed system. It handles task assignment to workers,
  monitors their completion, and manages the overall state of the job.
  It uses a GenServer to maintain the state and handle asynchronous
  messages related to task execution and worker readiness.

  ## Usage:
  To use this module, you would typically start a job with `Job.start_link/1` and then
  send tasks to it using `Job.task_completed/2` and `Job.worker_ready/2`.
  It is designed to work with a distributed system where tasks can be executed by multiple workers,
  and it handles the complexities of task dependencies and worker availability.
  """
  use GenServer
  require Logger

  @doc """
  Initializes the job state when the GenServer starts.
  This function sets up the initial state of the job, including
  the task returns, ongoing tasks, free workers, results, and the start time.
  It also logs the start of the job for debugging purposes.
  #### Parameters:
  - `state`: The initial state of the job, which includes the job ID and tasks to be executed.
  #### Returns:
  - `{:ok, state}`: Returns the initial state of the job wrapped in an `:ok` tuple.
  #### Example:
  ```elixir
  {:ok, initial_state} = Job.init(%{id: "job_1", tasks: %{"task_1" => %{"exec" => "some_exec", "timeout" => 5000}}})
  ```
  """
  def init(state) do
    Logger.debug("[Job #{inspect(self())}]: Job starting...")
    {:ok, state}
  end

  @doc """
  Starts the job with the given initial state.
  This function initializes the job by setting up the necessary state
  and prepares it to handle tasks and workers. It also logs the start of the
  #### Parameters:
  - `state`: The initial state of the job, which includes the job ID and tasks to be executed.   Returns::
  - `{:ok, pid}`: Returns the PID of the started job process wrapped in an `:ok` tuple.
  ```elixir
  {:ok, job_pid} = Job.start_link(%{id: "job_1", tasks: %{"task_1" => %{"exec" => "some_exec", "timeout" => 5000}
  }})
  ```
  """
  def start_link(state) do
    Logger.debug("[Job #{inspect(self())}]: Starting job...")

    new_state =
      state
      # This also serves for the task arguments
      |> Map.put(:returns, %{})
      |> Map.put(:ongoing_tasks, [])
      |> Map.put(:free_workers, [])
      |> Map.put(:results, %{})
      # Start the timer
      |> Map.put(:time_start, :erlang.monotonic_time(:millisecond))
      |> Map.put(:busy_workers, %{})

    GenServer.start_link(__MODULE__, new_state, [])
  end

  @doc """
  Handles asynchronous messages related to worker readiness and task completion.
  This function processes messages sent to the job, such as when a worker is ready
  to take tasks or when a task has been completed by a worker. It updates the job state
  accordingly and checks if the job should finish based on the current state.
  #### Parameters:
  - `message`: The message to be processed, which can be either a worker readiness notification
    or a task completion notification.
  - `state`: The current state of the job, which includes ongoing tasks, free workers, and results.
  #### Returns:
  - `{:noreply, new_state}`: Returns the updated state of the job wrapped in a `:noreply` tuple.
  """
  def handle_cast({:worker_ready, worker_pid}, state) do
    Logger.debug("[Job #{inspect(self())}]: New worker #{inspect(worker_pid)}...")
    free_workers = state[:free_workers] ++ [worker_pid]
    # If it crashes, I will get a notification
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

    # Logger.debug("[Job #{inspect(self())}]: These are the tasks stil running => #{inspect(new_ongoing_tasks)}")
    Logger.debug(
      "[Job #{inspect(self())}]: Task Name: #{inspect(task_name)} Result: #{inspect(value)}"
    )

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
      # Logger.debug("[Job #{inspect(self())}]: Next plan floor = > #{inspect(state[:plan])}")
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

    Logger.debug(
      "[Job #{inspect(self())}]: Remaining tasks to do #{inspect(new_ongoing_tasks)}..."
    )

    Logger.debug("[Job #{inspect(self())}]: Tasks to disable #{inspect(disable_tasks)}...")

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
          Enum.reduce(disable_tasks, state[:plan], fn d_task, acc_plan ->
            Enum.map(
              acc_plan,
              fn next_tasks ->
                List.delete(next_tasks, d_task)
              end
            )
          end)

        new_plan_cleaned =
          new_plan
          |> Enum.filter(fn sublist -> !Enum.empty?(sublist) end)

        Logger.debug("New plan: #{inspect(new_plan_cleaned)}")

        new_returns =
          Enum.reduce(disable_tasks, state[:returns], fn d_task, acc_returns ->
            Map.put(acc_returns, d_task, :not_run)
          end)
          |> Map.put(task_name, {:failed, reason})

        new_results =
          Enum.reduce(disable_tasks, state[:results], fn d_task, acc_results ->
            Map.put(acc_results, d_task, :not_run)
          end)
          |> Map.put(task_name, {:failed, reason})

        Logger.debug("Tasks done #{inspect(new_returns)}")
        # Update the state with the new values

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

  @doc """
  Handles informational messages related to worker readiness notifications.
  This function processes messages sent to the job when a worker is ready
  to take tasks. It updates the job state by adding the worker to the list of
  free workers and logs the addition of the new worker.
  #### Parameters:
  - `message`: The informational message containing the worker PID.
  - `state`: The current state of the job, which includes ongoing tasks, free workers, and results.
  #### Returns:
  - `{:noreply, state}`: Returns the current state of the job wrapped in a `:noreply` tuple.
  """
  def handle_info({:notify_ready, worker_pid}, state) do
    Logger.debug("[Job #{inspect(self())}]: Adding new worker #{inspect(worker_pid)}...")
    worker_ready(self(), worker_pid)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, pid, reason}, state) do
    if reason != :normal do
      Logger.debug(
        "[Job #{inspect(self())}]: Handling Worker #{inspect(pid)} failing because of #{inspect(reason)}"
      )

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

  @doc """
  Notifies the job that a task has been completed by a worker.
  This function is called when a worker finishes executing a task.
  It sends a message to the job process indicating that the task has been completed,
  along with the task name, result, and worker PID. The job process will then
  handle this message to update its state accordingly.
  #### Parameters:
  - `job_id`: The ID of the job process that is handling the task.
  - `task_name`: The name of the task that has been completed.
  - `result`: The result of the task execution, which can be a success or failure.
  - `worker_pid`: The PID of the worker that completed the task.
  #### Example:
  ```elixir
  Job.task_completed(job_id, {"task_1", {:result, "task_result"}, worker_pid})
  ```
  """
  def task_completed(job_id, {task_name, result, worker_pid}) do
    GenServer.cast(job_id, {:task_terminated, {task_name, result, worker_pid}})
  end

  @doc """
  Notifies the job that a worker is ready to take tasks.
  This function is called when a worker signals that it is ready to receive tasks.
  It sends a message to the job process indicating that the worker is ready,
  allowing the job to assign tasks to the worker as they become available.
  #### Parameters:
  - `job_pid`: The PID of the job process that is managing the workers.
  - `worker_pid`: The PID of the worker that is ready to take tasks.
  #### Example:
  ```elixir
  Job.worker_ready(job_pid, worker_pid)
  ```
  """
  def worker_ready(job_pid, worker_pid) do
    GenServer.cast(job_pid, {:worker_ready, worker_pid})
  end

  defp should_we_finish?(state) do
    case dispatch_tasks(state) do
      :wait ->
        if length(Map.keys(state[:tasks])) == length(Map.keys(state[:results])) do
          Logger.debug("[Job #{inspect(self())}]: Finished all tasks...")
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

          Logger.debug(
            "[Job #{inspect(self())}]: Sending to PubSub message queue #{inspect(state[:id])}"
          )

          Phoenix.PubSub.local_broadcast(
            SPE.PubSub,
            state[:id],
            {
              :spe,
              :erlang.monotonic_time(:millisecond) - state[:time_start],
              {state[:id], :result, {status, state[:results]}}
            }
          )

          {:stop, :normal, state}
        else
          {:noreply, state}
        end

      new_state ->
        Logger.debug("[Job #{inspect(self())}]: Job is not finished yet. Continuing..")
        {:noreply, new_state}
    end
  end

  defp dispatch_tasks(state) do
    case state[:plan] do
      [] ->
        Logger.debug("[Job #{inspect(self())}]: No more tasks left to run...")
        :wait

      [first_tasks | next_tasks] ->
        Logger.debug(
          "[Job #{inspect(self())}]: This is the sequence of tasks #{inspect(state[:plan])}"
        )

        free_workers = state[:free_workers]
        {to_assign, remaining_tasks} = Enum.split(first_tasks, length(free_workers))

        Logger.debug(
          "[Job #{inspect(self())}]: The are these workers #{inspect(free_workers)} available"
        )

        busy_workers =
          Enum.zip(to_assign, free_workers)
          |> Enum.reduce(%{}, fn {task_name, worker_pid}, acc ->
            Map.put(acc, worker_pid, task_name)
          end)

        # Pair tasks and workers
        Enum.zip(to_assign, free_workers)
        |> Enum.each(fn {task_name, worker_pid} ->
          Logger.debug(
            "[Job #{inspect(self())}]: Assigning task #{inspect(task_name)} to worker #{inspect(worker_pid)}"
          )

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

        # The workers that remain free after assignment
        new_free_workers = Enum.drop(free_workers, length(to_assign))

        # Build the new plan: if there are tasks left unassigned, leave them at the beginning
        new_plan =
          if remaining_tasks == [] do
            next_tasks
          else
            [remaining_tasks | next_tasks]
          end

        Logger.debug(
          "[Job #{inspect(self())}]: Remaining tasks after dispatching #{inspect(remaining_tasks)}"
        )

        Logger.debug("[Job #{inspect(self())}]: New sequence of tasks #{inspect(new_plan)}")

        state
        |> Map.put(:plan, new_plan)
        |> Map.put(:ongoing_tasks, state[:ongoing_tasks] ++ to_assign)
        |> Map.put(:free_workers, new_free_workers)
        |> Map.put(:busy_workers, busy_workers)
    end
  end
end
