defmodule Planner do
  @moduledoc """
  **Planner** is responsible for planning jobs based on their descriptions and the number of workers available.
  It implements Kahn's algorithm to create a plan for executing tasks while respecting their dependencies.
  ## Usage:
  - Call `Planner.planning/2` with a job description and the number of workers to get a planned execution order.
  ## Example:
  ```elixir
  job_desc = %{
    "name" => "nisse",
    "tasks" => [
      %{
        "name" => "t0",
        "enables" => [],
        "exec" => fn _ -> 1 + 2 end,
        "timeout" => :infinity
      },
      %{
        "name" => "t1",
        "enables" => ["t0"],
        "exec" => fn _ -> 3 + 4 end,
        "timeout" => :infinity
      }
    ]
  }
  {:ok, plan} = Planner.planning(job_desc, 2)
  ```
  ## Notes:
  - The job description should contain a list of tasks, each with a name and a list of tasks it enables.
  - The `planning/2` function will return an ordered list of task groups that can be executed in parallel, respecting their dependencies.
  - If the job description contains a "priority" field, it will be used to prioritize tasks in the planning.
  """
  require Logger

  @doc """
  Plans a job based on its description and the number of workers available.
  It uses Kahn's algorithm to create a plan for executing tasks while respecting their dependencies.
  #### Parameters:
  - `job_desc`: A map containing the job description, including tasks and their dependencies.
  - `num_workers`: The number of workers available for executing the tasks. It can be an integer or `:unbound` for dynamic allocation.
  #### Returns:
  - `{:ok, plan}`: A list of task groups that can be executed in parallel, respecting their dependencies.
  - `{:error, reason}`: If the job description is invalid or if there is a cycle in the task dependencies.
  #### Example:
  ```elixir
  job_desc = %{
    "name" => "nisse",
    "tasks" => [
      %{
        "name" => "t0",
        "enables" => [],
        "exec" => fn _ -> 1 + 2 end,
        "timeout" => :infinity
      },
      %{
        "name" => "t1",
        "enables" => ["t0"],
        "exec" => fn _ -> 3 + 4 end,
        "timeout" => :infinity
      }
    ]
  }
  {:ok, plan} = Planner.planning(job_desc, 2)
  ```
  """
  def planning(job_desc, num_workers) do
    tasks = job_desc["tasks"]
    # Preparing Kahn's algorithm

    enabled_names =
      List.foldr(tasks, [], fn task, acc -> [Map.get(task, "enables", []) | acc] end)
      |> Enum.uniq()
      |> List.foldr([], fn enables, acc ->
        if enable = List.first(enables), do: [enable | acc], else: acc
      end)

    # Extracting nodes with no incoming edges
    free_tasks =
      List.foldr(tasks, [], fn task, acc ->
        if task["name"] not in enabled_names, do: [task["name"] | acc], else: acc
      end)

    # Extracting dependencies, edges to remove
    tasks_dependencies =
      List.foldr(tasks, %{}, fn task, acc ->
        task_name = task["name"]

        dependent_tasks =
          tasks
          |> Enum.filter(fn t -> task_name in Map.get(t, "enables", []) end)
          |> Enum.map(& &1["name"])

        if dependent_tasks == [], do: acc, else: Map.put(acc, task_name, dependent_tasks)
      end)

    planned = khan_loop(tasks_dependencies, free_tasks, [])
    priority = Map.get(job_desc, "priority", [])

    case planned do
      {:ok, plan} ->
        eff_num_workers = if num_workers == :unbound, do: length(plan), else: num_workers
        # NOTE: Here you could check if the description has something like priority and choose
        # another function that takes task priorities into account, instead of using group_tasks
        if priority != [] do
          {:ok, group_tasks_with_priority(plan, tasks_dependencies, eff_num_workers, priority)}
        else
          {:ok, group_tasks(plan, tasks_dependencies, eff_num_workers)}
        end

      {:error, error} ->
        {:error, error}
    end
  end

  def find_next_independent([], _deps, _done), do: nil

  @doc """
  Finds the next independent task from a list of tasks, given their dependencies and the already completed tasks.
  #### Parameters:
  - `tasks`: A list of tasks to check for independence.
  - `deps`: A map where keys are task names and values are lists of dependencies for each task.
  - `undone`: A list of tasks that are yet to be completed.
  - `done`: A list of tasks that have already been completed.
  #### Returns:
  - The first independent task found in the list, or `nil` if no independent task is found.
  #### Example:
  ```elixir
  tasks = ["task1", "task2", "task3"]
  deps = %{"task1" => [], "task2" => ["task1"], "task3" => ["task1", "task2"]}
  undone = ["task2", "task3"]
  done = ["task1"]
  next_task = Planner.find_next_independent(tasks, deps, undone, done)
  ```
  """
  def find_next_independent([task | rest], deps, undone, done) do
    # PRE: The task list is flat, no lists within lists
    task_deps = Map.get(deps, task)

    if !task_deps or
         (Enum.all?(task_deps, &(&1 in done)) and !Enum.any?(task_deps, &(&1 in undone))) do
      task
    else
      find_next_independent(rest, deps, undone, done)
    end
  end

  @doc """
  Finds all tasks that depend on a given start task, including transitive dependencies.
  #### Parameters:
  - `enables`: A map where keys are task names and values are lists of tasks that the key task enables.
  - `start_task`: The task from which to start finding dependent tasks.
  #### Returns:
  - A list of all tasks that depend on the `start_task`, including transitive dependencies.
  #### Example:
  ```elixir
  enables = %{
    "task1" => ["task2", "task3"],
    "task2" => ["task4"],
    "task3" => [],
    "task4" => []
  }
  start_task = "task1"
  dependent_tasks = Planner.find_dependent_tasks(enables, start_task)
  ```
  """
  def find_dependent_tasks(enables, start_task) do
    find_dependent_tasks_(enables, [start_task], [])
  end

  defp find_dependent_tasks_(_enables, [], acc), do: acc

  defp find_dependent_tasks_(enables, [task | rest], acc) do
    dependent_tasks = Map.get(enables, task, [])

    new_tasks = Enum.filter(dependent_tasks, &(!Enum.member?(acc, &1)))

    find_dependent_tasks_(enables, rest ++ new_tasks, acc ++ new_tasks)
  end

  @doc """
  Groups tasks into independent sets, considering task dependencies and worker limits.
  #### Parameters:
  - `tasks`: A list of tasks to be grouped.
  - `deps`: A map where keys are task names and values are lists of dependencies for each task.
  - `num_workers`: The maximum number of workers available for executing tasks. If `:unbound`, it allows dynamic allocation.
  - `priority`: A list of tasks that should be prioritized in the grouping.
  #### Returns:
  - A list of lists, where each inner list contains tasks that can be executed in parallel, respecting their dependencies and priorities.
  #### Example:
  ```elixir
  tasks = ["task1", "task2", "task3"]
  deps = %{"task1" => [], "task2" => ["task1"], "task3" => ["task1", "task2"]}
  num_workers = 2
  priority = ["task1"]
  grouped_tasks = Planner.group_tasks_with_priority(tasks, deps, num_workers, priority)
  ```
  """
  def group_tasks_with_priority(tasks, deps, num_workers, priority) do
    group_tasks_with_priority_(tasks, deps, num_workers, priority, [])
  end

  defp group_tasks_with_priority_([], _deps, _num_workers, _priority, acc), do: Enum.reverse(acc)

  defp group_tasks_with_priority_(tasks, deps, num_workers, priority, acc) do
    # Separate priority and non-priority tasks
    {prio_tasks, other_tasks} = Enum.split_with(tasks, &(&1 in priority))

    # Try to add independent priority tasks first
    {group, rest_prio} = take_independent(prio_tasks, deps, num_workers, [], [])
    n_left = num_workers - length(group)

    # If there is space left, fill with independent non-priority tasks
    {group2, rest_others} =
      if n_left > 0 do
        take_independent(other_tasks, deps, n_left, [], [])
      else
        {[], other_tasks}
      end

    new_group = group ++ group2
    rest = rest_prio ++ rest_others

    group_tasks_with_priority_(rest, deps, num_workers, priority, [new_group | acc])
  end

  @doc """
  Groups tasks into independent sets, considering task dependencies and worker limits.
  #### Parameters:
  - `tasks`: A list of tasks to be grouped.
  - `deps`: A map where keys are task names and values are lists of dependencies for each task.
  - `num_workers`: The maximum number of workers available for executing tasks. If `:unbound`, it allows dynamic allocation.
  #### Returns:
  - A list of lists, where each inner list contains tasks that can be executed in parallel, respecting their dependencies.
  #### Example:
  ```elixir
  tasks = ["task1", "task2", "task3"]
  deps = %{"task1" => [], "task2" => ["task1"], "task3" => ["task1", "task2"]}
  num_workers = 2
  grouped_tasks = Planner.group_tasks(tasks, deps, num_workers)
  ```
  """
  def group_tasks([], _deps, _num_workers), do: []

  def group_tasks(tasks, deps, num_workers) do
    {group, rest} = take_independent(tasks, deps, num_workers, [], [])
    [group | group_tasks(rest, deps, num_workers)]
  end

  defp take_independent([], _deps, _nworkers, task_floor, _rechazados),
    do: {Enum.reverse(task_floor), []}

  defp take_independent(tasks, _deps, 0, task_floor, _rechazados),
    do: {Enum.reverse(task_floor), tasks}

  defp take_independent([t | ts], deps, nworkers, task_floor, rechazados) do
    cond do
      Enum.any?(rechazados, fn a -> t in Map.get(deps, a, []) or a in Map.get(deps, t, []) end) ->
        # We avoid adding unnecessary nils, the lists are of maximum size num_workers
        {taken, remaining} = take_independent(ts, deps, nworkers - 1, task_floor, rechazados)
        # Select another and postpone this task
        {taken, [t | remaining]}

      Enum.any?(task_floor, fn a -> t in Map.get(deps, a, []) or a in Map.get(deps, t, []) end) ->
        # We avoid adding unnecessary nils, the lists are of maximum size num_workers
        {taken, remaining} =
          take_independent(ts, deps, nworkers - 1, task_floor, [t | rechazados])

        # Select another and postpone this task
        {taken, [t | remaining]}

      true ->
        take_independent(ts, deps, nworkers - 1, [t | task_floor], rechazados)
    end
  end

  @doc """
  Implements Kahn's algorithm to find a topological ordering of tasks based on their dependencies.
  #### Parameters:
  - `dependencies`: A map where keys are task names and values are lists of tasks that depend on the key task.
  - `free_tasks`: A list of tasks that have no dependencies and can be executed immediately.
  - `planned`: A list that accumulates the planned tasks in the order they can be executed.
  #### Returns:
  - `{:ok, planned}`: If a valid topological ordering is found, returning the ordered list of tasks.
  - `{:error, :graph_has_cycle}`: If a cycle is detected in the task dependencies, making it impossible to create a valid ordering.
  #### Example:
  ```elixir
  dependencies = %{
    "task1" => ["task2"],
    "task2" => [],
    "task3" => ["task1"]
  }
  free_tasks = ["task2"]
  planned = []
  {:ok, ordered_tasks} = Planner.khan_loop(dependencies, free_tasks, planned)
  ```
  """
  def khan_loop(dependencies, free_tasks, planned) do
    if Enum.empty?(free_tasks) do
      if !Enum.empty?(dependencies) do
        {:error, :graph_has_cycle}
      else
        {:ok, planned}
      end
    else
      [n | _] = free_tasks

      edges =
        Enum.reduce(dependencies, %{}, fn {task_name, deps}, acc ->
          Map.put(acc, task_name, List.delete(deps, n))
        end)

      empty_keys =
        Enum.reduce(edges, [], fn {key, value}, acc ->
          if value == [], do: [key | acc], else: acc
        end)

      clean_edges =
        Enum.reduce(edges, %{}, fn {key, value}, acc ->
          if key in empty_keys, do: acc, else: Map.put(acc, key, value)
        end)

      new_free_tasks = (free_tasks |> List.delete(n)) ++ empty_keys
      khan_loop(clean_edges, new_free_tasks, planned ++ [n])
    end
  end
end
