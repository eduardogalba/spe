defmodule Planner do
  require IEx

  def planning(caller, {job_id, job_desc}, num_workers) do
    tasks = job_desc["tasks"]
    # Preparing Kahn´s algorithm


    enabled_names =
      List.foldr(tasks, [], fn task, acc -> [Tuple.to_list(Map.get(task, "enables", {})) | acc] end)
      |> Enum.uniq()
      |> List.foldr([], fn enables, acc ->
        if (enable = List.first(enables)), do: [enable|acc], else: acc
      end)

      # Extracting nodes with no incoming edges
    free_tasks = List.foldr(tasks, [], fn task, acc ->
      if (task["name"] not in enabled_names), do: [task["name"] | acc], else: acc
    end)

    # Extracting dependencies, edges to remove
    tasks_dependencies =
      List.foldr(tasks, %{}, fn task, acc ->
        task_name = task["name"]

        dependent_tasks =
          tasks
          |> Enum.filter(fn t -> task_name in Tuple.to_list(Map.get(t, "enables", {})) end)
          |> Enum.map(& &1["name"])

        if (dependent_tasks == []), do: acc, else: Map.put(acc, task_name, dependent_tasks)
      end)

    planned = khan_loop(tasks_dependencies, free_tasks, [])
    plan = group_tasks(planned, tasks_dependencies, num_workers)


    send(caller, {:planning, {job_id, plan, tasks_dependencies}})

  end

  def find_next_independent([], _deps, _done), do: nil
  def find_next_independent([task | rest], deps, undone, done) do
    # PRE: La lista de tareas es flatten, nada de listas dentro de listas
    task_deps = Map.get(deps, task)

    if !task_deps or (Enum.all?(task_deps, &(&1 in done)) and !Enum.any?(task_deps, &(&1 in undone))) do
      task
    else
      find_next_independent(rest, deps, undone, done)
    end
  end

  def complete_task([], _task, new_tasks), do: new_tasks
  def complete_task([task_level | rest], task, acc) do
    if task in task_level do
      complete_task(rest, task, acc ++ [List.delete(task_level, task)])
    else
      complete_task(rest, task, acc ++ [task_level])
    end
  end

  def group_tasks([], _deps, _num_workers), do: []
  def group_tasks(tasks, deps, num_workers) do
    {group, rest} = take_independent(tasks, deps, num_workers, [], [])
    [group | group_tasks(rest, deps, num_workers)]
  end

  defp take_independent([], _deps, _nworkers, task_floor, _rechazados), do: {Enum.reverse(task_floor), []}
  defp take_independent(tasks, _deps, 0, task_floor, _rechazados), do: {Enum.reverse(task_floor), tasks}
  defp take_independent([t | ts], deps, nworkers, task_floor, rechazados) do

    cond do
      Enum.any?(rechazados, fn a -> t in Map.get(deps, a, []) or a in Map.get(deps, t, []) end) ->
        # Pasamos de añadir nils innecesario la lista son de tamaño maximo num_workers
        {taken, remaining} = take_independent(ts, deps, nworkers - 1, task_floor, rechazados)
        # Selecciona otro y postpone esta tarea
        {taken, [t | remaining]}

      Enum.any?(task_floor, fn a -> t in Map.get(deps, a, []) or a in Map.get(deps, t, []) end) ->
        # Pasamos de añadir nils innecesario la lista son de tamaño maximo num_workers
        {taken, remaining} = take_independent(ts, deps, nworkers - 1, task_floor, [t | rechazados])
        # Selecciona otro y postpone esta tarea
        {taken, [t | remaining]}

      true -> take_independent(ts, deps, nworkers - 1, [t | task_floor], rechazados)
    end

  end

  def khan_loop(dependencies, free_tasks, planned) do
    if (Enum.empty?(free_tasks)) do
      if (!Enum.empty?(dependencies)) do
        {:error, :graph_has_cycle}
      else
        planned
      end
    else
      IO.puts("Free Tasks: #{inspect(free_tasks)} Deps: #{inspect(dependencies)} Planned: #{inspect(planned)}")
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
