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

  defp group_tasks([], _deps, _num_workers), do: []
  defp group_tasks(tasks, deps, num_workers) do
    {group, rest} = take_independent(tasks, deps, num_workers, [])
    filled_group = group ++ List.duplicate(nil, num_workers - length(group))
    [filled_group | group_tasks(rest, deps, num_workers)]
  end

  defp take_independent([], _deps, _nworkers, acc), do: {Enum.reverse(acc), []}
  defp take_independent(tasks, _deps, 0, acc), do: {Enum.reverse(acc), tasks}
  defp take_independent([t | ts], deps, nworkers, acc) do
    if Enum.any?(acc, fn a -> t in Map.get(deps, a, []) or a in Map.get(deps, t, []) end) do
      # Si es dependiente de alguna de la lista rellena con nil y sigue buscando
      {taken, remaining} = take_independent(ts, deps, nworkers, [nil |acc])
      # Selecciona otro y postpone esta tarea
      {taken, [t | remaining]}
    else
      # Si no dependiente la inserta
      take_independent(ts, deps, nworkers - 1, [t | acc])
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
