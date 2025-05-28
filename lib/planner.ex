defmodule Planner do
  require Logger

  def planning(job_desc, num_workers) do
    tasks = job_desc["tasks"]
    # Preparing Kahn´s algorithm


    enabled_names =
      List.foldr(tasks, [], fn task, acc -> [Map.get(task, "enables", []) | acc] end)
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
          |> Enum.filter(fn t -> task_name in Map.get(t, "enables", []) end)
          |> Enum.map(& &1["name"])

        if (dependent_tasks == []), do: acc, else: Map.put(acc, task_name, dependent_tasks)
      end)

    planned = khan_loop(tasks_dependencies, free_tasks, [])
    priority = Map.get(job_desc, "priority", [])
    case planned do
      {:ok, plan} ->
        eff_num_workers = if num_workers == :unbound, do: length(plan), else: num_workers
        # OJO AQUI se podria ver si en la descripcion tiene algo asi como priority y elegir
        # otra funcion que tenga en cuenta las prioridades de las tareas, en vez de usar,
        # group_tasks
        if priority != [] do
          {:ok, group_tasks_with_priority(plan, tasks_dependencies, eff_num_workers, priority)}
        else
          {:ok, group_tasks(plan, tasks_dependencies, eff_num_workers)}
        end
      {:error, error} ->
        {:error, error}
    end


  end

  def planning_task_description(task_desc, num_workers) do
    tasks = Map.values(task_desc)

    enabled_names =
      tasks
      |> Enum.flat_map(&Map.get(&1, "enables", []))
      |> Enum.uniq()

    # Extraer tareas sin dependencias entrantes
    free_tasks =
      tasks
      |> Enum.filter(fn task -> not (task["name"] in enabled_names) end)
      |> Enum.map(& &1["name"])

    # Extraer dependencias
    tasks_dependencies =
      tasks
      |> Enum.reduce(%{}, fn task, acc ->
        task_name = task["name"]

        dependent_tasks =
          tasks
          |> Enum.filter(fn t -> task_name in Map.get(t, "enables", []) end)
          |> Enum.map(& &1["name"])

        if dependent_tasks == [], do: acc, else: Map.put(acc, task_name, dependent_tasks)
      end)

    planned = khan_loop(tasks_dependencies, free_tasks, [])

    case planned do
      {:ok, plan} ->
        eff_num_workers = if num_workers == :unbound, do: length(plan), else: num_workers
        {:ok, group_tasks(plan, tasks_dependencies, eff_num_workers)}
      {:error, error} ->
        {:error, error}
    end
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

  def find_dependent_tasks(enables, start_task) do
    find_dependent_tasks_(enables, [start_task], [])
  end

  defp find_dependent_tasks_(_enables, [], acc), do: acc
  defp find_dependent_tasks_(enables, [task | rest], acc) do
    dependent_tasks = Map.get(enables, task, [])

    new_tasks = Enum.filter(dependent_tasks, &(!Enum.member?(acc, &1)))

    find_dependent_tasks_(enables, rest ++ new_tasks, acc ++ new_tasks)
  end


  def group_tasks_with_priority(tasks, deps, num_workers, priority) do
    group_tasks_with_priority_(tasks, deps, num_workers, priority, [])
  end

  defp group_tasks_with_priority_([], _deps, _num_workers, _priority, acc), do: Enum.reverse(acc)
  defp group_tasks_with_priority_(tasks, deps, num_workers, priority, acc) do
    # Separa tareas prioritarias y no prioritarias
    {prio_tasks, other_tasks} = Enum.split_with(tasks, &(&1 in priority))

    # Intenta meter primero las prioritarias independientes
    {group, rest_prio} = take_independent(prio_tasks, deps, num_workers, [], [])
    n_left = num_workers - length(group)

    # Si queda hueco, rellena con no prioritarias independientes
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
