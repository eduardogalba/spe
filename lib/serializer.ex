defmodule Serializer do
  require Logger
  def serialize_job(state) do
    Logger.debug("Serializing job #{inspect(state)}")
    %{
      name: name,
      plan: plan,
      results: results,
      returns: returns,
      enables: enables,
      num_workers: num_workers,
      tasks: tasks
    } = state

    serializable_results =
      Enum.into(results, %{}, fn {k, v} -> {k, serialize_result_value(v)} end)

    serializable_returns =
      Enum.into(returns, %{}, fn {k, v} -> {k, serialize_return_value(v)} end)

    %{
      name: name,
      plan: %{list: plan},
      results: serializable_results,
      returns: serializable_returns,
      enables: enables,
      num_workers: num_workers,
      tasks: Map.new(tasks, fn {name, task_desc} -> {name, Map.drop(task_desc, ["exec"])} end)
    }
  end

  def deserialize_job(job) do
    deserializable_results =
      Enum.into(job.results, %{}, fn {k, v} -> {k, deserialize_result_value(v)} end)
    deserializable_returns =
      Enum.into(job.returns, %{}, fn {k, v} -> {k, deserialize_return_value(v)} end)

    deserializable_job =
      %{
          name: job.name,
          plan: job.plan["list"],
          results: deserializable_results,
          returns: deserializable_returns,
          enables: job.enables,
          num_workers: job.num_workers,
          tasks: job.tasks
        }
    Logger.debug("Deserializing job #{inspect(deserializable_job)}")
    deserializable_job
  end

  defp serialize_return_value({:failed, {:crashed, reason}}), do: [:failed, [:crashed, reason]]
  defp serialize_return_value(value) when is_tuple(value), do: Tuple.to_list(value)
  defp serialize_return_value(value), do: value

  defp deserialize_return_value(["failed", ["crashed", reason]]), do: {:failed, {:crashed, reason}}
  defp deserialize_return_value(value) when is_list(value), do: List.to_tuple(value)
  defp deserialize_return_value(value), do: value

  defp serialize_result_value(:not_run), do: [:not_run]
  defp serialize_result_value({:result, value}), do: [:result, value]
  defp serialize_result_value({:failed, {:crashed, reason}}), do: [:failed, [:crashed, reason]]
  defp serialize_result_value({:failed, reason}) when is_tuple(reason), do: [:failed, Tuple.to_list(reason)]
  defp serialize_result_value({:failed, reason}) do [:failed, reason] end


  defp deserialize_result_value(["not_run"]), do: {:not_run}
  defp deserialize_result_value(["result", value]), do: {:result, value}
  defp deserialize_result_value(["failed", ["crashed", reason]]), do: {:failed, {:crashed, reason}}
  defp deserialize_result_value(["failed", reason]) when is_list(reason), do: {:failed, List.to_tuple(reason)}
  defp deserialize_result_value(["failed", reason]) do {:failed, reason} end

end
