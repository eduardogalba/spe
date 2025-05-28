defmodule Serializer do
  require Logger
  def serialize_job(state) do
    Logger.info("Serializing job #{inspect(state)}")
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

    %{
      name: name,
      plan: %{list: plan},
      results: serializable_results,
      returns: returns,
      enables: enables,
      num_workers: num_workers,
      tasks: Map.new(tasks, fn {name, task_desc} -> {name, Map.drop(task_desc, ["exec"])} end)
    }
  end

  def deserialize_job(job) do
    deserializable_results =
      Enum.into(job.results, %{}, fn {k, v} -> {k, deserialize_result_value(v)} end)
    deserializable_job =
      %{
          name: job.name,
          plan: job.plan["list"],
          results: deserializable_results,
          returns: job.returns,
          enables: job.enables,
          num_workers: job.num_workers,
          tasks: job.tasks
        }
    Logger.info("Deserializing job #{inspect(deserializable_job)}")
    deserializable_job
  end

  defp serialize_result_value(:not_run), do: [:not_run]
  defp serialize_result_value({:result, value}), do: [:result, value]
  defp serialize_result_value({:failed, :timeout}), do: [:failed, :timeout]
  defp serialize_result_value({:failed, {:crashed, reason}}), do: [:failed, [:crashed, reason]]

  defp deserialize_result_value(["not_run"]), do: {:not_run}
  defp deserialize_result_value(["result", value]), do: {:result, value}
  defp deserialize_result_value(["failed", "timeout"]), do: {:failed, :timeout}
  defp deserialize_result_value(["failed", ["crashed", reason]]), do: {:failed, {:crashed, reason}}
end
