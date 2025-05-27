defmodule JobRepository do
  import Ecto.Query

  def save_job_state(job_id, state) do
    %{
      plan: plan,
      results: results,
      returns: returns,
      enables: enables,
      num_workers: num_workers,
      tasks: tasks
    } = state

    completed_tasks =
      results
      |> Enum.filter(fn {_, status} -> status != :not_run end)
      |> Enum.map(fn {task, _} -> task end)

    job_data = %{
      id: job_id,
      plan: Jason.encode!(plan),
      completed_tasks: completed_tasks,
      num_workers: num_workers,
      enables: Jason.encode!(enables),
      returns: Jason.encode!(returns),
      results: Jason.encode!(results),
      tasks: Jason.encode!(tasks)
    }

    job_struct = SPE.Schemas.Job |> struct(job_data)

    SPE.Repo.insert!(
      job_struct,
      on_conflict: :replace_all,
      conflict_target: :id
    )
  end

  def get_job_state(job_id) do
    case SPE.Repo.get(SPE.Schemas.Job, job_id) do
      nil -> {:error, :not_found}
      job ->
        {:ok, %{
          plan: Jason.decode!(job.plan),
          results: Jason.decode!(job.results),
          returns: Jason.decode!(job.returns),
          enables: Jason.decode!(job.enables),
          num_workers: job.num_workers,
          tasks: Jason.decode!(job.tasks)
        }}
    end
  end

  def delete_job(job_id) do
    from(j in SPE.Schemas.Job, where: j.id == ^job_id)
    |> SPE.Repo.delete_all()
  end

  def list_unfinished_jobs() do
    from(j in SPE.Schemas.Job,
      where: fragment("? @> ?", j.results, ^Jason.encode!(%{"status" => "pending"})))
    |> SPE.Repo.all()
  end
end
