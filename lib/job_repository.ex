defmodule JobRepository do
  import Ecto.Query

  def save_job_state(state) do
    job_data = Serializer.serialize_job(state)

    job_struct = SPE.Schemas.Job |> struct(job_data)

    SPE.Repo.insert!(
      job_struct,
      on_conflict: :replace_all,
      conflict_target: :name
    )
  end

  def get_job_state(job_name) do
    case SPE.Repo.get_by(SPE.Schemas.Job, name: job_name) do
      nil -> {:error, :not_found}
      job ->
        {:ok, Serializer.deserialize_job(job)}
    end
  end

  def delete_job(job_name) do
    from(j in SPE.Schemas.Job, where: j.name == ^job_name)
    |> SPE.Repo.delete_all()
  end

end
