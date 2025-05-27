defmodule Repo.Migrations.CreateJobsTable do
  use Ecto.Migration

  def change do
    create table(:jobs, primary_key: false) do
      add :id, :string, primary_key: true
      add :plan, :jsonb
      add :num_workers, :integer
      add :enables, :jsonb
      add :returns, :jsonb
      add :results, :jsonb
      add :tasks, :jsonb
      add :inserted_at, :utc_datetime_usec, default: fragment("NOW()")
    end

    create index(:jobs, ["(results->'status')"], name: :jobs_status_index)
  end
end
