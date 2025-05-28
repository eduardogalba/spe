defmodule SPE.Schemas.Job do
  use Ecto.Schema

  @primary_key {:name, :string, autogenerate: false}
  schema "jobs" do
    field :plan, :map
    field :num_workers, :integer
    field :enables, :map
    field :returns, :map
    field :results, :map
    field :tasks, :map
    field :inserted_at, :utc_datetime
  end
end
