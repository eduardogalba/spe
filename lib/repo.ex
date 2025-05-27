defmodule SPE.Repo do
  use Ecto.Repo,
    otp_app: :spe,
    adapter: Ecto.Adapters.Postgres
end
