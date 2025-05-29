defmodule SuperSPE do
  use Supervisor

  @impl Supervisor
  def init(children) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Supervisor.init(children, opts)
  end

  def start_link(opts) do
    pubsub = Phoenix.PubSub.child_spec(name: SPE.PubSub)

    super_manager = %{
      id: :super_manager,
      start: {SuperManager, :start_link, []}
    }

    manager_opts = Keyword.put(opts, :name, SPE.JobManager)

    manager = %{
      id: :manager,
      start: {JobManager, :start_link, [manager_opts]}
    }

    children = [
      pubsub,
      super_manager,
      manager
    ]

    Supervisor.start_link(__MODULE__, children, name: SPE.SuperSPE)
  end
end
