defmodule SuperSPE do
  @moduledoc """
  SuperSPE is the main supervisor for the SuperSPE application.
  It manages the SuperManager and JobManager processes.
  """
  use Supervisor

  @doc """
  Starts the SuperSPE supervisor with its children processes.
  The children include the Phoenix PubSub, SuperManager, and JobManager.
  The supervisor uses a one-for-one strategy with a maximum of one restart
  within five seconds.
  The JobManager is started with a name specified in the options.
  The SuperManager is started without any options.
  The PubSub is started with a name defined in the SPE module.
  The supervisor is named SPE.SuperSPE.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, sup_pid} = SuperSPE.start_link([])
  ```
  """
  @impl Supervisor
  def init(children) do
    opts = [
      strategy: :one_for_one,
      max_restarts: 1,
      max_seconds: 5
    ]

    Supervisor.init(children, opts)
  end

  @doc """
  Starts the SuperSPE supervisor with the given options.
  This function initializes the Supervisor and sets it up to manage the SuperManager and JobManager processes.
  #### Parameters:
  - `opts`: Options for the JobManager, including the name to be used.
  #### Returns:
  - `{:ok, pid}`: The Supervisor's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, sup_pid} = SuperSPE.start_link(name: SPE.JobManager)
  ```
  """
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
