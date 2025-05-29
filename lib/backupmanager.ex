defmodule BackupManager do
  use GenServer
  require Logger

  @ets_table :job_states

  def start_link(_opts) do
    GenServer.start_link(MODULE, [], name: MODULE)
  end

  def update_job_state(job_id, job_state) do
    GenServer.cast(MODULE, {:update_job_state, job_id, job_state})
  end

  def job_ended(job_id) do
    GenServer.cast(MODULE, {:job_ended, job_id})
  end

  def load_job_state(job_id) do
    GenServer.call(MODULE, {:load_job_state, job_id})
  end

  # Callbacks

  @impl true
  def init(_args) do
    Logger.info("BackupManager starting...")
    :ets.new(@ets_table, [:set, :public, :named_table])
    Logger.info("ETS table #{@ets_table} created/found.")
    {:ok, %{}}
  end

  @impl true
  def handle_cast({:update_job_state, job_id, job_state}, state) do
    Logger.debug("Updating state for Job #{inspect(job_id)}")
    :ets.insert(@ets_table, {job_id, job_state})
    {:noreply, state}
  end

  @impl true
  def handle_cast({:job_ended, job_id}, state) do
    Logger.info("Job #{inspect(job_id)} ended. Deleting state from backup.")
    :ets.delete(@ets_table, job_id)
    {:noreply, state}
  end

  @impl true
  def handle_call({:load_job_state, job_id}, _from, state) do
    Logger.debug("Loading state for Job #{inspect(job_id)}")
    case :ets.lookup(@ets_table, job_id) do
      [{^job_id, job_state}] ->
        {:reply, {:ok, job_state}, state}
      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end
end
