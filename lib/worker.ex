defmodule Worker do
  @moduledoc """
  **Worker** is responsible for executing tasks within a job in the SPE system.
  It is a GenServer that handles task execution, including applying functions with parameters and managing timeouts.
  ## Usage:
  - Start a Worker
  ```elixir
  {:ok, worker_pid} = Worker.start_link(%{job: job_pid})
  ```
  - Send a task to the Worker
  ```elixir
  Worker.send_task(worker_pid, job_id, task_name, timeout, function, params)
  ```
  - Check if the Worker is ready
  ```elixir
  Worker.are_u_ready?(worker_pid)
  ```
  ## Notes:
  - The Worker uses a GenServer to manage its state and handle task execution.
  - It applies functions with parameters and handles exceptions that may occur during execution.
  - It communicates task completion back to the Job that it belongs to.
  ## Dependencies:
  - `GenServer`: For managing the state and handling calls.
  - `Logger`: For logging debug information.
  - `Phoenix.PubSub`: For broadcasting task start and termination events.
  """
  use GenServer
  require Logger

  @doc """
  Initializes the Worker with the given state.
  The state should include the job PID that the Worker is associated with.
  #### Parameters:
  - `state`: A map containing the job PID and any other necessary state information.
  #### Returns:
  - `{:ok, state}`: The initial state of the Worker is returned, ready to handle tasks.
  #### Example:
  ```elixir
  {:ok, worker_pid} = Worker.start_link(%{job: job_pid})
  ```
  """
  def init(state) do
    Logger.debug("[Worker #{inspect(self())}]: init: Me llega en state #{inspect(state)}")
    send(state[:job], {:notify_ready, self()})
    Logger.debug("[Worker #{inspect(self())}]: Entrando a init")
    {:ok, state}
  end

  @doc """
  Starts the Worker process with the given state.
  This function initializes the Worker and sets it up to handle tasks.
  #### Parameters:
  - `state`: A map containing the job PID and any other necessary state information.
  #### Returns:
  - `{:ok, pid}`: The Worker process's PID is returned, indicating that it has been successfully started.
  #### Example:
  ```elixir
  {:ok, worker_pid} = Worker.start_link(%{job: job_pid})
  ```
  """
  def start_link(state) do
    Logger.debug("[SuperWorker #{inspect(self())}]: Iniciando Worker...")
    Logger.debug("[SuperWorker #{inspect(self())}]: Esto tengo en mi estado #{inspect(state)}")
    GenServer.start_link(__MODULE__, state)
  end

  @doc """
  Sends a task to the Worker for execution.
  This function sends a task to the Worker, which includes the job ID, task name, timeout, function to execute, and parameters.
  #### Parameters:
  - `worker_pid`: The PID of the Worker to which the task will be sent.
  - `job_id`: The ID of the job that the task belongs to.
  - `name`: The name of the task to be executed.
  - `timeout`: The timeout for the task execution, in milliseconds.
  - `fun`: The function to be executed as part of the task.
  - `params`: The parameters to be passed to the function.
  #### Returns:
  - `:ok`: Indicates that the task has been sent to the Worker for execution.
  #### Example:
  ```elixir
  Worker.send_task(worker_pid, job_id, task_name, timeout, function, params)
  ```
  """
  def send_task(worker_pid, job_id, name, timeout, fun, params) do
    GenServer.cast(worker_pid, {:task, {job_id, {name, timeout, fun, params}}})
  end

  @doc """
  Handles the task execution in the Worker.
  This function is called when a task is sent to the Worker. It applies the function with the given parameters and handles the result.
  #### Parameters:
  - `{:task, {job_id, {name, timeout, fun, params}}}`: A tuple containing the job ID, task name, timeout, function to execute, and parameters.
  #### Returns:
  - `{:noreply, state}`: Indicates that the Worker has completed the task and is ready for the next one.
  #### Example:
  ```elixir
  GenServer.cast(worker_pid, {:task, {job_id, {task_name, timeout, function, params}}})
  ```
  """
  def handle_cast({:task, {job_id, {name, timeout, fun, params}}}, state) do
    result = apply(job_id, name, timeout, fun, params)
    Job.task_completed(state[:job], {name, result, self()})
    {:noreply, state}
  end

  @doc """
  Handles the call to check if the Worker is ready.
  This function responds to a call to check if the Worker is ready to handle tasks.
  #### Parameters:
  - `:are_u_ready`: A message indicating that the caller wants to check if the Worker is ready.
  #### Returns:
  - `{:reply, :ok, state}`: Indicates that the Worker is ready and returns the current state.
  #### Example:
  ```elixir
  GenServer.call(worker_pid, :are_u_ready)
  ```
  """
  def handle_call(:are_u_ready, _from, state) do
    {:reply, :ok, state}
  end

  @doc """
  Checks if the Worker is ready to handle tasks.
  This function sends a call to the Worker to check its readiness.
  #### Parameters:
  - `worker_pid`: The PID of the Worker to check.
  #### Returns:
  - `:ok`: Indicates that the Worker is ready to handle tasks.
  #### Example:
  ```elixir
  Worker.are_u_ready?(worker_pid)
  ```
  """
  def are_u_ready?(worker_pid) do
    GenServer.call(worker_pid, :are_u_ready)
  end

  @doc """
  Applies a function with the given parameters and handles exceptions.
  This function executes the provided function with the specified parameters and manages timeouts and exceptions.
  #### Parameters:
  - `job_id`: The ID of the job that the task belongs to.
  - `task_name`: The name of the task to be executed.
  - `timeout`: The timeout for the task execution, in milliseconds.
  - `function`: The function to be executed as part of the task.
  - `args`: The parameters to be passed to the function.
  #### Returns:
  - `{:result, result}`: If the function executes successfully, it returns the result wrapped in a tuple.
  - `{:failed, reason}`: If the function fails or times out, it returns an error tuple with the reason.
  #### Example:
  ```elixir
  result = Worker.apply(job_id, task_name, timeout, function, args)
  ```
  """
  def apply(job_id, task_name, timeout, function, args) do
    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_started, task_name}}
    )

    effective_timeout = if !timeout, do: :infinity, else: timeout

    # Logger.debug("[Worker #{inspect(self())}]: Task #{inspect(task_name)} Arguments #{inspect(args)}")

    task_fun = fn ->
      try do
        {:result, Kernel.apply(function, [args])}
      rescue
        exception ->
          # Logger.debug("[#{inspect(task_name)}]: Capturada excepción en el hijo: #{inspect(exception)}")
          # Logger.error("#{inspect(__STACKTRACE__)}")
          {:failed, {:crashed, Exception.message(exception)}}
      end
    end

    task = Task.async(task_fun)

    # Logger.debug("[Worker #{inspect(self())}]: Primera linea de defensa atravesada")

    result =
      try do
        Task.await(task, effective_timeout)
      catch
        :exit, {:timeout, _} ->
          # Logger.debug("Task.await ha hecho timeout.")
          {:failed, :timeout}

        :exit, reason ->
          # Logger.debug("Task.await ha terminado por exit: #{inspect(reason)}")
          {:failed, reason}
      end

    Logger.debug(
      "[Worker #{inspect(self())}]: Sending to PubSub #{inspect(job_id)} message queue #{inspect(result)}"
    )

    # Comunicación a todos de las tareas terminadas
    Phoenix.PubSub.local_broadcast(
      SPE.PubSub,
      job_id,
      {:spe, :erlang.monotonic_time(:millisecond), {job_id, :task_terminated, task_name}}
    )

    result
  end
end
