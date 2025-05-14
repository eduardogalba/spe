defmodule Job do
  use GenServer
  require Logger

  def init(state) do
    id = state[:id]
    # Me suscribo a la cola donde recibo los mensajes de las tareas
    Phoenix.PubSub.subscribe(SPE.PubSub, "#{inspect(id)}" <> ":reports", [])



    # Asumo que plan es correcto y viene como una lista de listas [[]],
    # cada uno de los niveles de tamaño máximo num_workers
    case state[:plan] do
      [] ->
        {:stop, {:error, :empty_task_plan}}
      [x | xs] ->
        # Actualizamos las tareas
        state = Map.put(state, :plan, xs)
        Logger.info("[Job #{inspect(self())}]: Job starting...")

        # Ponemos en marcha todas las tareas del primer nivel
        children =
          Enum.map(x, fn task ->
            %{
              id: task,
              start: {Task, :apply, [state[:id], state[:tasks][task]["exec"]]}
            }
          end)

        case Supervisor.start_link(children, strategy: :one_for_one) do
          {:ok, supv} ->
            {:ok, Map.put(state, :supv, supv)}
          {:error, {:already_started, _}} = error ->
            Logger.error("[Job #{inspect(self())}]: Server is already started.")
            error
          {:error, {:shutdown, reason}} ->
            Logger.error("[Job #{inspect(self())}]: One of the child processes is crashing caused by #{inspect(reason)}")
            reason
        end
      _ -> {:stop, {:error, :wrong_plan_format}}
    end
  end

  def start_link(state) do
    # Asumo que me debe llegar por los args, mi id como trabajo, el plan a seguir en una lista,
    # el numero de trabajadores y la descripción del trabajo.

    # Quiero solo las descripciones de tarea como un mapa y no toda la descripción teniendo
    # las tareas como una lista

    tasks =
      Enum.into(state[:desc]["tasks"], %{}, fn task_desc ->
        {task_desc["name"], task_desc}
      end)

    new_state =
      state
      |> Map.delete(:desc)
      |> Map.put(:tasks, tasks)
      |> Map.put(:undone, %{})
      |> Map.put(:done, %{})

    GenServer.start_link(__MODULE__, new_state, [])
  end

  def handle_info(msg, state) do

  end

end
