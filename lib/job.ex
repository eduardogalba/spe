defmodule Job do
  use GenServer
  require Logger

  def init(state) do
    {:ok, state}
  end

  def start_link(args) do
    # En teoria, me debe llegar por los args, el plan a seguir en una lista,
    # el numero de trabajadores y la descripción del trabajo
    GenServer.start_link(__MODULE__, args, [])
  end

  def handle_cast(request, state) do
    case request do

    end
  end

  def handle_info(msg, state) do

  end

end
