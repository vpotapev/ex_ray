defmodule ExRay.Store do
  @moduledoc """
  Store the span chains in an ETS table. The table must be created during
  the application initialization using the create call. The span chain acts
  like a call stack by pushing and popping spans as they come in and out of
  scope.
  """
  use GenServer
  require Logger

  @table_name :ex_ray_tracers_table
  @ex_ray_ets_tables_list [@table_name]
  @logs_enabled Application.get_env(:ex_ray, :logs_store_enabled, false)

  # API

  @spec start_link :: {atom, pid} | {atom, {atom, pid}}
  def start_link do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Pushes a new span to the span stack. The key must be unique.
  """
  @spec push(String.t, any) :: any
  def push(key, val) when is_integer(key) do
    GenServer.call(__MODULE__, {:push, key, val})
  end

  @doc """
  Pops the top span off the stack.
  """
  @spec pop(String.t) :: any
  def pop(key) when is_integer(key) do
    GenServer.call(__MODULE__, {:pop, key})
  end

  @doc """
  Fetch span stack for the given key
  """
  @spec get(String.t) :: [any]
  def get(key) when is_integer(key) do
    GenServer.call(__MODULE__, {:get, key})
  end

  @doc """
  Fetch the top level span for a given key
  """
  @spec current(String.t) :: [any]
  def current(key) when is_integer(key) do
    GenServer.call(__MODULE__, {:current, key})
  end

  # GenServer handlers

  def init(_args) do
    {:ok, %{}, {:continue, %{}}}
  end

  def handle_continue(_continue, state) do
    create_impl()
    {:noreply, state}
  end

  def handle_call({:push, key, value}, _from, state) do
    res = push_impl(key, value)
    {:reply, res, state}
  end

  def handle_call({:pop, key}, _from, state) do
    res = pop_impl(key)
    {:reply, res, state}
  end

  def handle_call({:get, key}, _from, state) do
    res = get_impl(key)
    {:reply, res, state}
  end

  def handle_call({:current, key}, _from, state) do
    res = current_impl(key)
    {:reply, res, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  @doc """
  Initializes the spans ETS table. The span table can be shared across
  process boundary.
  """
  @spec create_impl :: any
  defp create_impl do
    table_props = [:set, :named_table, :public, read_concurrency: true, write_concurrency: true]
    Enum.each(@ex_ray_ets_tables_list,
      fn(t) -> if :ets.info(t) == :undefined, do: :ets.new(t, table_props) end)
  end

  @spec push(String.t, any) :: any
  defp push_impl(key, val) do
    if @logs_enabled do
      Logger.debug(fn -> ">>> Store.push #{inspect key}, #{inspect val}" end)
    end
    vals = get_impl(key)

    if length(vals) > 0 do
      :ets.insert(@table_name, {key, [val] ++ vals})
    else
      :ets.insert(@table_name, {key, [val]})
    end
    val
  end

  @spec pop_impl(String.t) :: any
  defp pop_impl(key) do
    v = get_impl(key)
    if @logs_enabled do
      Logger.debug(fn -> ">>> Store.pop #{inspect key} from #{inspect v}" end)
    end
    [h | t] = v
    :ets.insert(@table_name, {key, t})
    if @logs_enabled do
      Logger.debug(fn -> ">>> Store.pop #{inspect key}: val=#{inspect h}" end)
    end
    h
  end

  @spec get_impl(String.t) :: [any]
  defp get_impl(key) do
    @table_name
    |> :ets.lookup(key)
    |> case do
      []             -> []
      [{_key, vals}] -> vals
    end
  end

  @spec current_impl(String.t) :: [any]
  defp current_impl(key) do
    key
    |> get_impl
    |> case do
     []       -> nil
     [h | _t] -> h
    end
  end

end
