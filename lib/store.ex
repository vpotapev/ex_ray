defmodule ExRay.Store do
  @moduledoc """
  Store the span chains in an ETS table. The table must be created during
  the application initialization using the create call. The span chain acts
  like a call stack by pushing and popping spans as they come in and out of
  scope.
  """

  @table_name :ex_ray_tracers_table
  @ex_ray_ets_tables_list [@table_name]
  @logs_enabled Application.get_env(:ex_ray, :logs_store_enabled, false)

  require Logger

  @doc """
  Initializes the spans ETS table. The span table can be shared across
  process boundary.
  """
  @spec create :: any
  def create do
    table_props = [:set, :named_table, :public, read_concurrency: true, write_concurrency: true]
    Enum.each(@ex_ray_ets_tables_list,
      fn(t) -> if :ets.info(t) == :undefined, do: :ets.new(t, table_props) end)
  end

  @doc """
  Pushes a new span to the span stack. The key must be unique.
  """
  @spec push(String.t, any) :: any
  def push(key, val) when is_binary(key) do
    if @logs_enabled do
      Logger.debug(fn -> ">>> Store.push #{inspect key}, #{inspect val}" end)
    end
    vals = get(key)

    if length(vals) > 0 do
      :ets.insert(@table_name, {key, [val] ++ vals})
    else
      :ets.insert(@table_name, {key, [val]})
    end
    val
  end

  @doc """
  Pops the top span off the stack.
  """
  @spec pop(String.t) :: any
  def pop(key) when is_binary(key) do
    v = get(key)
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

  @doc """
  Fetch span stack for the given key
  """
  @spec get(String.t) :: [any]
  def get(key) when is_binary(key) do
    @table_name
    |> :ets.lookup(key)
    |> case do
      []             -> []
      [{_key, vals}] -> vals
    end
  end

  @doc """
  Fetch the top level span for a given key
  """
  @spec current(String.t) :: [any]
  def current(key) when is_binary(key) do
    key
    |> get
    |> case do
     []       -> nil
     [h | _t] -> h
    end
  end

end
