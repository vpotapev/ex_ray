defmodule ExRay.Store do
  @moduledoc """
  Store the span chains in an ETS table. The table must be created during
  the application initialization using the create call. The span chain acts
  like a call stack by pushing and popping spans as they come in and out of
  scope.
  """
  use GenServer
  require Logger

  @spans_table_name :ex_ray_spans_table
  @top_span_table_name :ex_ray_top_span_table
  @ex_ray_ets_tables_list [@spans_table_name, @top_span_table_name]
  @logs_enabled Application.get_env(:ex_ray, :logs_store_enabled, false)

  # API

  @spec start_link :: {atom, pid} | {atom, {atom, pid}}
  def start_link do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Pushes a new span to the span stack. The key must be unique.
  """
  @spec push(
    integer,
    {:span, integer, integer, String.t, integer, integer | :undefined, list, list, integer | :undefined} | function()
    ) :: any
  def push(trace_id, {:span, _, _, _, _, _, _, _, _} = span) when is_integer(trace_id)
  do
    GenServer.call(__MODULE__, {:push, trace_id, span})
  end
  def push(trace_id, span_fn) when is_integer(trace_id) and is_function(span_fn, 0)
  do
    GenServer.call(__MODULE__, {:push, trace_id, span_fn})
  end

  @doc """
  Pops the top span off the stack. The `pop` operation is based on key and span_id.
  The span_id is used to select proper span in case of any async operations when an non-top span is popped.
  """
  @spec pop(integer, tuple) :: any
  def pop(trace_id, {:span, _, _, _, _, _, _, _, _} = span) when is_integer(trace_id)
  do
    GenServer.call(__MODULE__, {:pop, trace_id, span})
  end
  def pop(trace_id, span_fn) when is_integer(trace_id) and is_function(span_fn, 0)
  do
    GenServer.call(__MODULE__, {:pop, trace_id, span_fn})
  end

  @doc """
  Fetch span stack for the given key
  """
  @spec get(integer) :: [any]
  def get(trace_id) when is_integer(trace_id) do
    GenServer.call(__MODULE__, {:get, trace_id})
  end

  @doc """
  Fetch the top level span for a given key
  """
  @spec current(integer) :: [any]
  def current(trace_id) when is_integer(trace_id) do
    GenServer.call(__MODULE__, {:current, trace_id})
  end

  @doc """
  Unsafe fetch the top level span for a given key
  """
  @spec current(integer) :: [any]
  def current_unsafe(trace_id) when is_integer(trace_id) do
    current_impl(trace_id)
  end

  # GenServer handlers

  def init(_args) do
    {:ok, %{}, {:continue, %{}}}
  end

  @doc """
  Initializes the spans ETS table. The span table can be shared across
  process boundary.
  """
  def handle_continue(_continue, state) do
    create_impl()
    {:noreply, state}
  end

  def handle_call({:push, trace_id, span}, _from, state) do
    res = push_impl(trace_id, span)
    {:reply, res, state}
  end

  def handle_call({:pop, trace_id, span}, _from, state) do
    res = pop_impl(trace_id, span)
    {:reply, res, state}
  end

  def handle_call({:get, trace_id}, _from, state) do
    res = get_impl(trace_id)
    {:reply, res, state}
  end

  def handle_call({:current, trace_id}, _from, state) do
    res = current_impl(trace_id)
    {:reply, res, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  @spec create_impl :: any
  defp create_impl do
    table_props = [:set, :named_table, :public, read_concurrency: true, write_concurrency: true]
    Enum.each(@ex_ray_ets_tables_list,
      fn(t) -> if :ets.info(t) == :undefined, do: :ets.new(t, table_props) end)
  end

  @spec push_impl(integer, tuple|function) :: any
  defp push_impl(trace_id, {:span, _, trace_id, _, span_id, _, _, _, _} = span) do
    key = {trace_id, span_id}
    if @logs_enabled do
      Logger.debug(fn -> ">>> Store.push #{inspect key}, #{inspect span}" end)
    end
    # store into spans stack table
    :ets.insert(@spans_table_name, {key, span})
    # store into top span table
    :ets.insert(@top_span_table_name, {trace_id, span})
    span
  end
  defp push_impl(trace_id, span_fn) when is_function(span_fn) do
    {:span, _, _, _, _, _, _, _, _} = span = span_fn.()
    push_impl(trace_id, span)
  end

  @spec pop_impl(integer, tuple|function) :: any
  defp pop_impl(trace_id, {:span, timestamp, trace_id, name, span_id, parent_span_id, tags, logs, duration} = span) do
    key = {trace_id, span_id}
    p_key = {trace_id, parent_span_id}
    if @logs_enabled do
      Logger.debug(fn -> ">>> Store.pop #{inspect key} from #{inspect span}" end)
    end
    # check if popped elem is on top of the stack
    case current_impl(trace_id) do
      ^span ->
        :ets.delete(@spans_table_name, key)
        if parent_span_id == :undefined do
          :ets.delete(@top_span_table_name, trace_id)
        else
          case :ets.lookup(@spans_table_name, p_key) do
            [] ->
              :ok
            [p_span] when is_tuple(p_span) ->
              # :ets.insert(@spans_table_name, {p_key, p_span})
              :ets.insert(@top_span_table_name, {trace_id, p_span})
          end
        end
      _other_span ->
        # mark current span in stack as deleted; all other things aren't changed
        :ets.insert(
          @spans_table_name,
          {key, {:deleted_span, timestamp, trace_id, name, span_id, parent_span_id, tags, logs, duration}})
    end
    span
  end
  defp pop_impl(trace_id, span_fn) do
    {:span, _, _, _, _, _, _, _, _} = span = span_fn.()
    pop_impl(trace_id, span)
  end

  @spec get_impl(integer) :: [any]
  defp get_impl(trace_id) do
    @spans_table_name
    |> :ets.match({{trace_id, :_}, :"$1"})
    |> case do
      [] -> []
      vals -> for [val] <- vals, do: val
    end
  end

  @spec current_impl(integer) :: [any]
  defp current_impl(trace_id) do
    res = :ets.lookup(@top_span_table_name, trace_id)
    case res do
      [] -> nil
      [{_key, span}] -> span
    end
  end

end
