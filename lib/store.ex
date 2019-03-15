defmodule ExRay.Store do
  @moduledoc """
  Store the span chains in an ETS tables. The tables are created during
  the application initialization. The span chain acts like a call stack
  by pushing and popping spans as they come in and out of scope.
  """
  use GenServer
  require Logger

  # list of all stacked spans on this node
  @spans_table_name :ex_ray_spans_table
  # list of all top spans on this node
  @top_span_table_name :ex_ray_top_span_table
  @ex_ray_ets_tables_list [@spans_table_name, @top_span_table_name]

  # API

  @spec start_link :: {atom, pid} | {atom, {atom, pid}}
  def start_link do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Pushes a new span to the span stack.
  `trace_id` - TraceID of the `span`
  `span_fn` - function which generated new span
  """
  @spec push(
    integer,
    {:span, integer, integer, String.t, integer, integer | :undefined, list, list, integer | :undefined} | function
    ) :: any
  def push(trace_id, {:span, _, _, _, _, _, _, _, _} = span) when is_integer(trace_id) do
    GenServer.call(__MODULE__, {:push, trace_id, span})
  end
  def push(trace_id, span_fn) when is_integer(trace_id) and is_function(span_fn, 0) do
    GenServer.call(__MODULE__, {:push, trace_id, span_fn})
  end

  @doc """
  Pops the top span off the stack. The `pop` operation is based on key and span_id.
  The span_id is used to select proper span in case of any async operations when an non-top span is popped.
  """
  @spec pop(integer, tuple | function) :: any
  def pop(trace_id, {:span, _, _, _, _, _, _, _, _} = span) when is_integer(trace_id) do
    GenServer.call(__MODULE__, {:pop, trace_id, span})
  end
  def pop(trace_id, span_fn) when is_integer(trace_id) and is_function(span_fn, 0) do
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

  @doc """
  Cleanup state. Useful for unit tests.
  """
  def clean() do
    :ets.delete_all_objects(@spans_table_name)
    :ets.delete_all_objects(@top_span_table_name)
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
    debug_log(">>> Store.push with key #{inspect key} the span #{inspect span}")
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
    debug_log("<<< Store.pop by key #{inspect key} the span #{inspect span}")
    # check if popped elem is on top of the stack
    case current_impl(trace_id) do
      {:span, _, ^trace_id, _, ^span_id, _, _, _, _} ->
        delete_span(trace_id, span)
      _other_span ->
        # mark the span below current as deleted
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

  @spec delete_span(integer, tuple) :: any
  defp delete_span(trace_id, span) when is_integer(trace_id) and is_tuple(span) do
    {:span, _timestamp, trace_id, _name, span_id, parent_span_id, _tags, _logs, _duration} = span
    key = {trace_id, span_id}
    :ets.delete(@spans_table_name, key)
    case maybe_parent_span_already_deleted(trace_id, parent_span_id) do
      nil ->
        :ets.delete(@top_span_table_name, trace_id)
      new_top_span ->
        :ets.insert(@top_span_table_name, {trace_id, new_top_span})
    end
  end

  @spec maybe_parent_span_already_deleted(integer, integer | :undefined) :: any
  defp maybe_parent_span_already_deleted(_trace_id, :undefined) do
    # parent span is absent so current span is a root span
    nil
  end
  defp maybe_parent_span_already_deleted(trace_id, span_id) do
    key = {trace_id, span_id}
    case :ets.lookup(@spans_table_name, key) do
      [] ->
        # NOTE: this should't be happen, but the check will be there for a while
        raise ArgumentError, "Span with id #{span_id} should exist!"
      [{_key, span}] when is_tuple(span) ->
        deleted_span_processing(span)
    end
  end

  defp deleted_span_processing({:span, _, _, _, _, _, _, _, _} = span) do
    span
  end
  defp deleted_span_processing({:deleted_span, _, trace_id, _, span_id, parent_span_id, _, _, _}) do
    key = {trace_id, span_id}
    :ets.delete(@spans_table_name, key)
    maybe_parent_span_already_deleted(trace_id, parent_span_id)
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
    case :ets.lookup(@top_span_table_name, trace_id) do
      [] -> nil
      [{_key, span}] -> span
    end
  end

  defp debug_log(msg) when is_bitstring(msg) do
    if Application.get_env(:ex_ray, :logs_store_enabled, false), do: Logger.debug(msg)
  end
end
