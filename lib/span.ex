defmodule ExRay.Span do
  @moduledoc """
  A set of convenience functions to manage spans.
  Span tuple contains the next fields:
  {:span, timestamp, trace_id, name, span_id, parent_span_id, tags, logs, duration}
  """

  @doc """
  Create a new root span with a given name and unique request chain ID.
  The request ID uniquely identifies the call chain and will be used as
  the primary key in the ETS table tracking the span chain.
  """
  @spec open(String.t, integer) :: any
  def open(name, trace_id)
  when is_bitstring(name) and is_integer(trace_id)
  do
    ExRay.Store.push(
      trace_id,
      fn ->
        _span =
          trace_id
          |> ExRay.Store.current_unsafe
          |> case do
              nil    -> IO.inspect 1; :otter.start(name, trace_id)
              p_span -> IO.inspect {2, p_span}; :otter.start(name, p_span)
            end
      end)
  end

  @doc """
  Creates a new span with a given parent span
  """
  @spec open(String.t, integer, any) :: any
  def open(name, trace_id, p_span)
  when is_bitstring(name) and is_integer(trace_id) and is_tuple(p_span)
  do
    ExRay.Store.push(trace_id, fn -> _span = :otter.start(name, p_span) end)
  end

  @doc """
  Closes the given span and pops the span state in the associated ETS
  span chain.
  """
  @spec close(any, integer) :: any
  def close(span, trace_id)
  when is_integer(trace_id) and is_tuple(span)
  do
    ExRay.Store.pop(trace_id, fn -> :otter.finish(span); span end)
  end

  @doc """
  Convenience to retrive the parent span ID from a given span
  """
  @spec parent_id({:span, integer, integer, String.t, integer, integer | :undefined, list, list, integer | :undefined}) :: String.t
  def parent_id({:span, _timestamp, _trace_id, _name, _span_id, parent_span_id, _tags, _logs, _duration} = _span) do
    parent_span_id
  end
end
