defmodule ExRay.Span do
  @moduledoc """
  A set of convenience functions to manage spans.
  """

  @doc """
  Create a new root span with a given name and unique request chain ID.
  The request ID uniquely identifies the call chain and will be used as
  the primary key in the ETS table tracking the span chain.
  """
  @spec open(String.t, integer) :: any
  def open(name, req_id) do
    span = req_id
    |> ExRay.Store.current
    |> case do
      nil    -> :otter.start(name, req_id)
      p_span -> :otter.start(name, p_span)
    end

    ExRay.Store.push(req_id, span)
  end

  @doc """
  Creates a new span with a given parent span
  """
  @spec open(String.t, integer, any) :: any
  def open(name, req_id, p_span) do
    span = :otter.start(name, p_span)

    ExRay.Store.push(req_id, span)
  end

  @doc """
  Closes the given span and pops the span state in the associated ETS
  span chain.
  """
  @spec close(any, integer) :: any
  def close(span, req_id) do
    :otter.finish(span)
    ExRay.Store.pop(req_id)
  end

  @doc """
  Convenience to retrive the parent span ID from a given span
  """
  @spec parent_id({:span, integer, integer, String.t, integer, integer | :undefined, list, list, integer | :undefined}) :: String.t
  def parent_id({:span, _timestamp, _trace_id, _name, _span_id, parent_span_id, _tags, _logs, _duration}) do
    parent_span_id
  end
end
