defmodule ExRay.Context do
  @moduledoc """
  Captures the essence of decorated function call.
  It contains the following elements:

  * target: The name of the annotated function
  * args: A collection of arguments passed to the wrapped function
  * guards: A collection of guard clauses that identifies the function
  * meta: Metadata specified in the annotation
  * default_trace_id: Used in situation when there is no trace_id in the params of an API call
  """
  defstruct target: nil, args: [], guards: [], meta: nil, default_trace_id: nil

  @type t :: %ExRay.Context {
    target: String.t,
    args:   [any],
    guards: [any],
    meta:   [any],
    default_trace_id: String.t
  }
end
