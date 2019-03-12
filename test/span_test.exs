defmodule ExRay.SpanTest do
  use ExUnit.Case
  doctest ExRay

  use ExRay, pre: :f1, post: :f2

  alias ExRay.{Store, Span}

  setup_all do
    :ok
  end

  setup do
    trace_id = ExRay.rand_id
    span = {
      :span,
      1509045368683303,
      trace_id,
      :root,
      12387109925362352574,
      :undefined,
      [],
      [],
      :undefined
    }
    {:ok, %{span: span, trace_id: trace_id}}
  end

  def f1(ctx) do
    assert ctx.meta[:kind] == :test
    trace_id = 123123123
    Span.open("span_name", trace_id)
  end

  def f2(_ctx, span, _res) do
    {:span, _timestamp, trace_id, _name, _span_id, _parent_span_id, _tags, _logs, _duration} = span
    Span.close(span, trace_id)
  end

  @trace kind: :test
  def test1(a, b) do
    a + b
  end

  test "basic" do
    assert test1(1, 2) == 3
  end

  test "child span", ctx do
    Store.push(ctx.trace_id, ctx.span)
    assert test1(1, 2) == 3
  end

  test "open/3", ctx do
    span = Span.open("fred", ctx.trace_id, ctx.span)
    assert Store.current(ctx.trace_id) == span
    span |> Span.close(ctx.trace_id)
  end

  test "open/2", ctx do
    span = Span.open("fred", ctx.trace_id)
    assert length(Store.get(ctx.trace_id)) == 1
    span |> Span.close(ctx.trace_id)
  end

  test "parent_id/1", ctx do
    assert Span.parent_id(ctx.span) == :undefined
  end
end
