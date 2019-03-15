defmodule ExRay.SpanTest do
  use ExUnit.Case
  doctest ExRay

  use ExRay, pre: :f1, post: :f2
  alias ExRay.{Store, Span}

  @trace_id 123123123

  setup_all do
    :ok
  end

  setup do
    Store.clean()
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
    assert ctx.meta[:kind] in [:test, :test2, :spawned_fn]
    trace_id = ctx.meta[:trace_id]
    Span.open("span_name", trace_id)
  end

  def f2(_ctx, span, _res) do
    {:span, _timestamp, trace_id, _name, _span_id, _parent_span_id, _tags, _logs, _duration} = span
    Span.close(span, trace_id)
  end

  @trace kind: :test, trace_id: @trace_id
  def test1(a, b) do
    a + b
  end

  @trace kind: :test2, trace_id: @trace_id
  def test2(a, b) do
    Task.start_link(fn -> spawned_fn(a, b) end)
    b - a
  end

  @trace kind: :spawned_fn, trace_id: @trace_id
  def spawned_fn(a, b) do
    a + b
  end

  test "basic" do
    assert test1(1, 2) == 3
  end

  test "child span", ctx do
    Store.push(ctx.trace_id, ctx.span)
    assert test1(1, 2) == 3
  end

  test "child span with spawned fn", ctx do
    Store.push(ctx.trace_id, ctx.span)
    assert test2(1, 2) == 1
  end

  test "open/3", ctx do
    Store.push(ctx.trace_id, ctx.span)
    span = Span.open("fred", ctx.trace_id, ctx.span)
    assert Store.current(ctx.trace_id) == span
    Span.close(span, ctx.trace_id)
  end

  test "open/2", ctx do
    span = Span.open("fred", ctx.trace_id)
    assert length(Store.get(ctx.trace_id)) == 1
    Span.close(span, ctx.trace_id)
  end

  test "parent_id/1", ctx do
    assert Span.parent_id(ctx.span) == :undefined
  end
end
