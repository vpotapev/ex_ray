defmodule ExRay.SpanTest do
  use ExUnit.Case
  doctest ExRay

  use ExRay, pre: :f1, post: :f2

  alias ExRay.{Store, Span}

  setup_all do
    :ok
  end

  setup do
    span = {
      :span,
      1509045368683303,
      12387109925362352574,
      :root,
      15549390946617352406,
      :undefined,
      [],
      [],
      :undefined
    }
    {:ok, %{span: span}}
  end

  def f1(ctx) do
    assert ctx.meta[:kind] == :test
    trace_id = 123123123
    Span.open("span_name", trace_id)
  end

  def f2(_ctx, span, _res) do
    trace_id = 123123123
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
    trace_id = ExRay.rand_id
    Store.push(trace_id, ctx.span)
    assert test1(1, 2) == 3
  end

  test "open/3", ctx do
    trace_id = ExRay.rand_id
    span = Span.open("fred", trace_id, ctx.span)
    assert Store.current(trace_id) == span
    span |> Span.close(trace_id)
  end

  test "open/2" do
    trace_id = ExRay.rand_id
    span = Span.open("fred", trace_id)
    assert length(Store.get(trace_id)) == 1
    span |> Span.close(trace_id)
  end

  test "parent_id/1", ctx do
    IO.inspect ctx.span
    assert Span.parent_id(ctx.span) == :undefined
  end
end
