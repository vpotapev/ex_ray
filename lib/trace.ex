defmodule ExRay.Trace do
  # TODO: should be documented

  defmacro __using__(_opts) do
    _ = :ex_ray_trace_unique_atom # define unique atom in compile time

    quote do
      use ExRay, pre: :before_fun, post: :after_fun
      require Logger
      alias ExRay.Span
      alias ExRay.Store

      defp get_opentracing_tags(ctx, predefined_tags) do
        tags = predefined_tags ++ [
          "span.kind": "server",
          type: ctx.meta[:type] || nil,
          hostname: System.get_env("HOSTNAME") || nil,
          rts_id: System.get_env("RTS_ID") || nil,
          rts_host: System.get_env("RTS_HOST") || nil,
          cts_id: System.get_env("CCS_ID") || nil,
          ccs_host: System.get_env("CCS_HOST") || nil,
          cts: System.get_env("CTS") || nil,
          args: "#{inspect ctx.args}",
          lc: __MODULE__
        ] |> Enum.filter(fn({_, val}) -> not is_nil(val) end)
      end

      defp before_fun(ctx) do
        String.to_existing_atom("ex_ray_trace_unique_atom")
      rescue
        # this is runtime-only call since no an :ex_ray_trace_unique_atom is defined on runtime
        ArgumentError ->
          before_fun_body(ctx)
        _ ->
          :ok
      end

      defp before_fun_body(ctx) do
        trace_enabled? = Application.get_env(:ex_ray, :enabled, false)
        trace_id_extractor = Application.get_env(:ex_ray, :trace_id_extractor)
        if trace_enabled? and trace_id_extractor != nil do
          predefined_tags = Application.get_env(:ex_ray, :predefined_tags, [])
          # list of available tags
          tags = get_opentracing_tags(ctx, predefined_tags)
          trace_id = get_trace_id(ctx)
          span = Span.open(ctx.target, trace_id)
          otter_log(span, ">>> Open span #{inspect span} with trace_id=#{inspect trace_id}")
          _span = Enum.reduce(tags, span, fn({tag, val}, acc) -> :otter.tag(acc, tag, val) end)
        end
      end

      defp after_fun(ctx, span, res) do
        String.to_existing_atom("ex_ray_trace_unique_atom")
      rescue
        # this is runtime-only call since no an :ex_ray_trace_unique_atom is defined on runtime
        ArgumentError -> after_fun_body(ctx, span, res)
      end

      defp after_fun_body(ctx, span, res) do
        trace_enabled? = Application.get_env(:ex_ray, :enabled, false)
        trace_id_extractor = Application.get_env(:ex_ray, :trace_id_extractor)
        if trace_enabled? and trace_id_extractor != nil do
          otter_log(span, "<<< Close span #{inspect span} which returned #{inspect res}; ...")
          trace_id = get_trace_id(ctx)
          otter_log(span, "<<< ... and the span #{inspect span} has trace_id=#{inspect trace_id}")
          Span.close(span, trace_id)
        end
      end

      @doc """
      Trying to determine request id from context (`ctx` param variable)
      """
      @spec get_trace_id(map()) :: String.t | nil
      def get_trace_id(ctx) do
        trace_id_extractor = Application.get_env(:ex_ray, :trace_id_extractor)
        get_trace_id_impl(ctx, trace_id_extractor)
      end

      defp get_trace_id_impl(ctx, nil), do: nil
      defp get_trace_id_impl(ctx, trace_id_extractor) do
        case trace_id_extractor.get_trace_id(ctx) do
          {:ok, trace_id} ->
            trace_id
          {:error, ctx} ->
            if Application.get_env(:ex_ray, :debug, false) do
              st = Process.info(self(), :current_stacktrace)
              Logger.error("The trace_id value is not found in the next args: #{inspect(ctx.args)}")
              Logger.error("Stacktrace: #{inspect(st)}")
            end
            nil
        end
      end

      defp otter_log(span, msg) when is_tuple(span) and is_bitstring(msg) do
        if Application.get_env(:ex_ray, :logs_enabled, false), do: :otter.log(span, msg)
      end
    end
  end
end
