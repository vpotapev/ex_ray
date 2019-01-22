defmodule ExRay.Trace do
  # TODO: should be documented

  defmacro __using__(_opts) do
    _ = :ex_ray_trace_unique_atom # define unique atom in compile time

    quote do
      use ExRay, pre: :before_fun, post: :after_fun
      require Logger
      alias ExRay.Span
      alias ExRay.Store

      @request_id_extractor Application.get_env(:ex_ray, :request_id_extractor)

      defp get_opentracing_tags(ctx, predefined_tags) do
        tags = predefined_tags ++ [
          "span.kind": "server",
          type: ctx.meta[:type] || nil,
          hostname: System.get_env("HOSTNAME") || nil,
          rts_id: System.get_env("RTS_ID") || nil,
          rts_host: System.get_env("RTS_HOST") || nil,
          cts_id: System.get_env("CCS_ID") || nil,
          cts_host: System.get_env("CCS_HOST") || nil,
          cts: System.get_env("CTS") || nil,
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
        if trace_enabled? do
          predefined_tags = Application.get_env(:ex_ray, :predefined_tags, [])
          # list of available tags
          tags = get_opentracing_tags(ctx, predefined_tags)
          Logger.debug(fn -> ">>> Starting span for `#{inspect ctx.target}" end)
          request_id = get_request_id(ctx)
          span = Span.open(ctx.target, request_id)
          span = Enum.reduce(tags, span, fn({tag, val}, acc) -> :otter.tag(acc, tag, val) end)
          if Application.get_env(:ex_ray, :logs_enabled, false) do
            :otter.log(span, ">>> #{inspect ctx.target} with args: #{inspect ctx.args}")
          else
            span
          end
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
        if trace_enabled? do
          Logger.debug(fn -> "<<< Closing span for `#{inspect ctx.target}" end)
          res =
            if Application.get_env(:ex_ray, :logs_enabled, false) do
              :otter.log(span, "<<< #{inspect ctx.target} returned #{inspect res}")
            else
              span
            end
          request_id = get_request_id(ctx)
          Span.close(res, request_id)
        end
      end

      @doc """
      Trying to determine request id from context (`ctx` param variable)
      """
      @spec get_request_id(map()) :: String.t | nil
      def get_request_id(ctx) when is_nil(@request_id_extractor), do: nil
      def get_request_id(ctx) do
        case @request_id_extractor.get_request_id(ctx) do
          {:ok, request_id} ->
            request_id
          {:error, ctx} ->
            if Application.get_env(:ex_ray, :debug, false) do
              st = Process.info(self(), :current_stacktrace)
              Logger.error("The request_id value is not found in the next args: #{inspect(ctx.args)}")
              Logger.error("Stacktrace: #{inspect(st)}")
            end
            if Application.get_env(:ex_ray, :raise_when_not_found, false) do
              raise ArgumentError, "The `request_id` value is missing in a request params"
            end
            nil
        end
      end

    end
  end
end
