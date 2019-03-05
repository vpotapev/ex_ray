defmodule ExRay.Application do
  use Application

  import Supervisor.Spec

  def start(_type, _args) do
    children = [
      worker(ExRay.Store, []),
    ]

    opts = [strategy: :one_for_one, name: ExRay.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
