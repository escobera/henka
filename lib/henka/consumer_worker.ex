defmodule Henka.ConsumerWorker do
  alias Henka
  use GenServer, restart: :transient

  def start_link(args) do
    GenServer.start_link(__MODULE__, args)
  end

  def init(state) do
    Process.send_after(self(), :consume, 1_000)
    {:ok, state}
  end

  def handle_info(:consume, state) do
    do_consume(state)
    |> case do
      {:ok, :end} -> {:stop, :normal, state}
      _ -> {:noreply, state}
    end
  end

  defp do_consume(state) do
    Henka.consume_events(state.henka_job_pid, state.consumer_chunk)
    |> case do
      [] ->
        case Henka.producing?(state.henka_job_pid) do
          true ->
            Process.send_after(self(), :consume, 50)

          false ->
            Henka.iniciar_finalizacao_da_carga(state.henka_job_pid)
            {:ok, :end}
        end

      events when is_list(events) ->
        state.consumer_function.(events, state.henka_job_meta)
        Process.send(self(), :consume, [])
    end
  end
end
