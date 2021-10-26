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
      {:ok, :fim} -> {:stop, :normal, state}
      _ -> {:noreply, state}
    end
  end

  defp do_consume(state) do
    Henka.recuperar_entidades_da_fila(state.etl_pid, state.consumer_chunk)
    |> case do
      [] ->
        case Henka.enfileirando?(state.etl_pid) do
          true ->
            Process.send_after(self(), :consume, 50)

          false ->
            Henka.iniciar_finalizacao_da_carga(state.etl_pid)
            {:ok, :fim}
        end

      entidades when is_list(entidades) ->
        state.consumer_function.(entidades, state.etl_meta)
        Process.send(self(), :consume, [])
    end
  end
end
