defmodule Henka do
  use GenServer

  alias Henka.{ConsumerWorker, ConsumerSupervisor}

  @fila_maxima 20_000
  @fila_minima 2_000

  @impl true
  def init(options) do
    default_options = %{
      status: :started,
      queue: :queue.new(),
      henka_ref: self(),
      begin_hook: & &1,
      end_hook: & &1,
      ack_hook: & &1,
      producer_pid: nil,
      number_of_consumers: 10,
      consumer_chunk: 100,
      queue_size: 0,
      meta: %{}
    }

    merged_options = Map.merge(default_options, options)

    initial_state = merged_options.begin_hook.(merged_options)

    {:ok, initial_state, {:continue, :start_enqueuing}}
  end

  def run(opts) do
    case running?() do
      true ->
        {:error, "There's a running Henka job on pid #{inspect(Process.whereis(__MODULE__))}"}

      false ->
        GenServer.start_link(__MODULE__, opts, name: __MODULE__, spawn_opt: [fullsweep_after: 0])
    end
  end

  def status(pid) do
    GenServer.call(pid, :status, 10_000)
  end

  def enfileirar_eventos(eventos, %{queue: fila} = state) do
    {fila, ultimo_evento} =
      Enum.reduce(eventos, {fila, nil}, fn evento, {fila, _ultima_evento} ->
        {:queue.in(evento, fila), evento}
      end)

    state.ack_hook.(state)

    state
    |> Map.put(:queue, fila)
    |> Map.put(:ultimo_id, get_ultimo_id(ultimo_evento, state))
  end

  defp get_ultimo_id(evt, %{campo_ultimo_id: campo_ultimo_id}), do: Map.get(evt, campo_ultimo_id)
  defp get_ultimo_id(evt, _), do: evt

  defp produzir_mais(%{status: :enfileiramento_finalizado} = state), do: state
  defp produzir_mais(%{status: :finalizando_carga} = state), do: state

  defp produzir_mais(%{producer: producer, queue: fila, producer_pid: producer_pid} = state) do
    case produzindo?(producer_pid) do
      true ->
        state

      false ->
        {:ok, task_pid} =
          Task.start_link(fn ->
            case :queue.len(fila) do
              tamanho when tamanho > @fila_maxima ->
                state

              _ ->
                producer.(state)
                |> case do
                  [] ->
                    GenServer.cast(state.henka_ref, {:alterar_status, :enfileiramento_finalizado})

                  eventos ->
                    GenServer.cast(state.henka_ref, {:enfileirar, eventos})
                end
            end
          end)

        %{state | producer_pid: task_pid}
    end
  end

  defp produzindo?(nil), do: false

  defp produzindo?(pid) do
    case Process.info(pid) do
      nil -> false
      _ -> true
    end
  end

  def running? do
    running_consumers() > 0
  end

  def enfileirando?(pid) do
    status_atom(pid) == :enfileirando_entidades
  end

  defp status_atom(pid) do
    status(pid) |> Map.get(:status)
  end

  def retirar(fila, qtd), do: do_retirar(fila, {qtd, []})

  defp do_retirar(fila, {n, acc}) when n > 0 do
    case :queue.out(fila) do
      {{:value, e}, resto} ->
        do_retirar(resto, {n - 1, [e | acc]})

      {:empty, fila} ->
        {Enum.reverse(acc), fila}
    end
  end

  defp do_retirar(fila, {_, acc}), do: {Enum.reverse(acc), fila}

  def recuperar_entidades_da_fila(pid, quantidade) do
    GenServer.call(pid, {:retirar, quantidade})
  end

  def iniciar_finalizacao_da_carga(pid) do
    GenServer.cast(pid, :iniciar_finalizacao_da_carga)
  end

  defp running_consumers() do
    Process.whereis(Henka.ConsumerSupervisor)
    |> Supervisor.count_children()
    |> Map.get(:workers)
  end

  ####
  #  CALLBACKS
  ####

  @impl true
  def handle_continue(:start_enqueuing, state) do
    Process.send_after(self(), :produzir, 10)
    {:noreply, %{state | status: :enfileirando_entidades}, {:continue, :consumir_fila}}
  end

  @impl true
  def handle_continue(:consumir_fila, state) do
    for _i <- 1..state.number_of_consumers do
      DynamicSupervisor.start_child(
        ConsumerSupervisor,
        {ConsumerWorker,
         %{
           etl_pid: state.henka_ref,
           producer_pid: state.producer_pid,
           consumer_function: state.consumer,
           consumer_chunk: state.consumer_chunk,
           etl_meta: state.meta
         }}
      )
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast(:iniciar_finalizacao_da_carga, state) do
    case state.status == :finalizando_carga do
      true ->
        {:noreply, state}

      _ ->
        Process.send_after(state.henka_ref, :maybe_finalizar_carga, 1_000)
        {:noreply, %{state | status: :finalizando_carga}}
    end
  end

  @impl true
  def handle_cast({:enfileirar, eventos}, state) do
    {:noreply, enfileirar_eventos(eventos, state)}
  end

  @impl true
  def handle_cast({:alterar_status, status}, state) do
    {:noreply, %{state | status: status}}
  end

  @impl true
  def handle_call(:status, _from, state) do
    resumo_state =
      Map.delete(state, :queue)
      |> Map.put(:tamanho_fila, :queue.len(state.queue))

    {:reply, resumo_state, state}
  end

  @impl true
  def handle_call({:retirar, qtd}, _from, state) do
    case :queue.len(state.queue) < @fila_minima do
      true ->
        Process.send(self(), :produzir, [])
        {items, resto} = retirar(state.queue, qtd)
        {:reply, items, %{state | queue: resto}}

      false ->
        {items, resto} = retirar(state.queue, qtd)
        {:reply, items, %{state | queue: resto}}
    end
  end

  @impl true
  def handle_info(:maybe_finalizar_carga, state) do
    case running_consumers() do
      0 ->
        state = state.end_hook.(state)
        {:stop, :normal, state}

      i when i < state.number_of_consumers ->
        Process.send_after(self(), :maybe_finalizar_carga, 1_000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:produzir, state) do
    {:noreply, produzir_mais(state)}
  end
end
