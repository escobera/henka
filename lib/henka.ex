defmodule Henka do
  use GenServer

  alias Henka.{ConsumerWorker, ConsumerSupervisor}

  @impl true
  def init(options) do
    default_options = %{
      status: :started,
      queue: :queue.new(),
      henka_job_pid: self(),
      begin_hook: & &1,
      end_hook: & &1,
      ack_hook: & &1,
      min_queue: 2_000,
      max_queue: 20_000,
      producer_pid: nil,
      last_processed_event_id: 0,
      number_of_consumers: 10,
      consumer_chunk: 100,
      queue_size: 0,
      meta: %{}
    }

    merged_options = Map.merge(default_options, options)

    initial_state = merged_options.begin_hook.(merged_options)

    {:ok, initial_state, {:continue, :start_producing_events}}
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
    GenServer.call(pid, :state_summary, 10_000)
  end

  def enqueue_events(events, %{queue: queue} = state) do
    {queue, last_event} =
      Enum.reduce(events, {queue, nil}, fn evento, {queue, _ultima_evento} ->
        {:queue.in(evento, queue), evento}
      end)

    state.ack_hook.(state)

    state
    |> Map.put(:queue, queue)
    |> Map.put(:last_processed_event_id, last_processed_event_id(last_event, state))
  end

  defp last_processed_event_id(evt, %{
         last_processed_event_id_field: last_processed_event_id_field
       }),
       do: Map.get(evt, last_processed_event_id_field)

  defp last_processed_event_id(evt, _), do: evt

  defp produce(%{status: :consuming_stopped} = state), do: state
  defp produce(%{status: :ending_job} = state), do: state

  defp produce(%{producer: producer, queue: fila, producer_pid: producer_pid} = state) do
    case producer_running?(producer_pid) do
      true ->
        state

      false ->
        {:ok, task_pid} =
          Task.start_link(fn ->
            case :queue.len(fila) do
              size when size > state.max_queue ->
                state

              _ ->
                producer.(state)
                |> case do
                  [] ->
                    GenServer.cast(state.henka_job_pid, {:change_status, :consuming_stopped})

                  eventos ->
                    GenServer.cast(state.henka_job_pid, {:enqueue, eventos})
                end
            end
          end)

        %{state | producer_pid: task_pid}
    end
  end

  defp producer_running?(nil), do: false
  defp producer_running?(pid), do: Process.alive?(pid)

  def running? do
    running_consumers() > 0
  end

  def producing?(pid) do
    status_atom(pid) == :producing_events
  end

  defp status_atom(pid) do
    status(pid) |> Map.get(:status)
  end

  def dequeue(queue, qty), do: do_dequeue(queue, {qty, []})

  defp do_dequeue(queue, {n, acc}) when n > 0 do
    case :queue.out(queue) do
      {{:value, e}, rest} ->
        do_dequeue(rest, {n - 1, [e | acc]})

      {:empty, queue} ->
        {Enum.reverse(acc), queue}
    end
  end

  defp do_dequeue(queue, {_, acc}), do: {Enum.reverse(acc), queue}

  def consume_events(pid, quantity) do
    GenServer.call(pid, {:dequeue, quantity})
  end

  def iniciar_finalizacao_da_carga(pid) do
    GenServer.cast(pid, :start_ending_job)
  end

  defp running_consumers() do
    Process.whereis(Henka.ConsumerSupervisor)
    |> Supervisor.count_children()
    |> Map.get(:workers)
  end

  defp present_state_summary(state) do
    %{
      status: state.status,
      henka_job_pid: state.henka_job_pid,
      producer_pid: state.producer_pid,
      last_processed_event_id: state.last_processed_event_id,
      queue_size: :queue.len(state.queue),
      meta: state.meta
    }
  end

  ####
  #  CALLBACKS
  ####

  @impl true
  def handle_continue(:start_producing_events, state) do
    Process.send_after(self(), :produce, 10)
    {:noreply, %{state | status: :producing_events}, {:continue, :start_consumers}}
  end

  @impl true
  def handle_continue(:start_consumers, state) do
    for _i <- 1..state.number_of_consumers do
      DynamicSupervisor.start_child(
        ConsumerSupervisor,
        {ConsumerWorker,
         %{
           henka_job_pid: state.henka_job_pid,
           producer_pid: state.producer_pid,
           consumer_function: state.consumer,
           consumer_chunk: state.consumer_chunk,
           henka_job_meta: state.meta
         }}
      )
    end

    {:noreply, state}
  end

  @impl true
  def handle_cast(:start_ending_job, state) do
    case state.status == :ending_job do
      true ->
        {:noreply, state}

      _ ->
        Process.send_after(state.henka_job_pid, :maybe_end_job, 500)
        {:noreply, %{state | status: :ending_job}}
    end
  end

  @impl true
  def handle_cast({:enqueue, events}, state) do
    {:noreply, enqueue_events(events, state)}
  end

  @impl true
  def handle_cast({:change_status, status}, state) do
    {:noreply, %{state | status: status}}
  end

  @impl true
  def handle_call(:state_summary, _from, state) do
    {:reply, present_state_summary(state), state}
  end

  @impl true
  def handle_call({:dequeue, qty}, _from, state) do
    case :queue.len(state.queue) < state.min_queue do
      true ->
        Process.send(self(), :produce, [])
        {items, rest} = dequeue(state.queue, qty)
        {:reply, items, %{state | queue: rest}}

      false ->
        {items, rest} = dequeue(state.queue, qty)
        {:reply, items, %{state | queue: rest}}
    end
  end

  @impl true
  def handle_info(:maybe_end_job, state) do
    case running_consumers() do
      0 ->
        state = state.end_hook.(state)
        {:stop, :normal, state}

      i when i < state.number_of_consumers ->
        Process.send_after(self(), :maybe_end_job, 1_000)
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(:produce, state) do
    {:noreply, produce(state)}
  end
end
