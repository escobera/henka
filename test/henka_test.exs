defmodule HenkaTest do
  use ExUnit.Case, async: true

  defmodule MockProducer do
    use Agent

    def start_link(initial_value) do
      Agent.start_link(fn -> initial_value end, name: __MODULE__)
    end

    def produce_events(%{last_processed_event_id: _last_processed_event_id}) do
      Agent.get_and_update(__MODULE__, fn state ->
        events = Enum.take(state, 100)
        rest = Enum.filter(state, fn state -> state > List.last(events) end)
        {events, rest}
      end)
    end
  end

  defmodule MockConsumer do
    use Agent

    def start_link(initial_value) do
      Agent.start_link(fn -> initial_value end, name: __MODULE__)
    end

    def value do
      Agent.get(__MODULE__, & &1)
    end

    def consume(chunk, _meta) do
      Enum.map(chunk, fn item -> Agent.update(__MODULE__, fn state -> [item | state] end) end)
    end
  end

  defmodule MockDB do
    use Agent

    def start_link(initial_value) do
      Agent.start_link(fn -> initial_value end, name: __MODULE__)
    end

    def push_value(value) do
      Agent.update(__MODULE__, fn state -> [value | state] end)
    end

    def value do
      Agent.get(__MODULE__, & &1)
    end
  end

  test "Henka.run" do
    HenkaTest.MockConsumer.start_link([])
    HenkaTest.MockProducer.start_link(Enum.to_list(1..1000))

    {:ok, pid} =
      Henka.run(%{
        producer: &MockProducer.produce_events/1,
        consumer: &HenkaTest.MockConsumer.consume/2,
        number_of_consumers: 10,
        last_processed_event_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert Enum.sort(HenkaTest.MockConsumer.value()) == Enum.to_list(1..1000)
  end

  test "end_hook is called at the end" do
    HenkaTest.MockProducer.start_link(Enum.to_list(1..1000))
    HenkaTest.MockConsumer.start_link([])
    HenkaTest.MockDB.start_link(nil)

    {:ok, pid} =
      Henka.run(%{
        producer: &MockProducer.produce_events/1,
        consumer: &HenkaTest.MockConsumer.consume/2,
        number_of_consumers: 10,
        end_hook: fn _state ->
          Agent.update(HenkaTest.MockDB, fn _state -> "end hook called!" end)
        end,
        last_processed_event_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert HenkaTest.MockDB.value() == "end hook called!"
    assert Enum.sort(HenkaTest.MockConsumer.value()) == Enum.to_list(1..1000)
  end

  test "begin_hook is called at the start of the job" do
    HenkaTest.MockProducer.start_link(Enum.to_list(1..1000))
    HenkaTest.MockConsumer.start_link([])
    HenkaTest.MockDB.start_link(nil)

    {:ok, pid} =
      Henka.run(%{
        producer: &MockProducer.produce_events/1,
        consumer: &HenkaTest.MockConsumer.consume/2,
        number_of_consumers: 10,
        begin_hook: fn state ->
          Agent.update(HenkaTest.MockDB, fn _state -> "begin hook called!" end)
          state
        end,
        last_processed_event_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert HenkaTest.MockDB.value() == "begin hook called!"
    assert Enum.sort(HenkaTest.MockConsumer.value()) == Enum.to_list(1..1000)
  end

  test "ack_hook is called at the start of the job" do
    HenkaTest.MockProducer.start_link(Enum.to_list(1..1000))
    HenkaTest.MockConsumer.start_link([])
    HenkaTest.MockDB.start_link([])

    {:ok, pid} =
      Henka.run(%{
        producer: &MockProducer.produce_events/1,
        consumer: &HenkaTest.MockConsumer.consume/2,
        number_of_consumers: 10,
        ack_hook: fn %{last_processed_event_id: last_processed_event_id} ->
          HenkaTest.MockDB.push_value(last_processed_event_id)
        end,
        last_processed_event_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert Enum.reverse(HenkaTest.MockDB.value()) == [0, 100, 200, 300, 400, 500, 600, 700, 800, 900]
    assert Enum.sort(HenkaTest.MockConsumer.value()) == Enum.to_list(1..1000)
  end
end
