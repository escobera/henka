defmodule HenkaTest do
  use ExUnit.Case, async: true
  doctest Henka

  defmodule TestAgent do
    use Agent

    def start_link(initial_value) do
      Agent.start_link(fn -> initial_value end, name: __MODULE__)
    end

    def value do
      Agent.get(__MODULE__, & &1)
    end

    def adicionar(chunk, _meta) do
      Enum.map(chunk, fn item -> Agent.update(__MODULE__, fn state -> [item | state] end) end)
    end
  end

  test "Henka.run" do
    HenkaTest.TestAgent.start_link([])

    {:ok, pid} =
      Henka.run(%{
        producer: fn %{ultimo_id: ultimo_id} ->
          Enum.filter(Enum.to_list(1..1000), fn x -> x > ultimo_id end)
        end,
        consumer: &HenkaTest.TestAgent.adicionar/2,
        number_of_consumers: 10,
        ultimo_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert Enum.sort(HenkaTest.TestAgent.value()) == Enum.to_list(1..1000)
  end

  test "end_hook is called at the end" do
    HenkaTest.TestAgent.start_link([])

    {:ok, pid} =
      Henka.run(%{
        producer: fn %{ultimo_id: ultimo_id} ->
          Enum.filter(Enum.to_list(1..10), fn x -> x > ultimo_id end)
        end,
        consumer: &HenkaTest.TestAgent.adicionar/2,
        number_of_consumers: 10,
        end_hook: fn _state ->
          Agent.update(HenkaTest.TestAgent, fn _state -> "end hook called!" end)
        end,
        ultimo_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert HenkaTest.TestAgent.value() == "end hook called!"
  end

  test "begin_hook is called at the start of the job" do
    HenkaTest.TestAgent.start_link([])

    {:ok, pid} =
      Henka.run(%{
        producer: fn %{ultimo_id: ultimo_id} ->
          Enum.filter(Enum.to_list(1..10), fn x -> x > ultimo_id end)
        end,
        consumer: fn _item, _meta -> nil end,
        number_of_consumers: 10,
        begin_hook: fn state ->
          Agent.update(HenkaTest.TestAgent, fn _state -> "begin hook called!" end)
          state
        end,
        ultimo_id: 0
      })

    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, _, :normal}, 10000

    assert HenkaTest.TestAgent.value() == "begin hook called!"
  end
end
