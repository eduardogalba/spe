defmodule SPETest do
  use ExUnit.Case
  doctest SPE
  test "Ejecucion correcta" do
    desc = %{
      "name" => "example",
      "tasks" => [
        %{
          "name" => "task1",
          "exec" => fn _ -> 1 + 2 end,
          "enables" => {"task3"}
        },
        %{
          "name" => "task2",
          "exec" => fn _ -> 3 + 4 end,
          "enables" => {"task4"}
        },
        %{
          "name" => "task3",
          "exec" => fn %{"task1" => v1} -> v1 + 2 end,
          "enables" => {"task5"}
        },
        %{
          "name" => "task4",
          "exec" => fn %{"task2" => v2} -> v2 * 3 end,
          "enables" => {"task5"}
        },
        %{
          "name" => "task5",
          "exec" => fn %{"task2" => v2, "task3" => v3, "task4" => v4} ->
            IO.puts("value: #{inspect(v2 + v3 + v4)}")
          end
        },
        %{
          "name" => "task6",
          "exec" => fn _ -> IO.puts("hello") end
        }
      ]
    }

    SPE.start_link([{:num_workers, 2}])

    {:ok, job_id} = SPE.submit_job(desc)

    SPE.start_job(job_id)

    Phoenix.PubSub.subscribe(SPE.PubSub, "#{inspect(job_id)}")

    receive_wait()
  end

  defp receive_wait() do
    receive do
      msg = {:spe, _, {_, :result, _}} ->
        IO.inspect(msg, label: "Test: ")
      msg = {:spe, _, {_, :task_terminated, _}} ->
        IO.inspect(msg, label: "Test: ")
        receive_wait()
    end
  end

end
