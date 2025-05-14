defmodule JobTest do
  use ExUnit.Case

  test "ejecución de un job con plan y dependencias" do
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

    job_id = make_ref()
    plan = [["task1", "task2"], ["task6", "task3"], ["task4", []], ["task5", []]]

    # Inicia el PubSub
    {:ok, _supv} = Supervisor.start_link([Phoenix.PubSub.child_spec(name: SPE.PubSub)], strategy: :one_for_one)

    # Suscríbete al topic del job
    Phoenix.PubSub.subscribe(SPE.PubSub, "#{inspect(job_id)}")

    job = %{
      id: job_id,
      plan: plan,
      desc: desc
    }

    {:ok, _pid} = Job.start_link(job)
  end
end
