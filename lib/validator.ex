defmodule Validator do
  @moduledoc """
  **Validator** is responsible for validating job descriptions and tasks within the SPE system.
  It checks if the job description is a valid map, contains necessary fields, and ensures that tasks are properly defined.
  ## Usage:
  - Validate a job description
  ```elixir
  is_valid = Validator.valid_job?(job_desc)
  ```
  - The `job_desc` should be a map containing the job's name and tasks.
  ## Notes:
  - The Validator checks for the presence of required fields, data types, and relationships between tasks.
  - It ensures that task names are unique and that the `enables` field in tasks references valid task names.
  ## Dependencies:
  - `Logger`: For logging debug information during validation.
  """
  require Logger

  @doc """
  Validates a job description to ensure it meets the required structure and constraints.
  This function checks if the job description is a map, contains the necessary fields, and validates each task within the job.
  #### Parameters:
  - `job`: A map representing the job description, which should include fields like `name` and `tasks`.
  #### Returns:
  - `true` if the job description is valid, `false` otherwise.
  #### Example:
  ```elixir
  job_desc = %{
    "name" => "nisse",
    "tasks" => [
      %{
        "name" => "t0",
        "enables" => [],
        "exec" => fn _ -> 1 + 2 end,
        "timeout" => :infinity
      },
      %{
        "name" => "t1",
        "enables" => ["t0"],
        "exec" => fn _ -> 3 + 4 end,
        "timeout" => :infinity
      }
    ]
  }
  is_valid = Validator.valid_job?(job_desc)
  ```
  """
  def valid_job?(job) do
    Logger.debug("[Validator #{inspect(self())}]: Validating job...")

    with _ <- Logger.debug("[Validator #{inspect(self())}]: Is the description a map?"),
         true <- is_map(job),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Contains fields: name and tasks?"),
         true <- Map.has_key?(job, "name") and Map.has_key?(job, "tasks"),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Is the name a non-empty String?"),
         true <- is_bitstring(job["name"]) and String.length(job["name"]) > 0,
         _ <- Logger.debug("[Validator #{inspect(self())}]: Is the tasks field a list?"),
         true <- is_list(job["tasks"]) and !Enum.empty?(job["tasks"]),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Validating tasks..."),
         nil <- Enum.find(job["tasks"], &(!valid_task(&1))),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Are the tasks names unique?"),
         true <- unique_task_names?(job["tasks"]),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Are the enables field properly set?"),
         true <- valid_enable_tasks?(job["tasks"]) do
      Logger.debug("[Validator #{inspect(self())}]: Job validation passed.")
      true
    else
      _ ->
        Logger.error("[Validator #{inspect(self())}]: Job validation failed.")
        false
    end
  end

  defp unique_task_names?(tasks) do
    Logger.debug("[Validator #{inspect(self())}]: Checking for unique task names...")
    names = Enum.map(tasks, & &1["name"])
    result = Enum.uniq(names) == names

    if result do
      Logger.debug("[Validator #{inspect(self())}]: All task names are unique.")
    else
      Logger.error("[Validator #{inspect(self())}]: Duplicate task names found.")
    end

    result
  end

  defp valid_enable_tasks?(tasks) do
    Logger.debug("[Validator #{inspect(self())}]: Validating 'enables' field for tasks...")
    task_names = Enum.map(tasks, & &1["name"])

    result =
      Enum.all?(tasks, fn task ->
        case Map.get(task, "enables") do
          nil ->
            Logger.debug(
              "[Validator #{inspect(self())}]: Task #{inspect(task["name"])} has no 'enables' field."
            )

            true

          enables when is_list(enables) ->
            valid = Enum.all?(enables, &(&1 in task_names))

            if valid do
              Logger.debug(
                "[Validator #{inspect(self())}]: Task #{inspect(task["name"])} has valid 'enables' field."
              )
            else
              Logger.error(
                "[Validator #{inspect(self())}]: Task #{inspect(task["name"])} has invalid 'enables' field."
              )
            end

            valid

          _ ->
            Logger.error(
              "[Validator #{inspect(self())}]: Task #{inspect(task["name"])} has an invalid 'enables' field type."
            )

            false
        end
      end)

    if result do
      Logger.debug("[Validator #{inspect(self())}]: All tasks have valid 'enables' fields.")
    else
      Logger.error("[Validator #{inspect(self())}]: Some tasks have invalid 'enables' fields.")
    end

    result
  end

  defp valid_task(task) do
    Logger.debug("Validating task: #{inspect(task["name"])}")

    with _ <- Logger.debug("[Validator #{inspect(self())}]: Is the description a map?"),
         true <- is_map(task),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Contains fields: name and exec?"),
         true <- Map.has_key?(task, "name") and Map.has_key?(task, "exec"),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Is the name a non-empty String?"),
         true <- is_bitstring(task["name"]) and String.length(task["name"]) > 0,
         _ <- Logger.debug("[Validator #{inspect(self())}]: Is exec a function with arity 1?"),
         true <- is_function(task["exec"], 1),
         _ <-
           Logger.debug(
             "[Validator #{inspect(self())}]: Has it timeout? Is it :infinity or a positive integer?"
           ),
         true <-
           (Map.has_key?(task, "timeout") and
              (task["timeout"] == :infinity or
                 (is_integer(task["timeout"]) and task["timeout"] > 0))) or
             !Map.has_key?(task, "timeout"),
         _ <- Logger.debug("[Validator #{inspect(self())}]: Has it enables? Is it a list?"),
         true <-
           (Map.has_key?(task, "enables") and is_list(task["enables"])) or
             !Map.has_key?(task, "enables") do
      Logger.debug("Task validation passed for: #{inspect(task["name"])}")
      true
    else
      _ ->
        Logger.error("Task validation failed for: #{inspect(task["name"])}")
        false
    end
  end
end
