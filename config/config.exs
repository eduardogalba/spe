import Config

config :logger, level: :info

config :spe, SPE.Repo,
  username: "postgres",
  password: "postgres",
  database: "spe",
  hostname: "localhost",
  pool_size: 10

config :spe, ecto_repos: [SPE.Repo]
