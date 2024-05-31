defmodule ExWal.MixProject do
  @moduledoc false
  use Mix.Project

  @name "ex_wal"
  @version "0.2.0"
  @repo_url "https://github.com/tt67wq/ex_wal"
  @description "ExWal is a project that aims to provide a solution for managing write-ahead log (WAL) in Elixir."

  def project do
    [
      app: :ex_wal,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      source_url: @repo_url,
      name: @name,
      package: package(),
      description: @description
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "examples", "benchmarks"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{
        "GitHub" => @repo_url
      }
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:nimble_options, "~> 1.1"},
      {:benchee, "~> 1.3", only: :dev},
      {:stream_data, "~> 0.5", only: :test},
      {:styler, "~> 0.11", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end
end
