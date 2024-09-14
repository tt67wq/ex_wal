.PHONY: build deps.get deps.upgrade compile lint test repl

all: help

help:
	@echo "Usage: make <target>"
	@echo "Targets:"
	@echo "  build - Build the project"
	@echo "  deps.get - Install dependencies"
	@echo "  deps.upgrade - Upgrade dependencies"
	@echo "  deps.purge - Purge dependencies"
	@echo "  compile - Compile the project"
	@echo "  lint - Lint the code"
	@echo "  test - Run tests"
	@echo "  repl - Start an interactive Elixir shell"
	@echo "  upload - Publish the project to Hex"
	@echo "  clean - Clean the project"
	@echo "  benchmark - Run benchmarks"

########### Elixir #########
# 构建项目
build: clean deps.get compile

deps.get:
	@mix deps.get
	@echo "Dependencies installed."

deps.upgrade:
	@mix deps.update --all
	@echo "Dependencies updated."

deps.purge:
	@rm -rf deps
	@rm -rf _build
	@rm -rf .elixir_ls
	@echo "Dependencies purged."

compile:
	@mix compile

# 代码质量检查
lint:
	@mix format --check-formatted

# 格式化代码
fmt:
	@mix format

# 运行测试
test:
	@mix test

clean:
	@mix clean
	@echo "Cleaning complete."

# 交互式Elixir Shell
repl:
	@iex -S mix

upload:
	@mix hex.publish


benchmark:
	@mix run benchmarks/write.exs
	@mix run benchmarks/write_batch.exs