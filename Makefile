.PHONY: build deps.get deps.upgrade compile lint test repl

all: build

########### Elixir #########
# 构建项目
build: clean deps.get compile

deps.get:
	@mix deps.get
	@echo "Dependencies installed."

deps.upgrade:
	@mix deps.update --all
	@echo "Dependencies updated."

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