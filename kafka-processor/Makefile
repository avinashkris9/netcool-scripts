all: help
default: help
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
help:  ## Show help messages for make targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(firstword $(MAKEFILE_LIST)) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[32m%-30s\033[0m %s\n", $$1, $$2}'


setup: activate ## install packages
	pip install -r requirements.txt

fmt: ## format using ruff
	ruff format --line-length 210

activate: ## activate virtual env
	@echo "Run: pyenv shell kafka-processor or pyenv activate kafka-processor in your shell"

up: ## start docker compose

	docker compose down -v
	docker compose up -d --build

produce: up ## send kafka producer data
	./kafka_producer.py