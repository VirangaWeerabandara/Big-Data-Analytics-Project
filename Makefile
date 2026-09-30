SHELL := /bin/bash
COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U ward -d ward
VENV := .venv

.DEFAULT_GOAL := help
.PHONY: help env check-env build up down restart ps logs clean migrate psql \
        topics clock-show clock-pause spark-smoke venv test

help: ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

# ----------------------------------------------------------------- setup
env: ## Create .env from .env.example with generated secrets
	python3 scripts/init_env.py

check-env:
	@test -f .env || { echo "No .env found - run 'make env' first."; exit 1; }

build: check-env ## Build the project images
	$(COMPOSE) build

# ------------------------------------------------------------- lifecycle
up: check-env ## Start the whole stack (resumes the simulated clock)
	$(COMPOSE) up -d --build
	@$(COMPOSE) ps

down: ## Pause the simulated clock, then stop the stack (data is kept)
	-@$(PSQL) -c "UPDATE sim_clock SET paused_at = now(), updated_at = now() WHERE paused_at IS NULL;" >/dev/null 2>&1 \
		&& echo "sim clock paused"
	$(COMPOSE) down

restart: down up ## Stop and start again

ps: ## Show service status
	$(COMPOSE) ps -a

logs: ## Follow logs (optionally: make logs s=api)
	$(COMPOSE) logs -f --tail=100 $(s)

clean: ## Stop and DELETE all data (volumes, checkpoints, landing files, reports)
	@read -p "This deletes all pipeline data. Continue? [y/N] " ans && [ "$$ans" = "y" ]
	$(COMPOSE) down -v --remove-orphans
	find data reports -type f ! -name .gitkeep -delete
	find data -mindepth 2 -type d -empty -delete

# ------------------------------------------------------------- utilities
migrate: ## Re-apply db/init (idempotent) after adding new SQL files
	$(COMPOSE) exec -T postgres sh /docker-entrypoint-initdb.d/00_init.sh

psql: ## Open a psql shell on the ward database
	$(COMPOSE) exec postgres psql -U ward -d ward

topics: ## Describe Kafka topics
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe

clock-show: ## Print the shared simulated clock
	$(COMPOSE) run --rm --no-deps clock-init python -m ward_common.sim_clock show

clock-pause: ## Freeze the simulated clock (resumed by the next `make up`)
	$(COMPOSE) run --rm --no-deps clock-init python -m ward_common.sim_clock pause

spark-smoke: ## Run the Spark connectivity smoke check
	$(COMPOSE) run --rm spark

# ----------------------------------------------------------------- tests
venv: ## Create a local virtualenv for tests
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q --upgrade pip
	$(VENV)/bin/pip install -q -r requirements-dev.txt

test: ## Run the unit tests
	$(VENV)/bin/pytest -q
