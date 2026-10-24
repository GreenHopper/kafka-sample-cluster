# Single-node Kafka + Schema Registry sample cluster.
COMPOSE := docker compose

.PHONY: up down seed append verify reset logs test

up: ## start broker, registry, and run the one-shot seeding
	$(COMPOSE) up -d

down: ## stop the cluster, keep the data volumes
	$(COMPOSE) stop

seed: ## seed whatever is still missing (no-op when already seeded)
	$(COMPOSE) run --rm --no-deps seeder python seed_data.py seed

append: ## add one further catalogue batch on top of the existing data
	$(COMPOSE) run --rm --no-deps seeder python seed_data.py append

verify: ## health check plus per-topic count and one decoded record
	./scripts/healthcheck.sh
	$(COMPOSE) run --rm --no-deps seeder python readback.py

reset: ## DESTRUCTIVE: delete the cluster and all its data
	$(COMPOSE) down -v

logs: ## follow broker and registry logs
	$(COMPOSE) logs -f --tail=100

test: ## run the data-model unit tests in the seeder image
	$(COMPOSE) build seeder
	$(COMPOSE) run --rm --no-deps seeder python -m unittest discover -v
