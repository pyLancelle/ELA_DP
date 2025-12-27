# ============================================
# ELA DataPlatform - Makefile
# ============================================

.PHONY: help
.DEFAULT_GOAL := help

# Variables
DOCKER_IMAGE := ela-dp
GCLOUD_CONFIG := ~/.config/gcloud

# Colors for help
BLUE := \033[36m
RESET := \033[0m

# ============================================
# Help
# ============================================

help: ## Affiche cette aide
	@echo "$(BLUE)ELA DataPlatform - Commandes disponibles:$(RESET)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(BLUE)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(BLUE)Exemples:$(RESET)"
	@echo "  make dbt-run MODEL=pct_homepage__music_time_daily"
	@echo "  make dbt-test MODEL=pct_homepage__music_time_daily"
	@echo "  make dbt-build-all"
	@echo ""

# ============================================
# Docker
# ============================================

build: ## Build l'image Docker
	docker build -t $(DOCKER_IMAGE) .

build-no-cache: ## Build l'image Docker sans cache
	docker build --no-cache -t $(DOCKER_IMAGE) .

# ============================================
# dbt - Mode Docker
# ============================================

dbt-run: ## dbt run un modèle (usage: make dbt-run MODEL=mon_modele)
	@if [ -z "$(MODEL)" ]; then \
		echo "Erreur: MODEL requis. Usage: make dbt-run MODEL=mon_modele"; \
		exit 1; \
	fi
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=run \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-e DBT_SELECT="$(MODEL)" \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-test: ## dbt test un modèle (usage: make dbt-test MODEL=mon_modele)
	@if [ -z "$(MODEL)" ]; then \
		echo "Erreur: MODEL requis. Usage: make dbt-test MODEL=mon_modele"; \
		exit 1; \
	fi
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=test \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-e DBT_SELECT="$(MODEL)" \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-build: ## dbt build un modèle (run + test) (usage: make dbt-build MODEL=mon_modele)
	@if [ -z "$(MODEL)" ]; then \
		echo "Erreur: MODEL requis. Usage: make dbt-build MODEL=mon_modele"; \
		exit 1; \
	fi
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=build \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-e DBT_SELECT="$(MODEL)" \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-run-all: ## dbt run tous les modèles
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=run \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-test-all: ## dbt test tous les modèles
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=test \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-build-all: ## dbt build tous les modèles (run + test)
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=build \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-full-refresh: ## dbt full-refresh un modèle (usage: make dbt-full-refresh MODEL=mon_modele)
	@if [ -z "$(MODEL)" ]; then \
		echo "Erreur: MODEL requis. Usage: make dbt-full-refresh MODEL=mon_modele"; \
		exit 1; \
	fi
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=run \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-e DBT_SELECT="$(MODEL)" \
		-e DBT_FULL_REFRESH=true \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-compile: ## dbt compile (pour voir le SQL généré)
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=compile \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		$(if $(MODEL),-e DBT_SELECT="$(MODEL)",) \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

dbt-docs-generate: ## Génère la documentation dbt
	docker run --rm \
		-e MODE=dbt \
		-e DBT_COMMAND=docs \
		-e DBT_TARGET=$(or $(TARGET),dev) \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/src/dbt_dataplatform/target:/app/src/dbt_dataplatform/target \
		$(DOCKER_IMAGE) \
		docs generate

# ============================================
# dbt - Mode Local (sans Docker)
# ============================================

dbt-local-run: ## dbt run en local (usage: make dbt-local-run MODEL=mon_modele)
	cd src/dbt_dataplatform && uv run dbt run $(if $(MODEL),--select $(MODEL),) --target $(or $(TARGET),dev)

dbt-local-test: ## dbt test en local (usage: make dbt-local-test MODEL=mon_modele)
	cd src/dbt_dataplatform && uv run dbt test $(if $(MODEL),--select $(MODEL),) --target $(or $(TARGET),dev)

dbt-local-build: ## dbt build en local (usage: make dbt-local-build MODEL=mon_modele)
	cd src/dbt_dataplatform && uv run dbt build $(if $(MODEL),--select $(MODEL),) --target $(or $(TARGET),dev)

dbt-local-compile: ## dbt compile en local
	cd src/dbt_dataplatform && uv run dbt compile $(if $(MODEL),--select $(MODEL),) --target $(or $(TARGET),dev)

dbt-local-docs: ## Génère et sert la documentation dbt en local
	cd src/dbt_dataplatform && uv run dbt docs generate && uv run dbt docs serve

# ============================================
# Connectors - Fetch
# ============================================

fetch-garmin: ## Fetch Garmin data (usage: make fetch-garmin DAYS=7)
	docker run --rm \
		-e MODE=fetch \
		-e SERVICE=garmin \
		-e DAYS=$(or $(DAYS),7) \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		$(DOCKER_IMAGE)

fetch-spotify: ## Fetch Spotify data
	docker run --rm \
		-e MODE=fetch \
		-e SERVICE=spotify \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		$(DOCKER_IMAGE)

fetch-chess: ## Fetch Chess.com data
	docker run --rm \
		-e MODE=fetch \
		-e SERVICE=chess \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		$(DOCKER_IMAGE)

# ============================================
# Connectors - Ingest
# ============================================

ingest-garmin: ## Ingest Garmin data to BigQuery
	docker run --rm \
		-e MODE=ingest \
		-e SERVICE=garmin \
		-e ENV=dev \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		$(DOCKER_IMAGE)

ingest-spotify: ## Ingest Spotify data to BigQuery
	docker run --rm \
		-e MODE=ingest \
		-e SERVICE=spotify \
		-e ENV=dev \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		$(DOCKER_IMAGE)

ingest-chess: ## Ingest Chess.com data to BigQuery
	docker run --rm \
		-e MODE=ingest \
		-e SERVICE=chess \
		-e ENV=dev \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		$(DOCKER_IMAGE)

# ============================================
# API
# ============================================

api-local: ## Lance l'API en local (sans Docker)
	uv run uvicorn api.main:app --reload --port 8080

api-docker: ## Lance l'API avec Docker
	docker run --rm \
		-e MODE=api \
		-e PORT=8080 \
		-p 8080:8080 \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		$(DOCKER_IMAGE)

# ============================================
# Utilities
# ============================================

shell: ## Ouvre un shell dans le container Docker
	docker run --rm -it \
		-v $(GCLOUD_CONFIG):/root/.config/gcloud:ro \
		-v $(PWD)/.env:/app/.env:ro \
		--entrypoint /bin/bash \
		$(DOCKER_IMAGE)

clean: ## Nettoie les fichiers temporaires
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	cd src/dbt_dataplatform && rm -rf target/ dbt_packages/ logs/ 2>/dev/null || true

gcloud-auth: ## Authentifie gcloud (nécessaire pour BigQuery)
	gcloud auth application-default login

install: ## Installe les dépendances en local
	uv sync

install-dev: ## Installe les dépendances de dev en local
	uv sync --extra dev --extra dbt
