# Declare all phony targets
.PHONY: test producer consumer up down restart all clean logs help db-start db-stop db-reset

# Configuration
KAFKA_COMPOSE_FILE := docker-compose.kafka.yaml
ORCHESTRATION_COMPOSE_FILE := ./src/orchestration/docker-compose.orchestration.yaml
DATA_LAKE_COMPOSE_FILE := docker-compose.data-lake.yaml
DWH_COMPOSE_FILE := docker-compose.dwh.yaml
ONLINE_STORE_COMPOSE_FILE := docker-compose.online-store.yaml
RAY_COMPOSE_FILE := docker-compose.ray.yaml
GRAFANA_COMPOSE_FILE := docker-compose.grafana.yaml
MODEL_REGISTRY_COMPOSE_FILE := docker-compose.model-registry.yaml
CDC_COMPOSE_FILE := docker-compose.cdc.yaml
SERVING_COMPOSE_FILE := docker-compose.serving.yaml
NGINX_COMPOSE_FILE := docker-compose.nginx.yaml
OBSERVABILITY_COMPOSE_FILE := docker-compose.observability.yaml
SUPERSET_COMPOSE_FILE := docker-compose.superset.yaml
PYTHON := python3

up-network:
	docker network create easymlops_network

# ------------------------------------------ Data and Training Pipeline Commands ------------------------------------------
up-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) up -d --build

up-cdc:
	docker compose -f $(CDC_COMPOSE_FILE) up -d --build

up-data-lake:
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) up -d --build

up-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) up -d --build

up-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) up -d --build

up-grafana:
	docker compose -f $(GRAFANA_COMPOSE_FILE) up -d --build

up-model-registry:
	docker compose -f $(MODEL_REGISTRY_COMPOSE_FILE) up -d --build

up-orchestration: up-ray-cluster up-model-registry up-grafana
	docker compose -f $(ORCHESTRATION_COMPOSE_FILE) up -d --build

# ------------------------------------------ Serving Pipeline Commands ------------------------------------------
up-online-store:
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) up -d --build

up-serving:
	docker compose -f $(SERVING_COMPOSE_FILE) up -d --build

up-nginx:
	docker compose -f $(NGINX_COMPOSE_FILE) up -d --build

up-observability:
	docker compose -f $(OBSERVABILITY_COMPOSE_FILE) up -d --build

up-superset:
	docker compose -f $(SUPERSET_COMPOSE_FILE) up -d --build

# ------------------------------------------ Down Commands ------------------------------------------
down-network:
	docker network rm easymlops_network

down-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) down

down-orchestration:
	docker compose -f $(ORCHESTRATION_COMPOSE_FILE) down

down-data-lake:
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) down

down-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) down

down-online-store:
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) down
	make start-feature-store

down-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) down -v

down-grafana:
	docker compose -f $(GRAFANA_COMPOSE_FILE) down -v

down-model-registry:
	docker compose -f $(MODEL_REGISTRY_COMPOSE_FILE) down -v

down-cdc:
	docker compose -f $(CDC_COMPOSE_FILE) down -v

down-serving:
	docker compose -f $(SERVING_COMPOSE_FILE) down

down-nginx:
	docker compose -f $(NGINX_COMPOSE_FILE) down -v

down-observability:
	docker compose -f $(OBSERVABILITY_COMPOSE_FILE) down -v

down-superset:
	docker compose -f $(SUPERSET_COMPOSE_FILE) down -v

# ------------------------------------------ Restart Commands ------------------------------------------
restart-kafka: down-kafka up-kafka
restart-orchestration: down-orchestration up-orchestration
restart-data-lake: down-data-lake up-data-lake
restart-dwh: down-dwh up-dwh
restart-online-store: down-online-store up-online-store
restart-ray-cluster: down-ray-cluster up-ray-cluster
restart-grafana: down-grafana up-grafana
restart-model-registry: down-model-registry up-model-registry
restart-cdc: down-cdc up-cdc
restart-serving: down-serving up-serving
restart-nginx: down-nginx up-nginx
restart-observability: down-observability up-observability
restart-superset: down-superset up-superset
# ------------------------------------------ Logs Commands ------------------------------------------
logs-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) logs -f

logs-orchestration:
	docker compose -f $(ORCHESTRATION_COMPOSE_FILE) logs -f

logs-data-lake:
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) logs -f

logs-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) logs -f

logs-online-store:
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) logs -f

logs-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) logs -f

logs-grafana:
	docker compose -f $(GRAFANA_COMPOSE_FILE) logs -f

logs-model-registry:
	docker compose -f $(MODEL_REGISTRY_COMPOSE_FILE) logs -f

logs-cdc:
	docker compose -f $(CDC_COMPOSE_FILE) logs -f

logs-serving:
	docker compose -f $(SERVING_COMPOSE_FILE) logs -f

logs-nginx:
	docker compose -f $(NGINX_COMPOSE_FILE) logs -f

logs-observability:
	docker compose -f $(OBSERVABILITY_COMPOSE_FILE) logs -f

logs-superset:
	docker compose -f $(SUPERSET_COMPOSE_FILE) logs -f

# ------------------------------------------ Clean Commands ------------------------------------------
clean:
	docker compose -f $(KAFKA_COMPOSE_FILE) down -v
	docker compose -f $(ORCHESTRATION_COMPOSE_FILE) down -v
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) down -v
	docker compose -f $(DWH_COMPOSE_FILE) down -v
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) down -v
	docker compose -f $(RAY_COMPOSE_FILE) down -v
	docker compose -f $(GRAFANA_COMPOSE_FILE) down -v
	docker compose -f $(MODEL_REGISTRY_COMPOSE_FILE) down -v
	docker compose -f $(CDC_COMPOSE_FILE) down -v
	docker compose -f $(SERVING_COMPOSE_FILE) down -v
	docker compose -f $(NGINX_COMPOSE_FILE) down -v
	docker compose -f $(OBSERVABILITY_COMPOSE_FILE) down -v
	docker compose -f $(SUPERSET_COMPOSE_FILE) down -v
	docker system prune -f

# ------------------------------------------ Utility Commands ------------------------------------------

# Kafka Commands
view-topics:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it broker kafka-topics --list --bootstrap-server broker:9092

view-schemas:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it schema-registry curl -X GET "http://localhost:8081/subjects"

view-consumer-groups:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it broker kafka-consumer-groups --bootstrap-server broker:9092 --list

deploy_s3_connector:
	uv run $(PYTHON) -m src.streaming.connectors.deploy_s3_connector

# ------------------------------------------ Feature Store Commands ------------------------------------------
start-feature-service:
	cd src/feature_stores && . .venv/bin/activate && uvicorn api:app --host 0.0.0.0 --port 8001 --reload

# ------------------------------------------ Streaming Commands ------------------------------------------
producer:
	uv run $(PYTHON) src/producer/produce.py -b=localhost:9092 -s=http://localhost:8081

schema_validation:
	uv run $(PYTHON) -m src.streaming.main schema_validation

alert_invalid_events:
	uv run $(PYTHON) -m src.streaming.main alert_invalid_events

ingest_stream:
	cd src/feature_stores && ./run.sh && . .venv/bin/activate && python ingest_stream.py

# ------------------------------------------ Help Command ------------------------------------------
help:
	@echo "Available commands:"
	@echo "  make producer         - Run Kafka producer test"
	@echo "  make consumer         - Run Kafka consumer test"
	@echo "  make up              - Start all services"
	@echo "  make down            - Stop all services"
	@echo "  make restart         - Restart all services"
	@echo "  make clean           - Remove all containers and volumes"
	@echo "  make logs-<service>  - View logs for specific service"
	@echo "  make view-<service>  - View specific service"
