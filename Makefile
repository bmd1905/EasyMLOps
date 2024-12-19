# Declare all phony targets
.PHONY: test producer consumer up down restart all clean logs help

# Configuration
KAFKA_COMPOSE_FILE := docker-compose.kafka.yaml
AIRFLOW_COMPOSE_FILE := src/airflow/docker-compose.airflow.yaml
DATA_LAKE_COMPOSE_FILE := docker-compose.data-lake.yaml
DWH_COMPOSE_FILE := docker-compose.dwh.yaml
ONLINE_STORE_COMPOSE_FILE := docker-compose.online-store.yaml
RAY_COMPOSE_FILE := src/ray/docker-compose.ray.yaml
MONITOR_COMPOSE_FILE := docker-compose.monitor.yaml
MLFLOW_COMPOSE_FILE := docker-compose.mlflow.yaml
PYTHON := python3

# Data Ingestion and Streaming Tests
producer:
	uv run $(PYTHON) src/producer/produce.py -b=localhost:9092 -s=http://localhost:8081

consumer:
	uv run $(PYTHON) -m src.streaming.main schema_validation

deploy_s3_connector:
	uv run $(PYTHON) -m src.streaming.connectors.deploy_s3_connector

alert_invalid_events:
	uv run $(PYTHON) -m src.streaming.main alert_invalid_events


# Docker Compose Commands
up-network:
	docker network create easydatapipeline_default

up-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) up -d --build

up-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) up -d --build

up-data-lake:
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) up -d --build

up-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) up -d --build

up-online-store:
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) up -d --build

up-ray-cluster:
	docker build -t raytest ./src/ray
	docker compose -f $(RAY_COMPOSE_FILE) up -d --build

up-monitor:
	docker compose -f $(MONITOR_COMPOSE_FILE) up -d --build

up-mlflow:
	docker compose -f $(MLFLOW_COMPOSE_FILE) up -d --build

down-network:
	docker network rm easydatapipeline_default

down-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) down

down-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down

down-data-lake:
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) down

down-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) down

down-online-store:
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) down

down-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) down -v

down-monitor:
	docker compose -f $(MONITOR_COMPOSE_FILE) down -v

down-mlflow:
	docker compose -f $(MLFLOW_COMPOSE_FILE) down -v

restart-kafka: down-kafka up-kafka
restart-airflow: down-airflow up-airflow
restart-data-lake: down-data-lake up-data-lake
restart-dwh: down-dwh up-dwh
restart-online-store: down-online-store up-online-store
restart-ray-cluster: down-ray-cluster up-ray-cluster
restart-monitor: down-monitor up-monitor
restart-mlflow: down-mlflow up-mlflow

# Convenience Commands
up: up-network up-kafka up-airflow up-data-lake up-dwh up-online-store up-ray-cluster up-monitor up-mlflow deploy_s3_connector
down: down-kafka down-airflow down-data-lake down-dwh down-online-store down-ray-cluster down-monitor down-network
restart: down up

# Utility Commands
logs-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) logs -f

logs-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) logs -f

logs-data-lake:
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) logs -f

logs-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) logs -f

logs-online-store:
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) logs -f

logs-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) logs -f

logs-monitor:
	docker compose -f $(MONITOR_COMPOSE_FILE) logs -f

logs-mlflow:
	docker compose -f $(MLFLOW_COMPOSE_FILE) logs -f

clean:
	docker compose -f $(KAFKA_COMPOSE_FILE) down -v
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down -v
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) down -v
	docker compose -f $(DWH_COMPOSE_FILE) down -v
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) down -v
	docker compose -f $(RAY_COMPOSE_FILE) down -v
	docker compose -f $(MONITOR_COMPOSE_FILE) down -v
	docker compose -f $(MLFLOW_COMPOSE_FILE) down -v
	docker system prune -f

view-topics:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it broker kafka-topics --list --bootstrap-server broker:9092

view-schemas:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it schema-registry curl -X GET "http://localhost:8081/subjects"

view-consumer-groups:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it broker kafka-consumer-groups --bootstrap-server broker:9092 --list

# Help Command
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

setup-feature-store:
	cd src/feature_stores && bash run.sh

materialize-features:
	cd src/feature_stores && source .venv/bin/activate && python materialize_features.py

start-feature-service:
	cd src/feature_stores && source .venv/bin/activate && uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

test-feature-store:
	cd src/feature_stores && source .venv/bin/activate && python test_features.py

# Combined command to start everything
run-feature-store: up-online-store up-dwh setup-feature-store materialize-features start-feature-service
