# Declare all phony targets
.PHONY: test producer consumer up down restart all clean logs help

# Configuration
KAFKA_COMPOSE_FILE := docker-compose.kafka.yaml
AIRFLOW_COMPOSE_FILE := src/airflow/docker-compose.airflow.yaml
MINIO_COMPOSE_FILE := docker-compose.minio.yaml
DWH_COMPOSE_FILE := docker-compose.dwh.yaml
RAY_COMPOSE_FILE := src/ray/docker-compose.ray.yaml
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

up-minio:
	docker compose -f $(MINIO_COMPOSE_FILE) up -d --build

up-dwh:
	docker build -t raytest ./src/ray
	docker compose -f $(DWH_COMPOSE_FILE) up -d --build

up-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) up -d --build

down-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) down

down-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down

down-minio:
	docker compose -f $(MINIO_COMPOSE_FILE) down

down-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) down

down-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) down

restart-kafka: down-kafka up-kafka
restart-airflow: down-airflow up-airflow
restart-minio: down-minio up-minio
restart-dwh: down-dwh up-dwh
restart-ray-cluster: down-ray-cluster up-ray-cluster
# Convenience Commands
up: up-kafka up-airflow up-minio up-dwh
down: down-kafka down-airflow down-minio down-dwh
restart: down up

# Utility Commands
logs-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) logs -f

logs-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) logs -f

logs-minio:
	docker compose -f $(MINIO_COMPOSE_FILE) logs -f

logs-dwh:
	docker compose -f $(DWH_COMPOSE_FILE) logs -f

logs-ray-cluster:
	docker compose -f $(RAY_COMPOSE_FILE) logs -f

clean:
	docker compose -f $(KAFKA_COMPOSE_FILE) down -v
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down -v
	docker compose -f $(MINIO_COMPOSE_FILE) down -v
	docker compose -f $(DWH_COMPOSE_FILE) down -v
	docker compose -f $(RAY_COMPOSE_FILE) down -v
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
