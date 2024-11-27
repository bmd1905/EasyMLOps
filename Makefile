# Declare all phony targets
.PHONY: test producer consumer up down restart all clean logs help

# Configuration
KAFKA_COMPOSE_FILE := docker-compose.kafka.yaml
AIRFLOW_COMPOSE_FILE := src/airflow/docker-compose.airflow.yaml
MINIO_COMPOSE_FILE := docker-compose.minio.yaml
PYTHON := python3

# Data Ingestion and Streaming Tests
producer:
	$(PYTHON) test/data_ingestion/kafka_producer/produce_json.py

consumer:
	$(PYTHON) -m src.streaming.main

# Docker Compose Commands
up-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) up -d

up-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) up -d

up-minio:
	docker compose -f $(MINIO_COMPOSE_FILE) up -d

down-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) down

down-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down

down-minio:
	docker compose -f $(MINIO_COMPOSE_FILE) down

restart-kafka: down-kafka up-kafka
restart-airflow: down-airflow up-airflow
restart-minio: down-minio up-minio

# Convenience Commands
up: up-kafka up-airflow up-minio
down: down-kafka down-airflow down-minio
restart: down up

# Utility Commands
logs-kafka:
	docker compose -f $(KAFKA_COMPOSE_FILE) logs -f

logs-airflow:
	docker compose -f $(AIRFLOW_COMPOSE_FILE) logs -f

logs-minio:
	docker compose -f $(MINIO_COMPOSE_FILE) logs -f

clean:
	docker compose -f $(KAFKA_COMPOSE_FILE) down -v
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down -v
	docker compose -f $(MINIO_COMPOSE_FILE) down -v
	docker system prune -f

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
