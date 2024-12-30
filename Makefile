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
CDC_COMPOSE_FILE := docker-compose.cdc.yaml
SERVING_COMPOSE_FILE := src/serving/docker-compose.serving.yaml
PYTHON := python3

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

up-cdc:
	docker compose -f $(CDC_COMPOSE_FILE) up -d --build

up-serving:
	docker compose -f $(SERVING_COMPOSE_FILE) up -d --build

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

down-cdc:
	docker compose -f $(CDC_COMPOSE_FILE) down -v

down-serving:
	docker compose -f $(SERVING_COMPOSE_FILE) down

restart-kafka: down-kafka up-kafka
restart-airflow: down-airflow up-airflow
restart-data-lake: down-data-lake up-data-lake
restart-dwh: down-dwh up-dwh
restart-online-store: down-online-store up-online-store
restart-ray-cluster: down-ray-cluster up-ray-cluster
restart-monitor: down-monitor up-monitor
restart-mlflow: down-mlflow up-mlflow
restart-cdc: down-cdc up-cdc
restart-serving: down-serving up-serving

# Convenience Commands
up: up-network up-kafka up-airflow up-data-lake up-dwh up-online-store up-ray-cluster up-monitor up-mlflow deploy_s3_connector
down: down-kafka down-airflow down-data-lake down-dwh down-online-store down-ray-cluster down-monitor down-network down-cdc down-serving
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

logs-cdc:
	docker compose -f $(CDC_COMPOSE_FILE) logs -f

logs-serving:
	docker compose -f $(SERVING_COMPOSE_FILE) logs -f

clean:
	docker compose -f $(KAFKA_COMPOSE_FILE) down -v
	docker compose -f $(AIRFLOW_COMPOSE_FILE) down -v
	docker compose -f $(DATA_LAKE_COMPOSE_FILE) down -v
	docker compose -f $(DWH_COMPOSE_FILE) down -v
	docker compose -f $(ONLINE_STORE_COMPOSE_FILE) down -v
	docker compose -f $(RAY_COMPOSE_FILE) down -v
	docker compose -f $(MONITOR_COMPOSE_FILE) down -v
	docker compose -f $(MLFLOW_COMPOSE_FILE) down -v
	docker compose -f $(CDC_COMPOSE_FILE) down -v
	docker compose -f $(SERVING_COMPOSE_FILE) down -v
	docker system prune -f

# ------------------------------------------ Utility Commands ------------------------------------------

# Kafka Commands
view-topics:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it broker kafka-topics --list --bootstrap-server broker:9092

view-schemas:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it schema-registry curl -X GET "http://localhost:8081/subjects"

view-consumer-groups:
	docker compose -f $(KAFKA_COMPOSE_FILE) exec -it broker kafka-consumer-groups --bootstrap-server broker:9092 --list

# ------------- Feature Store Commands
start-feature-store:
	cd src/feature_stores && ./run.sh && . .venv/bin/activate && python materialize_features.py && uvicorn api:app --host 0.0.0.0 --port 8002 --reload

materialize-features:
	cd src/feature_stores && . .venv/bin/activate && python materialize_features.py

start-feature-service:
	cd src/feature_stores && . .venv/bin/activate && uvicorn api:app --host 0.0.0.0 --port 8002 --reload

# ------------- Streaming Commands
producer:
	uv run $(PYTHON) src/producer/produce.py -b=localhost:9092 -s=http://localhost:8081

consumer:
	uv run $(PYTHON) -m src.streaming.main schema_validation

feature_calculation:
	uv run $(PYTHON) -m src.streaming.main feature_calculation

deploy_s3_connector:
	uv run $(PYTHON) -m src.streaming.connectors.deploy_s3_connector

alert_invalid_events:
	uv run $(PYTHON) -m src.streaming.main alert_invalid_events

# ------------- CDC Commands
cdc_setup:
	uv run bash src/cdc/run.sh register_connector src/cdc/configs/postgresql-cdc.json

insert_cdc_data:
	uv run $(PYTHON) -m src.cdc.create_table
	uv run $(PYTHON) -m src.cdc.insert_data

# ------------- Online Store Commands
ingest_stream_to_online_store:
	cd src/feature_stores && . .venv/bin/activate && python ingest_stream_to_online_store.py

# ------------- Help Command
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

# ------------------------------------------ Pipeline Commands ------------------------------------------

# Data pipeline
up-data-pipeline:
	make up-network
	make up-kafka
	make up-cdc
	make up-airflow
	make up-data-lake
	make up-dwh
	make cdc_setup
	make deploy_s3_connector

up-training-pipeline:
	make up-ray-cluster
	make up-mlflow

up-serving-pipeline:
	make up-ray-cluster
	make up-online-store
	make up-serving
