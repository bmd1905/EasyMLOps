.PHONY: test_producer test_consumer


test_producer:
	python3 test/data_ingestion/kafka_producer/produce_json.py
				

test_consumer:
	python3 -m src.streaming.main


start_airflow:
	docker compose -f docker-compose.airflow.yaml up -d

docker-compose-kafka-up:
	docker compose -f docker-compose.kafka.yaml up -d

docker-compose-kafka-down:
	docker compose -f docker-compose.kafka.yaml down

docker-compose-airflow-up:
	docker compose -f src/airflow/docker-compose.airflow.yaml up -d

docker-compose-airflow-down:
	docker compose -f src/airflow/docker-compose.airflow.yaml down

docker-compose-airflow-restart:
	docker compose -f src/airflow/docker-compose.airflow.yaml restart
