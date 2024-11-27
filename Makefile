.PHONY: test_producer test_consumer


test_producer:
	python3 test/data_ingestion/kafka_producer/produce_json.py
				

test_consumer:
	python3 -m src.streaming.main


start_airflow:
	docker compose -f docker-compose.airflow.yaml up -d
