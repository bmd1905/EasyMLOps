# Easy Data Pipeline

![Easy Data Pipeline Architecture](./docs/pipeline.png)


## Currect Flows

- The `Producer` acts as the source of raw events and produces data to the Kafka `Raw Events Topic`.

- The `Validation Flink job` consumes data from the `Raw Events Topic`, validates it, and publishes it to either the `Validated Topic` or the `Invalidated Topic` based on the outcome.

- The `Invalidated Topic` triggers an alerting system to notify stakeholders of invalid data.

- The `Validated Topic` is auto-synchronized with the `DataLake` for further processing.

- The `DataLake` stores validated data and serves as the central repository for downstream processing and analytics.

- `Airflow DAGs` orchestrate the following data processing steps:
    - Ingest raw data from the DataLake.
    - Validate and transform the data.
    - Create feature views, dimension tables, and fact tables in the PostgreSQL Data Warehouse.

- The `Ray Train` framework processes data for machine learning tasks, consisting of:
    - Load_data Task: Pulls data from the DataLake to prepare it for training.
    - Train_model Task: Trains a distributed model and stores checkpoints in MinIO for recovery or further analysis.

- The `Ray Cluster` scales model training across multiple workers to handle large datasets and computational loads.

- The `PostgreSQL Data Warehouse` serves as the final destination for transformed and processed data, supporting analytics and reporting.


## Usage

Check the `Makefile` for all the commands you can use.

```bash
# Start the Kafka cluster
make up-kafka

# Start the MinIO cluster
make up-minio

# Start the Data Warehouse cluster
make up-dwh

# Start the Airflow cluster
make up-airflow

# Start the Ray cluster
make up-ray-cluster
```

Deploy the S3 connector to the Kafka cluster.

```bash
make deploy-s3-connector
```

Start the consumer to validate the data.

```bash
make consumer
```
