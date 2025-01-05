# EasyMLOps

![EasyMLOps Architecture](./docs/pipeline.png)

## Current Flows

### Data Pipeline

- `Producers` act as sources of raw events and produce data to the Kafka `Raw Events Topic`.

- A `Validation Service` (Flink job) consumes data from the `Raw Events Topic`, validates schema, and publishes to either:

  - The `Validated Topic` for valid data
  - The `Invalidated Topic` for invalid data

- The `Validated Topic` is synchronized with:

  - The `DataLake` (MinIO) for persistent storage and further processing
  - The `Online Store` (Redis) for real-time feature serving

- The Data Warehouse (Offline Store) processing workflow is orchestrated by `Airflow DAGs`:
  - Ingest raw data from the DataLake
  - Quality check raw data
  - Transform data
  - Create dimension, fact, and feature tables
  - Quality check gold data

### Training Pipeline

- The `Ray Train` and `Ray Tune` framework handles distributed model training:

  - get_data: Pulls features from the Data Warehouse
  - hyper_parameter_tuning: Hyperparameter tuning
  - train_final_model: Train the final model with the best hyperparameters
  - the weights and metrics are stored in the Model Registry (MLflow and MinIO)

- Beside the automatic training pipeline, we also have a manual training pipeline in `notebook/train.ipynb`

### Serving Pipeline

- The `XGBoost` model is served by `RayServe`:
  - Loads model checkpoints from Model Registry
  - Pulls real-time features from the Online Store (Redis)
  - Serves predictions to the Product App

### Observability

- OpenTelemetry Collector gathers telemetry data from the serving pipeline
- SigNoz serves as the backend for the observability platform
  - Collects metrics, traces, and logs exported from OpenTelemetry
  - Provides visibility into system performance and health

## Usage

Check the `Makefile` for all the commands you can use.
