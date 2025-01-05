# EasyMLOps

A turnkey MLOps pipeline demonstrating how to go from raw events to real-time predictions at scale. This project leverages modern data engineering and ML platforms—including Kafka, Flink, Redis, Ray, and more—to provide an end-to-end solution for data ingestion, validation, training, serving, and observability.

## Architecture Overview

![EasyMLOps Architecture](./docs/pipeline.png)

The system is composed of four main pipelines—Data, Training, Serving, and Observability—along with a Dev Environment and a Model Registry.

---

### 1. Data Pipeline

1. **Producers**
   Multiple producers emit raw events to the **Kafka `Raw Events Topic`**.

2. **Validation Service**
   A Flink job consumes raw events, validates their schema, and routes them accordingly:

   - **`Validated Topic`** for valid data
   - **`Invalidated Topic`** for invalid data

3. **DataLake and Online Store**

   - **Validated Topic → DataLake (MinIO)**
     For permanent storage and downstream batch processing.
   - **Validated Topic → Online Store (Redis)**
     For real-time feature retrieval.

4. **Data Warehouse (Offline Store)**
   Orchestrated by **Airflow DAGs**, which:
   - Ingest raw data from the DataLake
   - Perform quality checks and transformations
   - Create dimension, fact, and feature tables
   - Re-check data quality on the final “gold” tables

---

### 2. Training Pipeline

1. **Distributed Training with Ray**

   - `get_data`: Pulls features from the Data Warehouse
   - `hyper_parameter_tuning`: Conducts hyperparameter tuning using Ray Tune
   - `train_final_model`: Trains the final model with the best hyperparameters

2. **Model Registry**

   - **MLflow and MinIO** store model weights, metrics, and artifacts.

3. **Manual Training Option**
   - Jupyter notebooks (`notebook/train.ipynb`) allow a custom or ad hoc workflow.

---

### 3. Serving Pipeline

1. **Real-Time Inference**

   - **Ray Serve** hosts an **XGBoost** model.
   - Model checkpoints are loaded from the **Model Registry**.
   - Real-time feature retrieval comes from **Redis**.
   - Predictions are served to the **Product App** via **NGINX**.

2. **Scalability & Performance**
   - Ray Serve scales horizontally to handle high-throughput requests.
   - NGINX acts as a reverse proxy to route traffic efficiently.

---

### 4. Observability

1. **OpenTelemetry Collector**
   Aggregates telemetry data (metrics, traces, logs) from the Serving Pipeline.

2. **SigNoz**
   - Receives data exported from the OpenTelemetry Collector.
   - Provides a centralized dashboard for system health and performance monitoring.

---

### Dev Environment

- **Jupyter Notebooks** for data exploration, ad hoc experimentation, and manual model development.
- Integration with the rest of the pipeline (Data, Training, Serving) ensures an end-to-end experience even in development.

---

## Usage

All available commands can be found in the `Makefile`.

---

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests to improve the Data Pipeline, Training Pipeline, Serving Pipeline, or Observability stack.

---

## License

This project is provided under an open-source license. See the [LICENSE](LICENSE) file for details.
