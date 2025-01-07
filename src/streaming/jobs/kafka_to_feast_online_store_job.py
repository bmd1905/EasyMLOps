import os
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict

import pandas as pd
import requests
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import (
    SparkKafkaProcessor,
    SparkProcessorConfig,
)
from loguru import logger
from pyflink.datastream import StreamExecutionEnvironment
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from ..jobs.base import FlinkJob
from ..utils.metrics import RequestCounter

# Initialize request counter
request_counter = RequestCounter(name="kafka_to_feast_online_store")

# Disable specific warnings
warnings.filterwarnings(
    "ignore", category=RuntimeWarning, module="feast.stream_feature_view"
)
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="pyarrow.pandas_compat"
)
warnings.filterwarnings("ignore", category=UserWarning, module="feast.utils")

# Define the schema using PySpark's StructType
EVENT_SCHEMA = StructType(
    [
        StructField("event_timestamp", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("user_session", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("brand", StringType(), True),
        StructField("category_code_level1", StringType(), True),
        StructField("category_code_level2", StringType(), True),
        StructField("processed_at", StringType(), True),
    ]
)


class KafkaToFeastOnlineStoreJob(FlinkJob):
    def __init__(self):
        self.jars_path = f"{os.getcwd()}/src/streaming/connectors/config/jars/"
        self.kafka_bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.kafka_feature_topic = os.getenv(
            "KAFKA_FEATURES_TOPIC", "model.features.ready"
        )

        # Session management
        self.session_activity_counts = defaultdict(int)
        self.session_last_seen = defaultdict(datetime.now)
        self.session_timeout = timedelta(hours=24 * 365 * 10)

        # Initialize Spark
        self._init_spark()

        # Initialize Feature Store
        self.store = FeatureStore(repo_path=".")

    @property
    def job_name(self) -> str:
        return "kafka_to_feast_online_store"

    def _init_spark(self):
        """Initialize Spark session with proper configuration"""
        # Configure the SparkSession with Kafka packages
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
            "org.apache.spark:spark-avro_2.12:3.0.0 pyspark-shell"
        )

        self.spark = (
            SparkSession.builder.master("local[*]")
            .appName("feast-feature-ingestion")
            .config("spark.sql.shuffle.partitions", 8)
            .config("spark.streaming.kafka.maxRatePerPartition", 100)
            .config("spark.streaming.backpressure.enabled", True)
            .config("spark.kafka.bootstrap.servers", self.kafka_bootstrap_servers)
            .config("spark.kafka.subscribe", self.kafka_feature_topic)
            .config("spark.kafka.startingOffsets", "latest")
            .config("spark.kafka.failOnDataLoss", "false")
            .getOrCreate()
        )

    def verify_online_features(
        self, user_id: int, product_id: int, user_session: str
    ) -> Dict[str, Any]:
        """Verify features were written to online store for a specific record."""
        url = "http://localhost:8001/features"
        payload = {
            "user_id": user_id,
            "product_id": product_id,
            "user_session": user_session,
        }

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            features = response.json()
            logger.debug(f"Features found for user {user_id}: {features}")
            return features
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to verify features: {str(e)}")
            return None

    def cleanup_old_sessions(self):
        """Remove sessions that haven't been seen for a while"""
        current_time = datetime.now()
        expired_sessions = [
            session_id
            for session_id, last_seen in self.session_last_seen.items()
            if current_time - last_seen > self.session_timeout
        ]

        for session_id in expired_sessions:
            del self.session_activity_counts[session_id]
            del self.session_last_seen[session_id]

        if expired_sessions:
            logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")

    def preprocess_fn(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess the incoming feature data."""
        try:
            logger.info(f"Processing batch of size {len(df)}")

            # Extract payload fields if they exist
            if "payload." in df.columns[0]:
                df.columns = [col.replace("payload.", "") for col in df.columns]

            # Drop schema fields if they exist
            schema_cols = [col for col in df.columns if col.startswith("schema.")]
            if schema_cols:
                df = df.drop(columns=schema_cols)

            # Handle missing values first
            df["price"] = df["price"].fillna(0.0)
            df["event_type"] = df["event_type"].fillna("")
            df["user_id"] = df["user_id"].fillna(-1)
            df["brand"] = df["brand"].fillna("")
            df["category_code_level1"] = df["category_code_level1"].fillna("")
            df["category_code_level2"] = df["category_code_level2"].fillna("")

            # Convert timestamp to datetime if needed
            if df["event_timestamp"].dtype != "datetime64[ns]":
                df["event_timestamp"] = pd.to_datetime(
                    df["event_timestamp"],
                    format="%Y-%m-%d %H:%M:%S UTC",
                    utc=True,
                    errors="coerce",
                )
                df["event_timestamp"] = df["event_timestamp"].dt.tz_localize(None)

            # Fill any NaT timestamps with a default value
            df["event_timestamp"] = df["event_timestamp"].fillna(pd.Timestamp.min)

            # Calculate temporal features
            df["event_weekday"] = df["event_timestamp"].dt.weekday.astype("int64")

            # Update activity counts and last seen time for each session
            current_time = datetime.now()
            for session_id in df["user_session"].unique():
                self.session_activity_counts[session_id] += len(
                    df[df["user_session"] == session_id]
                )
                self.session_last_seen[session_id] = current_time

            # Cleanup old sessions periodically
            self.cleanup_old_sessions()

            # Add activity count from the session tracker
            df["activity_count"] = (
                df["user_session"].map(self.session_activity_counts).astype("int64")
            )

            # Calculate purchase flag
            df["is_purchased"] = (
                (df["event_type"].str.lower() == "purchase")
                .fillna(False)
                .astype("int64")
            )

            # Handle price
            df["price"] = df["price"].fillna(0.0).astype("float64")

            # Ensure all required columns exist with proper types
            required_features = {
                "event_timestamp": "datetime64[ns]",
                "user_id": "int64",
                "product_id": "int64",
                "activity_count": "int64",
                "price": "float64",
                "is_purchased": "int64",
                "event_weekday": "int64",
                "brand": "string",
                "category_code_level1": "string",
                "category_code_level2": "string",
            }

            # Convert types after handling all missing values
            for col, dtype in required_features.items():
                if col not in df.columns:
                    logger.error(f"Missing required column: {col}")
                    raise ValueError(f"Missing required column: {col}")

                # Handle missing values before type conversion
                if dtype == "int64":
                    df[col] = df[col].fillna(0)
                elif dtype == "float64":
                    df[col] = df[col].fillna(0.0)
                elif dtype == "string":
                    df[col] = df[col].fillna("")

                # Skip timestamp as it's already handled
                if col != "event_timestamp":
                    df[col] = df[col].astype(dtype)

            logger.info(f"Successfully processed {len(df)} records")

            # Verify first record in debug mode
            if len(df) > 0:
                sample_record = df.iloc[0]
                logger.debug("Verifying first record in online store...")
                features = self.verify_online_features(
                    user_id=int(sample_record["user_id"]),
                    product_id=int(sample_record["product_id"]),
                    user_session=str(sample_record["user_session"]),
                )
                if features:
                    logger.debug("✓ Record successfully written to online store")
                    logger.debug(
                        f"Activity count for session {sample_record['user_session']}: {sample_record['activity_count']}"
                    )
                else:
                    logger.warning("✗ Record not found in online store")

            return df

        except Exception as e:
            logger.error(f"Error preprocessing data: {str(e)}")
            logger.error(f"Column dtypes: {df.dtypes}")
            logger.error(
                f"Sample timestamp value: {df['event_timestamp'].iloc[0] if len(df) > 0 else 'No data'}"
            )
            raise

    def create_pipeline(self, env: StreamExecutionEnvironment):
        """Create the Flink pipeline for feature ingestion"""
        try:
            # Initialize the stream feature view
            sfv = self.store.get_stream_feature_view("streaming_features")

            # Define ingestion config
            ingestion_config = SparkProcessorConfig(
                mode="spark",
                source="kafka",
                spark_session=self.spark,
                processing_time="2 seconds",
                query_timeout=30,
            )

            # Initialize the processor with custom class
            processor = CustomSparkKafkaProcessor(
                config=ingestion_config,
                fs=self.store,
                sfv=sfv,
                preprocess_fn=self.preprocess_fn,
            )

            # Start ingestion to online store
            logger.info(
                f"Starting feature ingestion from topic {self.kafka_feature_topic}"
            )
            logger.debug("Using custom Spark Kafka processor")
            logger.debug(f"Bootstrap servers: {self.kafka_bootstrap_servers}")
            logger.debug(f"Topic: {self.kafka_feature_topic}")

            query = processor.ingest_stream_feature_view(PushMode.ONLINE)
            query.awaitTermination()

        except Exception as e:
            logger.error(f"Failed to start feature ingestion: {str(e)}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(f"Error details: {str(e)}")
            raise


class CustomSparkKafkaProcessor(SparkKafkaProcessor):
    def _ingest_stream_data(self):
        stream_df = (
            self.spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            )
            .option(
                "subscribe", os.getenv("KAFKA_FEATURES_TOPIC", "model.features.ready")
            )
            .option("startingOffsets", "latest")
            .load()
        )

        from pyspark.sql.functions import col, from_json

        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select("data.*")

        return parsed_df
