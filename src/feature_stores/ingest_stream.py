import os
import sys
import threading
import time
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
import pyspark.sql.functions as F
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import (
    SparkKafkaProcessor,
    SparkProcessorConfig,
)
from loguru import logger
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Disable specific warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# Set up logging
logger.remove()
logger.add(sys.stdout, level="DEBUG")
logger.add("logs/ingest_stream.log", level="DEBUG")

# Kafka environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "tracking.user_behavior.validated")

# Schema for the nested "payload"
PAYLOAD_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True),
    ]
)

# Top-level event schema
EVENT_SCHEMA = StructType(
    [
        StructField(
            "schema",
            StructType(
                [
                    StructField("type", StringType(), True),
                    StructField(
                        "fields",
                        ArrayType(
                            StructType(
                                [
                                    StructField("name", StringType(), True),
                                    StructField("type", StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("payload", PAYLOAD_SCHEMA, True),
        StructField(
            "metadata",
            StructType(
                [
                    StructField("processed_at", StringType(), True),
                ]
            ),
            True,
        ),
        StructField("valid", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("error_type", StringType(), True),
    ]
)

# Set Spark packages using the config instead of environment variables
SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.spark:spark-avro_2.12:3.5.0,"
    "org.apache.kafka:kafka-clients:3.4.0"
)

# Create SparkSession
spark = (
    SparkSession.builder.master("local[*]")
    .appName("feast-feature-ingestion")
    .config("spark.jars.packages", SPARK_PACKAGES)
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.streaming.kafka.maxRatePerPartition", 100)
    .config("spark.streaming.backpressure.enabled", True)
    .config("spark.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .config("spark.kafka.subscribe", KAFKA_INPUT_TOPIC)
    .config("spark.kafka.startingOffsets", "latest")
    .config("spark.kafka.failOnDataLoss", "false")
    .getOrCreate()
)

# Initialize the FeatureStore
store = FeatureStore(repo_path=".")

# Session data tracking (optional usage)
session_activity_counts = defaultdict(int)
session_last_seen = defaultdict(datetime.now)
SESSION_TIMEOUT = timedelta(hours=24)


def preprocess_fn(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform validated events from the raw Kafka payload
    into a DataFrame of features required by 'streaming_features'.
    """
    try:
        if df.empty:
            logger.info("Received empty batch, returning empty DataFrame with schema.")
            return pd.DataFrame(
                columns=[
                    "event_timestamp",
                    "user_id",
                    "product_id",
                    "user_session",
                    "price",
                    "brand",
                    "category_code_level1",
                    "category_code_level2",
                    "event_weekday",
                    "activity_count",
                    "is_purchased",
                ]
            )

        # Only keep valid events but don't include valid column in output
        df = df[df["valid"] == "VALID"].copy()
        if df.empty:
            logger.info("No valid records in this batch.")
            return pd.DataFrame(columns=df.columns)

        # If needed, parse nested 'payload' columns
        if "payload" in df.columns:
            for field in [
                "event_time",
                "event_type",
                "product_id",
                "category_id",
                "category_code",
                "brand",
                "price",
                "user_id",
                "user_session",
            ]:
                df[field] = df["payload"].apply(lambda x: x.get(field) if x else None)

        # Ensure we have a proper event_timestamp column
        if "event_time" in df.columns:
            df.rename(columns={"event_time": "event_timestamp"}, inplace=True)
        if "event_timestamp" not in df.columns:
            logger.error("Missing column: event_timestamp")
            return pd.DataFrame(columns=df.columns)

        # Convert event_timestamp to datetime
        df["event_timestamp"] = pd.to_datetime(
            df["event_timestamp"],
            format="%Y-%m-%d %H:%M:%S UTC",  # Adjust if needed
            errors="coerce",
        )
        df["event_timestamp"] = df["event_timestamp"].dt.tz_localize(None)

        # Convert to Spark DataFrame for window-based activity_count
        spark_df = spark.createDataFrame(df)

        # Window: activity_count by user_session
        window_spec = Window.partitionBy("user_session")
        spark_df = spark_df.withColumn("activity_count", F.count("*").over(window_spec))

        # Convert Spark DF back to Pandas
        df = spark_df.toPandas()

        # Fill missing fields with defaults
        df["price"] = df["price"].fillna(0.0)
        df["brand"] = df["brand"].fillna("")
        df["category_code"] = df["category_code"].fillna("")
        df["event_type"] = df["event_type"].fillna("")

        # Split category_code into two levels
        cat_split = df["category_code"].str.split(".", n=1, expand=True).fillna("")
        df["category_code_level1"] = cat_split[0]
        df["category_code_level2"] = cat_split[1]

        # Calculate weekday
        df["event_weekday"] = df["event_timestamp"].dt.weekday

        # Derived feature: is_purchased
        df["is_purchased"] = (df["event_type"].str.lower() == "purchase").astype(int)

        # Final columns for pushing
        feature_cols = [
            "event_timestamp",
            "user_id",
            "product_id",
            "user_session",
            "price",
            "brand",
            "category_code_level1",
            "category_code_level2",
            "event_weekday",
            "activity_count",
            "is_purchased",
        ]
        df = df[feature_cols]

        # Cast to correct dtypes
        type_map = {
            "user_id": "int64",
            "product_id": "int64",
            "price": "float64",
            "event_weekday": "int64",
            "activity_count": "int64",
            "is_purchased": "int64",
        }
        for col_name, dtype in type_map.items():
            if col_name in df.columns:
                df[col_name] = df[col_name].astype(dtype)

        logger.info(f"Preprocessing produced {len(df)} rows")
        return df

    except Exception as e:
        logger.exception(f"Error in preprocess_fn: {str(e)}")
        return pd.DataFrame(columns=df.columns)


class ValidatedEventsProcessor(SparkKafkaProcessor):
    """
    Custom Kafka processor that reads from validated events,
    applies 'preprocess_fn', and pushes data to the Feast stream feature view.
    """

    def _ingest_stream_data(self):
        from pyspark.sql.functions import col, from_json

        logger.debug("Starting stream ingestion from validated events topic...")
        stream_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_INPUT_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Parse JSON
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select(
            F.col("data.payload.event_time").alias("event_timestamp"),
            F.col("data.payload.event_type"),
            F.col("data.payload.product_id"),
            F.col("data.payload.category_id"),
            F.col("data.payload.category_code"),
            F.col("data.payload.brand"),
            F.col("data.payload.price"),
            F.col("data.payload.user_id"),
            F.col("data.payload.user_session"),
            F.col("data.valid"),
            F.col("data.metadata.processed_at"),
        )

        # Change to ONLINE only mode since OFFLINE writes are not supported
        self.push_mode = PushMode.ONLINE

        return parsed_df


# SparkProcessorConfig
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="4 seconds",
    query_timeout=30,
)


def run_materialization():
    """
    Runs Feast materialize_incremental every hour, to sync
    data from offline to online store for historical completeness.
    """
    while True:
        try:
            logger.info("Starting materialization job...")
            store.materialize_incremental(end_date=datetime.utcnow())
            logger.info("Materialization completed successfully.")
        except Exception as e:
            logger.error(f"Materialization failed: {e}")
        # Sleep 1 hour
        time.sleep(3600)


if __name__ == "__main__":
    try:
        # Retrieve your streaming Feature View definition from the repo
        sfv = store.get_stream_feature_view("streaming_features")

        # Start a background thread to materialize every hour
        materialization_thread = threading.Thread(
            target=run_materialization, daemon=True
        )
        materialization_thread.start()
        logger.info("Background materialization thread started")

        # Initialize the processor
        processor = ValidatedEventsProcessor(
            config=ingestion_config,
            fs=store,
            sfv=sfv,  # Stream Feature View
            preprocess_fn=preprocess_fn,
        )

        # Start ingestion in ONLINE push mode
        logger.info(f"Starting feature ingestion from topic: {KAFKA_INPUT_TOPIC}")
        query = processor.ingest_stream_feature_view(PushMode.ONLINE)
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Failed to start feature ingestion: {str(e)}")
        raise
