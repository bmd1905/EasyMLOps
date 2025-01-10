import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import (
    SparkKafkaProcessor,
    SparkProcessorConfig,
)
from kafka.admin import KafkaAdminClient
from kafka.errors import KafkaError
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Set logging level
logger.remove()
logger.add(
    sys.stdout,
    level="INFO",
)
logger.add(
    "logs/ingest_stream.log",
    level="DEBUG",
)

# Configure environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "tracking.user_behavior.validated")

# Define the schema for validated events
PAYLOAD_SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField(
            "category_id", StringType(), True
        ),  # Changed to StringType as it's a large number
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_session", StringType(), True),
    ]
)

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
                            ),
                            True,
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

# Configure the SparkSession with Kafka packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.spark:spark-avro_2.12:3.5.0,"
    "org.apache.kafka:kafka-clients:3.4.0 "
    "pyspark-shell"
)

# Initialize Spark with proper configuration
spark = (
    SparkSession.builder.master("local[*]")
    .appName("feast-feature-ingestion")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "org.apache.spark:spark-avro_2.12:3.5.0,"
        "org.apache.kafka:kafka-clients:3.4.0",
    )
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.streaming.kafka.maxRatePerPartition", 100)
    .config("spark.streaming.backpressure.enabled", True)
    .config("spark.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .config("spark.kafka.subscribe", KAFKA_INPUT_TOPIC)
    .config("spark.kafka.startingOffsets", "latest")
    .config("spark.kafka.failOnDataLoss", "false")
    .getOrCreate()
)

# Initialize the feature store
store = FeatureStore(repo_path=".")

# Global session activity counter
session_activity_counts = defaultdict(int)
session_last_seen = defaultdict(datetime.now)
SESSION_TIMEOUT = timedelta(hours=24)


def cleanup_old_sessions():
    """Remove sessions that haven't been seen for a while"""
    current_time = datetime.now()
    expired_sessions = [
        session_id
        for session_id, last_seen in session_last_seen.items()
        if current_time - last_seen > SESSION_TIMEOUT
    ]

    for session_id in expired_sessions:
        del session_activity_counts[session_id]
        del session_last_seen[session_id]

    if expired_sessions:
        logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")


def preprocess_fn(df: pd.DataFrame) -> pd.DataFrame:
    """Transform validated events into feature data"""
    try:
        logger.info(f"Processing batch of size {len(df)}")
        logger.debug(f"Input columns: {df.columns.tolist()}")

        # Basic data cleaning
        df = df[df["valid"] == "VALID"].copy()  # Only process valid events

        # Rename event_time to event_timestamp
        df = df.rename(columns={"event_time": "event_timestamp"})
        logger.debug(f"After rename: {df.columns.tolist()}")

        # Handle missing values
        df["price"] = df["price"].fillna(0.0)
        df["event_type"] = df["event_type"].fillna("")
        df["brand"] = df["brand"].fillna("")
        df["category_code"] = df["category_code"].fillna("")

        # Extract category levels
        df[["category_code_level1", "category_code_level2"]] = (
            df["category_code"].str.split(".", n=1, expand=True).fillna("")
        )

        # Convert event_timestamp to datetime
        if df["event_timestamp"].dtype != "datetime64[ns]":
            df["event_timestamp"] = pd.to_datetime(
                df["event_timestamp"],
                format="%Y-%m-%d %H:%M:%S UTC",
                utc=True,
                errors="coerce",
            )
            df["event_timestamp"] = df["event_timestamp"].dt.tz_localize(None)

        # Calculate temporal features
        df["event_weekday"] = df["event_timestamp"].dt.weekday

        # Update session activity counts
        current_time = datetime.now()
        for session_id in df["user_session"].unique():
            session_activity_counts[session_id] += len(
                df[df["user_session"] == session_id]
            )
            session_last_seen[session_id] = current_time

        cleanup_old_sessions()

        # Add derived features
        df["activity_count"] = df["user_session"].map(session_activity_counts)
        df["is_purchased"] = (df["event_type"].str.lower() == "purchase").astype(int)

        # Select and rename columns for feature store
        feature_df = df[
            [
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
        ].copy()

        # Ensure proper types
        type_mapping = {
            "user_id": "int64",
            "product_id": "int64",
            "price": "float64",
            "event_weekday": "int64",
            "activity_count": "int64",
            "is_purchased": "int64",
            "brand": "string",
            "category_code_level1": "string",
            "category_code_level2": "string",
        }

        for col, dtype in type_mapping.items():
            feature_df[col] = feature_df[col].astype(dtype)

        # Add debug logging for column verification
        logger.debug(f"Final columns: {feature_df.columns.tolist()}")
        logger.debug(f"Final dtypes: {feature_df.dtypes}")

        # Verify event_timestamp exists and is datetime
        if "event_timestamp" not in feature_df.columns:
            raise ValueError("event_timestamp column is missing from final DataFrame")
        if not pd.api.types.is_datetime64_any_dtype(feature_df["event_timestamp"]):
            raise ValueError("event_timestamp is not datetime type")

        logger.info(f"Successfully processed {len(feature_df)} records")
        return feature_df  # Return feature_df instead of df

    except Exception as e:
        logger.error(f"Error preprocessing data: {str(e)}")
        logger.error(f"Column dtypes: {df.dtypes}")
        logger.error(f"Available columns: {df.columns.tolist()}")
        raise


class ValidatedEventsProcessor(SparkKafkaProcessor):
    def _ingest_stream_data(self):
        from pyspark.sql.functions import col, from_json

        logger.debug("Starting stream ingestion from validated events topic...")

        stream_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option(
                "subscribe", KAFKA_INPUT_TOPIC
            )  # This is now tracking.user_behavior.validated
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Add debug output without starting a new stream
        def debug_batch(df, epoch_id):
            count = df.count()
            if count > 0:
                logger.info(f"Processing batch {epoch_id} with {count} records")
                sample = df.first()
                logger.debug(f"Sample record: {sample}")

        # Parse the nested JSON structure
        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select("data.payload.*", "data.valid")

        # Add the debug callback
        parsed_df.writeStream.foreachBatch(debug_batch).start()

        return parsed_df


# Define ingestion config
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="2 seconds",
    query_timeout=30,
)


def verify_kafka_topic():
    """Verify that the Kafka topic exists and has messages"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="feature-store-admin"
        )

        topics = admin_client.list_topics()
        logger.info(f"Available Kafka topics: {topics}")

        if KAFKA_INPUT_TOPIC not in topics:
            logger.error(f"Topic {KAFKA_INPUT_TOPIC} not found!")
            return False

        logger.info(f"Found topic {KAFKA_INPUT_TOPIC}")
        return True

    except KafkaError as e:
        logger.error(f"Error connecting to Kafka: {str(e)}")
        return False
    finally:
        admin_client.close()


try:
    # Verify Kafka topic first
    if not verify_kafka_topic():
        raise ValueError(f"Required Kafka topic {KAFKA_INPUT_TOPIC} not available")

    # Initialize the stream feature view
    sfv = store.get_stream_feature_view("streaming_features")

    # Initialize the processor
    processor = ValidatedEventsProcessor(
        config=ingestion_config,
        fs=store,
        sfv=sfv,
        preprocess_fn=preprocess_fn,
    )

    # Start ingestion
    logger.info(f"Starting feature ingestion from topic {KAFKA_INPUT_TOPIC}")
    logger.debug("Using validated events processor")
    logger.debug(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.debug(f"Topic: {KAFKA_INPUT_TOPIC}")

    query = processor.ingest_stream_feature_view(PushMode.ONLINE_AND_OFFLINE)
    query.awaitTermination()

except Exception as e:
    logger.error(f"Failed to start feature ingestion: {str(e)}")
    logger.error(f"Error type: {type(e).__name__}")
    logger.error(f"Error details: {str(e)}")
    raise
