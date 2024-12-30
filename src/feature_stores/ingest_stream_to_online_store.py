import os
import warnings

import pandas as pd
from feast import FeatureStore
from feast.data_source import PushMode
from feast.infra.contrib.spark_kafka_processor import (
    SparkKafkaProcessor,
    SparkProcessorConfig,
)
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Disable specific warnings
warnings.filterwarnings(
    "ignore", category=RuntimeWarning, module="feast.stream_feature_view"
)
warnings.filterwarnings(
    "ignore", category=DeprecationWarning, module="pyarrow.pandas_compat"
)
warnings.filterwarnings("ignore", category=UserWarning, module="feast.utils")


# Configure environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_FEATURE_TOPIC = os.getenv("KAFKA_FEATURES_TOPIC", "feature-events-topic")

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

# Configure the SparkSession with Kafka packages
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,"
    "org.apache.spark:spark-avro_2.12:3.0.0 pyspark-shell"
)

# Initialize Spark with proper configuration
spark = (
    SparkSession.builder.master("local[*]")
    .appName("feast-feature-ingestion")
    .config("spark.sql.shuffle.partitions", 8)
    .config("spark.streaming.kafka.maxRatePerPartition", 100)
    .config("spark.streaming.backpressure.enabled", True)
    # Add Kafka configurations
    .config("spark.kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .config("spark.kafka.subscribe", KAFKA_FEATURE_TOPIC)
    .config("spark.kafka.startingOffsets", "latest")
    .config("spark.kafka.failOnDataLoss", "false")
    .getOrCreate()
)

# Initialize the feature store
store = FeatureStore(repo_path=".")


def preprocess_fn(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the incoming feature data.

    Args:
        df: Input DataFrame with raw feature data
    Returns:
        Processed DataFrame ready for feature store ingestion
    """
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

        # Calculate features with proper NaN handling
        df["activity_count"] = 1
        df["total_price"] = df["price"]
        df["avg_price"] = df.groupby("user_id")["price"].transform("mean")
        df["is_purchased"] = (
            (df["event_type"].str.lower() == "purchase").fillna(False).astype("int64")
        )

        # Ensure all required columns exist with proper types
        required_features = {
            "event_timestamp": "datetime64[ns]",
            "user_id": "int64",
            "activity_count": "int64",
            "total_price": "float64",
            "avg_price": "float64",
            "is_purchased": "int64",
            "event_weekday": "int64",
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

            # Skip timestamp as it's already handled
            if col != "event_timestamp":
                df[col] = df[col].astype(dtype)

        logger.info(f"Successfully processed {len(df)} records")
        return df

    except Exception as e:
        logger.error(f"Error preprocessing data: {str(e)}")
        logger.error(
            f"Sample timestamp value: {df['event_timestamp'].iloc[0] if len(df) > 0 else 'No data'}"
        )
        raise


class CustomSparkKafkaProcessor(SparkKafkaProcessor):
    def _ingest_stream_data(self):
        stream_df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_FEATURE_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        from pyspark.sql.functions import col, from_json

        parsed_df = stream_df.select(
            from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
        ).select("data.*")

        return parsed_df


# Define ingestion config
ingestion_config = SparkProcessorConfig(
    mode="spark",
    source="kafka",
    spark_session=spark,
    processing_time="2 seconds",
    query_timeout=30,
)

try:
    # Initialize the stream feature view
    sfv = store.get_stream_feature_view("streaming_features")

    # Initialize the processor with custom class
    processor = CustomSparkKafkaProcessor(
        config=ingestion_config,
        fs=store,
        sfv=sfv,
        preprocess_fn=preprocess_fn,
    )

    # Start ingestion to online store
    logger.info(f"Starting feature ingestion from topic {KAFKA_FEATURE_TOPIC}")
    logger.debug("Using custom Spark Kafka processor")
    logger.debug(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.debug(f"Topic: {KAFKA_FEATURE_TOPIC}")

    query = processor.ingest_stream_feature_view(PushMode.ONLINE)
    query.awaitTermination()

except Exception as e:
    logger.error(f"Failed to start feature ingestion: {str(e)}")
    logger.error(f"Error type: {type(e).__name__}")
    logger.error(f"Error details: {str(e)}")
    raise
