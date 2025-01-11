from typing import Any, Dict

import pandas as pd
from include.common.scripts.monitoring import PipelineMonitoring
from loguru import logger

from airflow.decorators import task

logger = logger.bind(name=__name__)


def categorize_price(price: float) -> str:
    """Categorize price into tiers"""
    logger.debug(f"Categorizing price: {price}")
    if price < 50:
        return "low"
    elif price < 250:
        return "medium"
    elif price < 1000:
        return "high"
    return "luxury"


def split_category_code(category_code: str) -> tuple:
    """Split category code into L1, L2, L3 categories"""
    logger.debug(f"Splitting category code: {category_code}")
    if pd.isna(category_code):
        return None, None, None

    parts = category_code.split(".")
    l1 = parts[0] if len(parts) > 0 else None
    l2 = parts[1] if len(parts) > 1 else None
    l3 = parts[2] if len(parts) > 2 else None

    return l1, l2, l3


def transform_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Convert event_time to datetime and add derived time columns"""
    logger.debug("Transforming timestamps")
    # Debug log the columns we have
    logger.debug(f"Available columns: {df.columns.tolist()}")

    # Check if event_time exists, if not try alternative column names
    timestamp_column = None
    possible_columns = ["event_time", "timestamp", "event_timestamp"]

    for col in possible_columns:
        if col in df.columns:
            timestamp_column = col
            break

    if timestamp_column is None:
        raise ValueError(
            f"No timestamp column found. Available columns: {df.columns.tolist()}"
        )

    # Convert timestamp
    df["event_timestamp"] = pd.to_datetime(df[timestamp_column])
    df["event_date"] = df["event_timestamp"].dt.date
    df["event_hour"] = df["event_timestamp"].dt.hour
    df["day_of_week"] = df["event_timestamp"].dt.day_name()

    logger.info(f"Transformed timestamps: {len(df)} records processed.")
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns based on existing data"""
    logger.debug("Adding derived columns")
    # Add price tiers
    df["price_tier"] = df["price"].apply(categorize_price)

    # Split category codes
    category_splits = df["category_code"].apply(split_category_code)
    df["category_l1"] = category_splits.apply(lambda x: x[0])
    df["category_l2"] = category_splits.apply(lambda x: x[1])
    df["category_l3"] = category_splits.apply(lambda x: x[2])

    logger.info(f"Derived columns added: {len(df)} records processed.")
    return df


def calculate_session_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate metrics per user session"""
    logger.debug("Calculating session metrics")
    session_metrics = (
        df.groupby("user_session")
        .agg(
            {
                "event_timestamp": ["count", "min", "max"],
                "price": ["mean", "min", "max"],
            }
        )
        .reset_index()
    )

    session_metrics.columns = [
        "user_session",
        "events_in_session",
        "session_start",
        "session_end",
        "avg_price",
        "min_price",
        "max_price",
    ]

    logger.info(
        f"Session metrics calculated: {len(session_metrics)} sessions processed."
    )
    return df.merge(session_metrics, on="user_session", how="left")


def check_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate records based on record_hash"""
    initial_count = len(df)
    df = df.drop_duplicates(subset=["record_hash"], keep="first")
    duplicate_count = initial_count - len(df)
    logger.info(f"Removed {duplicate_count} duplicate records")
    return df


def prepare_for_serialization(df: pd.DataFrame) -> Dict[str, Any]:
    """Convert DataFrame to serializable format"""
    logger.debug("Preparing DataFrame for serialization")
    # Convert timestamps to ISO format strings
    df["event_timestamp"] = df["event_timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["session_start"] = df["session_start"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["session_end"] = df["session_end"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["event_date"] = df["event_date"].astype(str)

    logger.info(f"DataFrame prepared for serialization: {len(df)} records.")
    return df.to_dict(orient="records")


@task()
def transform_data(validated_data: Dict[str, Any]) -> Dict[str, Any]:
    """Transform the validated data"""
    logger.info("Starting data transformation")
    try:
        # Convert to DataFrame and log initial state
        df = pd.DataFrame(validated_data["data"])

        if len(df) == 0:
            logger.warning("No data to transform")
            return {
                "data": [],
                "success": False,
                "message": "No data to transform",
                "skipp_downstream": True,
            }

        logger.debug(f"Initial DataFrame columns: {df.columns.tolist()}")
        logger.debug(f"DataFrame shape: {df.shape}")

        # Check and remove duplicates
        df = check_duplicates(df)

        # Apply transformations with better error handling
        try:
            df = transform_timestamps(df)
        except Exception as e:
            logger.error(f"Failed to transform timestamps: {str(e)}")
            raise

        df = add_derived_columns(df)
        df = calculate_session_metrics(df)

        # Calculate transformation metrics
        metrics = {
            "total_records": len(df),
            "unique_users": df["user_id"].nunique(),
            "unique_sessions": df["user_session"].nunique(),
            "avg_session_events": float(df["events_in_session"].mean()),
            "price_tier_distribution": df["price_tier"].value_counts().to_dict(),
        }

        logger.info(f"Transformation metrics calculated: {metrics}")

        # Log metrics
        PipelineMonitoring.log_metrics(metrics)

        # Convert to serializable format
        serializable_data = prepare_for_serialization(df)

        logger.info("Data transformation completed successfully")
        return {"data": serializable_data, "metrics": metrics}

    except Exception as e:
        logger.error(f"Failed to transform data: {str(e)}", exc_info=True)
        raise Exception(f"Failed to transform data: {str(e)}")
