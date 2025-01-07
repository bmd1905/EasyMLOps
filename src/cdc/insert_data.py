import os
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from postgresql_client import PostgresSQLClient

load_dotenv()

SAMPLE_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "data", "sample.parquet.gzip"
)
TABLE_NAME = "events"


def format_record(row):
    # Convert microseconds timestamp to datetime string in UTC
    if isinstance(row["event_time"], (int, float)):
        # Handle microseconds timestamp
        timestamp = pd.to_datetime(row["event_time"], unit="us")
    else:
        # Handle string or datetime timestamp
        timestamp = pd.to_datetime(row["event_time"])

    record = {
        "event_time": timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "event_type": str(row["event_type"]),
        "product_id": int(row["product_id"]),
        "category_id": int(row["category_id"]),
        "category_code": str(row["category_code"])
        if pd.notnull(row["category_code"])
        else None,
        "brand": str(row["brand"]) if pd.notnull(row["brand"]) else None,
        "price": max(float(row["price"]), 0),
        "user_id": int(row["user_id"]),
        "user_session": str(row["user_session"]),
    }
    return record


def load_sample_data():
    """Load and prepare sample data from parquet file"""
    try:
        logger.info(f"Loading sample data from {SAMPLE_DATA_PATH}")
        df = pd.read_parquet(SAMPLE_DATA_PATH, engine="fastparquet")
        records = []
        for idx, row in df.iterrows():
            record = format_record(row)
            records.append(record)
        logger.success(
            f"Successfully loaded {len(records)} records from {SAMPLE_DATA_PATH}"
        )
        return records
    except Exception as e:
        logger.error(f"Error loading sample data: {str(e)}")
        raise


def main():
    logger.info("Starting data insertion process")
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    logger.info("Successfully connected to PostgreSQL database")

    columns = [
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
    ]

    # Load and process records
    records = load_sample_data()
    valid_records = 0
    invalid_records = 0

    logger.info("Starting record insertion")
    for record in records:
        # Extract values in the correct order
        values = [record.get(col) for col in columns]

        # Insert record
        placeholders = ",".join(["%s"] * len(columns))
        query = f"""
            INSERT INTO {TABLE_NAME} ({",".join(columns)})
            VALUES ({placeholders})
        """
        try:
            pc.execute_query(query, values)
            valid_records += 1
            if valid_records % 1000 == 0:
                logger.info(f"Processed {valid_records} valid records")
        except Exception as e:
            logger.error(f"Failed to insert record: {str(e)}")
            invalid_records += 1

        sleep(0.5)

    logger.info("\nFinal Summary:")
    logger.info(f"Total records processed: {len(records)}")
    logger.success(f"Valid records inserted: {valid_records}")
    logger.warning(f"Invalid records skipped: {invalid_records}")


if __name__ == "__main__":
    main()
