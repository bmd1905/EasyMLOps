import os
from time import sleep

import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from models import Event
from postgresql_client import PostgresSQLClient

load_dotenv()

SAMPLE_DATA_PATH = os.path.join(
    os.path.dirname(__file__), "data", "sample.parquet.gzip"
)


def format_record(row):
    # Convert microseconds timestamp to datetime object
    if isinstance(row["event_time"], (int, float)):
        timestamp = pd.to_datetime(row["event_time"], unit="us")
    else:
        timestamp = pd.to_datetime(row["event_time"])

    return Event(
        event_time=timestamp,
        event_type=str(row["event_type"]),
        product_id=int(row["product_id"]),
        category_id=int(row["category_id"]),
        category_code=str(row["category_code"])
        if pd.notnull(row["category_code"])
        else None,
        brand=str(row["brand"]) if pd.notnull(row["brand"]) else None,
        price=max(float(row["price"]), 0),
        user_id=int(row["user_id"]),
        user_session=str(row["user_session"]),
    )


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

    # Load and process records
    records = load_sample_data()
    valid_records = 0
    invalid_records = 0

    # Get session
    session = pc.get_session()

    logger.info("Starting record insertion")
    batch_size = 100
    current_batch = []

    try:
        for record in records:
            try:
                current_batch.append(record)
                if len(current_batch) >= batch_size:
                    session.bulk_save_objects(current_batch)
                    session.commit()
                    valid_records += len(current_batch)
                    current_batch = []
                    logger.info(f"Processed {valid_records} valid records")
                    sleep(0.5)
            except Exception as e:
                logger.error(f"Failed to insert record: {str(e)}")
                invalid_records += 1
                session.rollback()

        # Insert remaining records
        if current_batch:
            session.bulk_save_objects(current_batch)
            session.commit()
            valid_records += len(current_batch)

    except Exception as e:
        logger.error(f"Batch insertion error: {str(e)}")
        session.rollback()
    finally:
        session.close()

    logger.info("\nFinal Summary:")
    logger.info(f"Total records processed: {len(records)}")
    logger.success(f"Valid records inserted: {valid_records}")
    logger.warning(f"Invalid records skipped: {invalid_records}")


if __name__ == "__main__":
    main()
