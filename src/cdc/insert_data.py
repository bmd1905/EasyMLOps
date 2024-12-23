import os
from time import sleep

import pandas as pd
from dotenv import load_dotenv

from .postgresql_client import PostgresSQLClient

load_dotenv()

SAMPLE_DATA_PATH = os.path.join(os.path.dirname(__file__), "data", "sample.parquet")
TABLE_NAME = "events"


def load_sample_data():
    """Load and prepare sample data from parquet file"""
    try:
        df = pd.read_parquet(SAMPLE_DATA_PATH)
        records = df.to_dict("records")
        print(f"Loaded {len(records)} records from {SAMPLE_DATA_PATH}")
        return records
    except Exception as e:
        print(f"Error loading sample data: {str(e)}")
        raise


def validate_and_transform_record(record: dict, columns: list) -> list:
    """Validate and transform record values"""
    try:
        values = []
        for col in columns:
            val = record.get(col)

            # Handle NULL values
            if pd.isna(val):
                values.append(None)
                continue

            # Type conversions and validations
            if col in ["product_id", "category_id", "user_id"]:
                values.append(
                    str(int(val))
                )  # Convert to string to handle large integers
            elif col == "price":
                values.append(float(val))
            elif col == "event_time":
                if isinstance(val, str):
                    values.append(pd.to_datetime(val))
                else:
                    values.append(val)
            else:
                values.append(str(val))

        return values
    except Exception as e:
        print(f"Error processing record: {record}")
        print(f"Error details: {str(e)}")
        return None


def main():
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

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

    for record in records:
        values = validate_and_transform_record(record, columns)

        if values is None:
            invalid_records += 1
            continue

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
                print(f"Processed {valid_records} valid records")
        except Exception as e:
            print(f"Failed to insert record: {str(e)}")
            invalid_records += 1

        sleep(1)

    print("\nFinal Summary:")
    print(f"Total records processed: {len(records)}")
    print(f"Valid records inserted: {valid_records}")
    print(f"Invalid records skipped: {invalid_records}")


if __name__ == "__main__":
    main()
