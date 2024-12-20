import os

from dotenv import load_dotenv
from .postgresql_client import PostgresSQLClient

load_dotenv()


def main():
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Create devices table
    create_table_query = """
        CREATE TABLE IF NOT EXISTS devices (
            device_id INT,
            created VARCHAR(30),
            feature_0 FLOAT,
            feature_4 FLOAT,
            feature_8 FLOAT,
            feature_6 FLOAT,
            feature_2 FLOAT,
            feature_9 FLOAT,
            feature_3 FLOAT
        );
    """
    try:
        pc.execute_query(create_table_query)
    except Exception as e:
        print(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
