import os

from dotenv import load_dotenv
from loguru import logger
from postgresql_client import PostgresSQLClient

load_dotenv()


def main():
    logger.info("Initializing PostgreSQL client")
    pc = PostgresSQLClient(
        port=os.getenv("POSTGRES_PORT"),
        database=os.getenv("POSTGRES_DB"),
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    # Drop events table if exists
    logger.info("Dropping existing events table if it exists")
    drop_table_query = "DROP TABLE IF EXISTS events;"
    pc.execute_query(drop_table_query)

    # Create events table with timestamp format that matches your requirement
    logger.info("Creating events table")
    create_table_query = """
        CREATE TABLE IF NOT EXISTS events (
            event_time TIMESTAMP WITH TIME ZONE DEFAULT timezone('UTC', now()),
            event_type VARCHAR(50),
            product_id BIGINT,
            category_id BIGINT,
            category_code VARCHAR(255),
            brand VARCHAR(255),
            price DOUBLE PRECISION,
            user_id BIGINT,
            user_session VARCHAR(255)
        );
    """
    try:
        pc.execute_query(create_table_query)
        logger.success("Successfully created events table")
    except Exception as e:
        logger.error(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
