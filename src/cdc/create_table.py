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

    try:
        # Drop all tables
        logger.info("Dropping existing tables")
        pc.drop_tables()

        # Create tables
        logger.info("Creating tables")
        pc.create_tables()

        logger.success("Successfully created events table")
    except Exception as e:
        logger.error(f"Failed to create table with error: {e}")


if __name__ == "__main__":
    main()
