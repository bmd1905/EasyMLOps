import os
from pathlib import Path

import psycopg2
from loguru import logger


def init_database():
    """Initialize the database with required schema and views."""
    try:
        # Get database connection details from environment variables
        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = os.getenv("POSTGRES_PORT", "5433")
        db_name = os.getenv("POSTGRES_DB", "dwh")
        db_user = os.getenv("POSTGRES_USER", "dwh")
        db_password = os.getenv("POSTGRES_PASSWORD", "dwh")

        logger.info(f"Connecting to PostgreSQL at {db_host}:{db_port}")

        # Connect to database
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Read and execute SQL initialization script
        sql_path = Path(__file__).parent / "sql" / "init_db.sql"
        with open(sql_path, "r") as f:
            init_sql = f.read()

            cursor.execute(init_sql)

        logger.info("Successfully initialized database schema and views")
        return True

    except psycopg2.Error as e:
        logger.error(f"Database initialization failed: {str(e)}")
        return False

    finally:
        if "conn" in locals():
            conn.close()
