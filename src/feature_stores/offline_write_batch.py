import os
from typing import List, Optional

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class PostgreSQLOfflineWriter:
    """Helper class to write batches of data to PostgreSQL offline store."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        database: str = None,
        db_schema: str = None,
        user: str = None,
        password: str = None,
        **kwargs,
    ):
        """Initialize PostgreSQL connection parameters."""
        # Get params from env vars if not provided
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "feast")
        self.db_schema = db_schema or os.getenv("POSTGRES_SCHEMA", "public")
        self.user = user or os.getenv("POSTGRES_USER", "feast")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "feast")

        # Create SQLAlchemy engine
        self.engine = self._create_engine()
        self.session = sessionmaker(bind=self.engine)

    def _create_engine(self):
        """Create SQLAlchemy engine with proper configuration."""
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(
            connection_string,
            connect_args={"options": f"-csearch_path={self.db_schema}"},
        )

    def write_batch(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = "append",
        index: bool = False,
        chunk_size: int = 10000,
    ) -> bool:
        """
        Write a batch of data to PostgreSQL table.
        """
        try:
            # Validate table name
            if not table_name or not isinstance(table_name, str):
                raise ValueError("Invalid table name")

            # Create schema if it doesn't exist
            with self.engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.db_schema}"))

                # Write DataFrame using pandas' built-in SQL writing within the same transaction
                df.to_sql(
                    name=table_name,
                    schema=self.db_schema,
                    con=conn,  # Use the connection object
                    if_exists=if_exists,
                    index=index,
                    chunksize=chunk_size,
                    method="multi",  # Use multi-row insert for better performance
                )

            logger.info(
                f"Successfully wrote {len(df)} rows to {self.db_schema}.{table_name}"
            )
            return True

        except Exception as e:
            logger.error(f"Error writing batch to PostgreSQL: {str(e)}")
            return False

    def create_table_from_features(
        self,
        table_name: str,
        feature_names: List[str],
        feature_types: List[str],
        entity_names: Optional[List[str]] = None,
        entity_types: Optional[List[str]] = None,
    ) -> bool:
        """
        Create a table with the proper schema for feature storage.
        """
        try:
            # Map Python/Feast types to PostgreSQL types
            type_mapping = {
                "int64": "BIGINT",
                "int32": "INTEGER",
                "float64": "DOUBLE PRECISION",
                "float32": "REAL",
                "string": "TEXT",
                "boolean": "BOOLEAN",
                "datetime64[ns]": "TIMESTAMP",
            }

            # Build CREATE TABLE statement
            columns = []

            # Add entity columns if provided
            if entity_names and entity_types:
                for name, type_ in zip(entity_names, entity_types):
                    pg_type = type_mapping.get(type_, "TEXT")
                    columns.append(f"{name} {pg_type}")

            # Add feature columns
            for name, type_ in zip(feature_names, feature_types):
                pg_type = type_mapping.get(type_, "TEXT")
                columns.append(f"{name} {pg_type}")

            # Add created_timestamp column
            columns.append("created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")

            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {self.db_schema}.{table_name} (
                {", ".join(columns)}
            )
            """

            # Execute CREATE TABLE using a transaction
            with self.engine.begin() as conn:
                # Create schema if it doesn't exist
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.db_schema}"))
                # Create table
                conn.execute(text(create_stmt))

            logger.info(f"Successfully created table {self.db_schema}.{table_name}")
            return True

        except Exception as e:
            logger.error(f"Error creating feature table: {str(e)}")
            return False
