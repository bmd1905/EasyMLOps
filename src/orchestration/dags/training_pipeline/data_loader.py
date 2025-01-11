from typing import Dict, List

import jinja2
import numpy as np
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.common.scripts.sql_utils import load_sql_template
from include.config.tune_config import (
    CATEGORICAL_COLUMNS,
    FEATURE_COLUMNS,
)
from loguru import logger

logger = logger.bind(name=__name__)


@task()
def load_training_data() -> Dict[str, List[Dict]]:
    """
    Load training data from DWH

    :param row_limit: Number of rows to load
    :return: Dictionary with training data
    """

    try:
        logger.info("Starting data loading process")
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        # Load and render SQL template
        logger.debug(f"Rendering SQL template with feature columns: {FEATURE_COLUMNS}")
        template = jinja2.Template(load_sql_template("queries/load_training_data.sql"))
        query = template.render(feature_columns=FEATURE_COLUMNS)
        logger.debug(f"Generated SQL query: {query}")

        df = postgres_hook.get_pandas_df(query)
        logger.info(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")

        # Data preprocessing
        logger.info("Starting data preprocessing")
        df["price"] = df["price"].astype(float)
        logger.debug(
            f"Converted price column to float. Price range: {df['price'].min()} - {df['price'].max()}"
        )

        # Create and save category mappings during training
        category_mappings = {}
        for col in CATEGORICAL_COLUMNS:
            logger.debug(f"Encoding categorical column: {col}")
            # Create mapping dictionary for each category
            unique_values = df[col].dropna().unique().tolist()  # Convert to Python list
            # Convert both keys and values to native Python types
            category_mapping = {
                (
                    int(val) if isinstance(val, (np.integer, np.floating)) else str(val)
                ): int(idx)
                for idx, val in enumerate(sorted(unique_values))
            }
            category_mappings[col] = category_mapping

            # Apply mapping (with a default for unseen categories)
            df[col] = df[col].map(category_mapping).fillna(-1).astype(int)

        logger.info("Data preprocessing completed successfully")

        # Convert DataFrame to native Python types
        records = df.to_dict(orient="records")
        records = [
            {
                k: int(v) if isinstance(v, (np.integer, np.floating)) else v
                for k, v in record.items()
            }
            for record in records
        ]

        return {
            "data": records,
            "category_mappings": category_mappings,
        }
    except Exception as e:
        logger.error(f"Error in data loading process: {e}", exc_info=True)
        raise AirflowException(f"Failed to load training data: {e}")
