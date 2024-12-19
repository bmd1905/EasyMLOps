import logging
from typing import Dict, List

from airflow.decorators import task

import jinja2
import pandas as pd

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.common.scripts.sql_utils import load_sql_template
from include.config.tune_config import (
    CATEGORICAL_COLUMNS,
    FEATURE_COLUMNS,
)

logger = logging.getLogger(__name__)


@task()
def load_training_data() -> Dict[str, List[Dict]]:
    """
    Load training data from DWH

    :param row_limit: Number of rows to load
    :return: Dictionary with training data
    """

    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        # Load and render SQL template
        template = jinja2.Template(load_sql_template("queries/load_training_data.sql"))
        query = template.render(feature_columns=FEATURE_COLUMNS)

        df = postgres_hook.get_pandas_df(query)[:20]

        logger.info(f"Loaded {len(df)} rows with columns: {df.columns.tolist()}")

        # Data preprocessing
        df["price"] = df["price"].astype(float)
        for col in CATEGORICAL_COLUMNS:
            df[col] = pd.Categorical(df[col]).codes

        return {"data": df.to_dict(orient="records")}
    except Exception as e:
        raise AirflowException(f"Failed to load training data: {e}")
