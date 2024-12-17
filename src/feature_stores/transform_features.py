import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


def transform_and_load_features():
    """Transform raw data into features and load into DWH"""
    try:
        load_dotenv()

        # Read sample data
        df = pd.read_parquet("../data/sample.parquet")

        # Transform user features
        user_features = (
            df.groupby("user_id")
            .agg(
                {
                    "event_type": ("count", lambda x: len(x[x == "view"].unique())),
                }
            )
            .reset_index()
        )

        user_features.columns = ["user_id", "activity_count", "unique_products_viewed"]
        user_features["event_timestamp"] = pd.Timestamp.now()

        # Transform product features
        product_features = df[["product_id", "price", "category_id"]].drop_duplicates()
        product_features["category_code_level1"] = (
            product_features["category_id"].astype(str).str[:2].astype(int)
        )
        product_features["category_code_level2"] = (
            product_features["category_id"].astype(str).str[2:4].astype(int)
        )
        product_features["event_timestamp"] = pd.Timestamp.now()
        product_features = product_features.drop("category_id", axis=1)

        # Create database connection
        engine = create_engine(
            f"postgresql://{os.getenv('DWH_USER')}:{os.getenv('DWH_PASSWORD')}"
            f"@{os.getenv('DWH_HOST')}:{os.getenv('DWH_PORT')}/{os.getenv('DWH_DATABASE')}"
        )

        # Create schema if not exists
        with engine.connect() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS dwh;")

        # Load features to DWH
        user_features.to_sql(
            "vw_ml_purchase_prediction_user",
            engine,
            schema="dwh",
            if_exists="replace",
            index=False,
        )

        product_features.to_sql(
            "vw_ml_purchase_prediction_product",
            engine,
            schema="dwh",
            if_exists="replace",
            index=False,
        )

        logger.info("Features transformed and loaded successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to transform and load features: {e}")
        return False


if __name__ == "__main__":
    transform_and_load_features()
