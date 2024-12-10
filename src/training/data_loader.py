import pandas as pd
import psycopg2
import ray


def create_connection():
    """
    Create a connection to the database.
    """
    return psycopg2.connect(
        user="dwh", password="dwh", host="localhost", dbname="dwh", port=5433
    )


def load_data():
    """
    Load data from the database.
    """
    # Read data directly as pandas DataFrame to handle decimal types consistently
    with create_connection() as conn:
        # Select only the specified columns
        feature_columns = [
            "brand",
            "price",
            "event_weekday",
            "category_code_level1",
            "category_code_level2",
            "activity_count",
            "is_purchased",
        ]

        # Convert columns to appropriate types
        query = f"SELECT {', '.join(feature_columns)} FROM vw_ml_purchase_prediction"
        df = pd.read_sql(query, conn)

        # Convert price column to consistent decimal type
        df["price"] = df["price"].astype(float)

        # Encode categorical columns
        categorical_columns = [
            "brand",
            "event_weekday",
            "category_code_level1",
            "category_code_level2",
        ]
        for col in categorical_columns:
            df[col] = pd.Categorical(df[col]).codes

        # Convert DataFrame back to Ray Dataset
        return ray.data.from_pandas(df)


if __name__ == "__main__":
    dataset = load_data()
    print(dataset)
