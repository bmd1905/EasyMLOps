import pandas as pd
import psycopg2
from sqlalchemy import create_engine


class PostgresSQLClient:
    def __init__(self, database, user, password, host="0.0.0.0", port="5434"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def create_conn(self):
        # Establishing the connection
        conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
        )
        # Creating a cursor object using the cursor() method
        return conn

    def execute_query(self, query, values=None):
        conn = self.create_conn()
        cursor = conn.cursor()
        try:
            if values:
                cursor.execute(query, values)
            else:
                cursor.execute(query)
            conn.commit()
            print("Query executed successfully!")
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            raise
        finally:
            conn.close()

    def get_columns(self, table_name):
        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}"
        )
        conn = engine.connect()
        df = pd.read_sql(f"select * from {table_name}", conn)
        return df.columns
