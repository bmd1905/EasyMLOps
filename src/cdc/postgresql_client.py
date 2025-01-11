import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class PostgresSQLClient:
    def __init__(self, database, user, password, host="0.0.0.0", port="5434"):
        self.database = database
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.engine = None
        self.Session = None
        self._connect()

    def _connect(self):
        # Create SQLAlchemy engine
        connection_string = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def drop_tables(self):
        Base.metadata.drop_all(self.engine)

    def get_session(self):
        return self.Session()

    def execute_query(self, query, values=None):
        """Maintained for backwards compatibility"""
        with self.engine.connect() as connection:
            if values:
                connection.execute(query, values)
            else:
                connection.execute(query)

    def get_columns(self, table_name):
        df = pd.read_sql(f"select * from {table_name} LIMIT 0", self.engine)
        return df.columns
