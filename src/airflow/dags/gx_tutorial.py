from pendulum import datetime
from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

POSTGRES_CONN_ID = "postgres_dwh"
MY_POSTGRES_SCHEMA = "dwh"
MY_GX_DATA_CONTEXT = "include/gx"


@dag(
    start_date=datetime(2023, 7, 1),
    schedule=None,
    catchup=False,
)
def gx_tutorial():
    create_table_pg = PostgresOperator(
        task_id="create_table_pg",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            CREATE SCHEMA IF NOT EXISTS {MY_POSTGRES_SCHEMA};
            CREATE TABLE {MY_POSTGRES_SCHEMA}.strawberries (
                id VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100),
                amount INT
            );

            INSERT INTO {MY_POSTGRES_SCHEMA}.strawberries (id, name, amount)
            VALUES ('001', 'Strawberry Order 1', 10),
                ('002', 'Strawberry Order 2', 5),
                ('003', 'Strawberry Order 3', 8),
                ('004', 'Strawberry Order 4', 3),
                ('005', 'Strawberry Order 5', 12);
            """,
    )

    gx_validate_pg = GreatExpectationsOperator(
        task_id="gx_validate_pg",
        conn_id=POSTGRES_CONN_ID,
        data_context_root_dir=MY_GX_DATA_CONTEXT,
        schema=MY_POSTGRES_SCHEMA,
        data_asset_name="strawberries",
        expectation_suite_name="strawberry_suite",
        return_json_dict=True,
    )

    drop_table_pg_1 = PostgresOperator(
        task_id="drop_table_pg_1",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {MY_POSTGRES_SCHEMA}.strawberries;
            """,
    )

    drop_table_pg_2 = PostgresOperator(
        task_id="drop_table_pg_2",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            DROP TABLE IF EXISTS {MY_POSTGRES_SCHEMA}.strawberries;
            """,
    )

    drop_table_pg_1 >> create_table_pg >> gx_validate_pg >> drop_table_pg_2


gx_tutorial()
