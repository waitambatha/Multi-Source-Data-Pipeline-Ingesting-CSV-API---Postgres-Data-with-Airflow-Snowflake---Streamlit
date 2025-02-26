from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
import logging

# Load environment variables from the mounted .env file
load_dotenv('/opt/airflow/.env')


def check_postgres_connection():
    """
    Check the connection to the Postgres database.
    """
    try:
        conn = psycopg2.connect(
            host="host.docker.internal",
            database="rdt_data",
            user="postgres",
            password="masterclass"
        )
        logging.info("Successfully connected to Postgres!")
        conn.close()
    except Exception as e:
        logging.error("Error connecting to Postgres: %s", e)
        raise


def transfer_consumption_data():
    """
    Extract data from Postgres table 'consumption_data' and load it into Snowflake.
    """
    try:
        pg_conn = psycopg2.connect(
            host="host.docker.internal",
            database="rdt_data",
            user="postgres",
            password="masterclass"
        )
        df = pd.read_sql("SELECT * FROM consumption_data;", pg_conn)
        pg_conn.close()
    except Exception as e:
        logging.error("Error reading from Postgres: %s", e)
        raise

    table_name = 'consumption_data'

    try:
        ctx = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
        )
        cs = ctx.cursor()
    except Exception as e:
        logging.error("Error connecting to Snowflake: %s", e)
        raise

    # Create the table in Snowflake based on DataFrame columns (using VARCHAR for simplicity)
    col_defs = [f"{col.upper()} VARCHAR" for col in df.columns]
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name.upper()} ({', '.join(col_defs)});"
    try:
        cs.execute(create_table_sql)
    except Exception as e:
        logging.error("Error creating table in Snowflake: %s", e)
        raise

    # Insert each row from the DataFrame into Snowflake
    for index, row in df.iterrows():
        values = [f"'{str(val)}'" for val in row]
        insert_sql = f"INSERT INTO {table_name.upper()} VALUES ({', '.join(values)});"
        try:
            cs.execute(insert_sql)
        except Exception as e:
            logging.error("Error inserting row %s: %s", index, e)

    cs.close()
    ctx.close()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('transfer_consumption_data',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         is_paused_upon_creation=False) as dag:
    check_connection = PythonOperator(
        task_id='check_postgres_connection',
        python_callable=check_postgres_connection
    )

    transfer_task = PythonOperator(
        task_id='transfer_consumption_data',
        python_callable=transfer_consumption_data
    )

    check_connection >> transfer_task
