from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector
import logging

# Load environment variables from the mounted .env file
load_dotenv('/opt/airflow/.env')


def fetch_and_load_healthcare_data():
    # URL of the open source API (CSV format)
    url = "https://www.healthit.gov/data/open-api?source=hospital-mu-public-health-measures.csv"

    # Read the CSV data directly from the URL
    df = pd.read_csv(url)

    table_name = 'health_care'

    # Connect to Snowflake using credentials from environment variables
    ctx = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )
    cs = ctx.cursor()

    # Create the table dynamically based on CSV columns (convert spaces in column names)
    col_defs = [f"{col.upper().replace(' ', '_')} VARCHAR" for col in df.columns]
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name.upper()} ({', '.join(col_defs)});"
    cs.execute(create_table_sql)

    # Insert data row-by-row
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

with DAG('transfer_healthcare_data',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         is_paused_upon_creation=False) as dag:
    healthcare_task = PythonOperator(
        task_id='fetch_and_load_healthcare_data',
        python_callable=fetch_and_load_healthcare_data
    )

    healthcare_task
