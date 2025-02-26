from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os
from dotenv import load_dotenv
import snowflake.connector

# Load environment variables from the mounted .env file
load_dotenv('/opt/airflow/.env')

def upload_csv_to_snowflake():
    # Read CSV file from the mounted data directory
    df = pd.read_csv('/opt/airflow/data/schools.csv')
    table_name = 'school_data'  # Table name derived from the data source

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

    # Create a table based on CSV columns (using VARCHAR for simplicity)
    col_defs = [f"{col.upper()} VARCHAR" for col in df.columns]
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name.upper()} ({', '.join(col_defs)});"
    cs.execute(create_table_sql)

    # Insert data row-by-row
    for _, row in df.iterrows():
        values = [f"'{str(val)}'" for val in row]
        insert_sql = f"INSERT INTO {table_name.upper()} VALUES ({', '.join(values)});"
        cs.execute(insert_sql)

    cs.close()
    ctx.close()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('upload_schools_csv',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         is_paused_upon_creation=False) as dag:

    upload_task = PythonOperator(
        task_id='upload_csv_to_snowflake',
        python_callable=upload_csv_to_snowflake
    )

    upload_task
