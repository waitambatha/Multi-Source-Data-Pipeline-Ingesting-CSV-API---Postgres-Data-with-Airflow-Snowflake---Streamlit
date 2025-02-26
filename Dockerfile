# Dockerfile
FROM apache/airflow:2.3.0

# Switch to root for permission adjustments (if needed)
USER root
RUN chown -R airflow: /opt/airflow

# Switch to the airflow user before installing packages
USER airflow
RUN pip install snowflake-connector-python psycopg2-binary plotly streamlit python-dotenv
