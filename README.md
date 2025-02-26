# Multi-Source Data Pipeline: Ingesting CSV, API, and Postgres Data with Airflow, Snowflake, and Streamlit

This project is an end-to-end data pipeline that ingests data from multiple sources—CSV, API, and Postgres—and loads it into Snowflake. Airflow orchestrates the data ingestion tasks via dedicated DAGs, and Streamlit is used for interactive data visualization. The entire solution is containerized using Docker Compose.

## Project Components

- **Data Sources**
  - **CSV File (Schools):** A generated CSV containing school data.
  - **Open API (Healthcare):** Data fetched from an open-source API.
  - **Postgres Database (Consumption):** Data stored in a Postgres database.

- **Orchestration: Apache Airflow**
  - **DAG `upload_schools_csv`:** Reads the CSV and loads the data into Snowflake.
  - **DAG `transfer_consumption_data`:** Extracts consumption data from Postgres and loads it into Snowflake.
  - **DAG `transfer_healthcare_data`:** Fetches healthcare data from an API and loads it into Snowflake.
  - All DAGs include default arguments (owner, start date, retries, retry delay), a daily schedule, and are not paused upon creation.

- **Data Warehouse: Snowflake**
  - Automatically creates tables based on the incoming data.
  - Serves as the central repository for all ingested data.

- **Visualization: Streamlit**
  - Provides an interactive dashboard with multiple charts and filters.
  - Enables users to explore data from different sources loaded into Snowflake.

- **Deployment: Docker**
  - Uses Docker Compose to run Airflow (webserver, scheduler, and initialization service) and Postgres.
  - A custom Airflow image includes all necessary dependencies.

## Flow Diagram

```mermaid
flowchart TD
    A[Data Sources]
    A1[CSV File (Schools)]
    A2[Open API (Healthcare)]
    A3[Postgres (Consumption)]
    
    B[Airflow DAGs]
    B1[upload_schools_csv]
    B2[transfer_consumption_data]
    B3[transfer_healthcare_data]
    
    C[Snowflake Data Warehouse]
    D[Streamlit Dashboard]

    A1 -->|Ingest| B1
    A2 -->|Ingest| B3
    A3 -->|Ingest| B2
    
    B1 -->|Load Data| C
    B2 -->|Load Data| C
    B3 -->|Load Data| C
    
    C -->|Consolidated Data| D

    D -->|Interactive Visualization| End[End User]
```

## Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/waitambatha/Multi-Source-Data-Pipeline-Ingesting-CSV-API---Postgres-Data-with-Airflow-Snowflake---Streamlit.git
 
   ```

2. **Generate the CSV File**

   Run the provided script to generate the CSV file for school data:

   ```bash
   python generate_csv.py
   ```

3. **Configure Environment Variables**

   Create a `.env` file in the project root with your Snowflake credentials:

   ```dotenv
   SNOWFLAKE_USER=TKWAITA
   SNOWFLAKE_PASSWORD=Mbatha45
   SNOWFLAKE_ACCOUNT=UZ20522.ap-south-1.aws
   SNOWFLAKE_DATABASE=AB_DATABASE
   SNOWFLAKE_SCHEMA=AB_SCHEMA
   SNOWFLAKE_WAREHOUSE=AB_WAREHOUSE
   ```

4. **Build and Start the Docker Containers**

   Use Docker Compose to build and start the services:

   ```bash
   docker-compose up --build
   ```

   This command will:
   - Start a Postgres container as the Airflow metadata database.
   - Run an `airflow-init` service that initializes the Airflow database and creates the admin user.
   - Launch Airflow webserver and scheduler containers (which share the same secret key).
   
5. **Access the Services**

   - **Airflow UI:** Open [http://localhost:8080](http://localhost:8080) and log in using the credentials `admin/admin`.
   - **Streamlit Dashboard:** Run the Streamlit app:
     ```bash
     streamlit run streamlit_app.py
     ```
     Then access the dashboard in your browser at the default port (usually [http://localhost:8501](http://localhost:8501)).

## Project Structure

```
project/
├── dags/
│   ├── upload_to_snowflake.py
│   ├── transfer_consumption_data.py
│   └── transfer_healthcare_data.py
├── data/
│   └── schools.csv
├── Dockerfile
├── docker-compose.yml
├── generate_csv.py
├── streamlit_app.py
└── .env
```

## Additional Notes

- **Logging & Synchronization:**  
  Ensure that all Airflow components (webserver, scheduler, workers) have the same `AIRFLOW__WEBSERVER__SECRET_KEY` configured and that system time is synchronized across containers.
  
- **Extensibility:**  
  The pipeline is designed to be easily extended to include additional data sources or transformations as needed.

