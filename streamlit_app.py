# streamlit_app.py
import streamlit as st
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv
import plotly.express as px

# Load Snowflake credentials from the .env file
load_dotenv('.env')

# Establish connection to Snowflake using credentials
ctx = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
)
cs = ctx.cursor()

st.title("Comprehensive Data Visualization Dashboard")

# Data Source selection
data_source = st.selectbox("Select Data Source", ("School Data", "Consumption Data", "Health Care Data"))

# Define query based on selected data source
if data_source == "School Data":
    query = "SELECT * FROM SCHOOL_DATA;"
elif data_source == "Consumption Data":
    query = "SELECT * FROM CONSUMPTION_DATA;"
else:
    query = "SELECT * FROM HEALTH_CARE;"

cs.execute(query)
data = cs.fetchall()
col_names = [desc[0] for desc in cs.description]
df = pd.DataFrame(data, columns=col_names)

if df.empty:
    st.warning("No data available for the selected source.")
else:
    if data_source != "Health Care Data":
        st.subheader(f"{data_source} - Data")
        st.dataframe(df)
        st.subheader("Summary Statistics")
        st.write(df.describe())
    else:
        st.subheader("Health Care Data - Interactive Analysis")

        # Sidebar filters for Health Care Data
        st.sidebar.header("Filters for Health Care Data")
        # Expecting columns like STATE, MEASURE_NAME, SCORE, and HOSPITAL_NAME (transformed to uppercase)
        if all(col in df.columns for col in ["STATE", "MEASURE_NAME", "SCORE", "HOSPITAL_NAME"]):
            # Convert SCORE to numeric (in case it was loaded as string)
            df["SCORE"] = pd.to_numeric(df["SCORE"], errors="coerce")

            selected_states = st.sidebar.multiselect("Select States", options=df["STATE"].unique(),
                                                     default=df["STATE"].unique())
            selected_measures = st.sidebar.multiselect("Select Measure Names", options=df["MEASURE_NAME"].unique(),
                                                       default=df["MEASURE_NAME"].unique())

            df_filtered = df[(df["STATE"].isin(selected_states)) & (df["MEASURE_NAME"].isin(selected_measures))]

            st.dataframe(df_filtered)
            st.subheader("Summary Statistics")
            st.write(df_filtered.describe())

            st.subheader("Interactive Charts")
            # Chart 1: Bar Chart – Average Score by State
            avg_score_by_state = df_filtered.groupby("STATE")["SCORE"].mean().reset_index()
            fig1 = px.bar(avg_score_by_state, x="STATE", y="SCORE", title="Average Score by State")
            st.plotly_chart(fig1, use_container_width=True)

            # Chart 2: Pie Chart – Distribution of Facilities by State
            state_counts = df_filtered["STATE"].value_counts().reset_index()
            state_counts.columns = ["STATE", "Count"]
            fig2 = px.pie(state_counts, names="STATE", values="Count", title="Facility Distribution by State")
            st.plotly_chart(fig2, use_container_width=True)

            # Chart 3: Scatter Plot – Score by Hospital Name
            fig3 = px.scatter(df_filtered, x="HOSPITAL_NAME", y="SCORE",
                              title="Score by Hospital",
                              hover_data=["STATE", "MEASURE_NAME"])
            st.plotly_chart(fig3, use_container_width=True)

            # Chart 4: Histogram – Score Distribution
            fig4 = px.histogram(df_filtered, x="SCORE", nbins=20, title="Score Distribution")
            st.plotly_chart(fig4, use_container_width=True)

            # Chart 5: Box Plot – Score by State
            fig5 = px.box(df_filtered, x="STATE", y="SCORE", title="Score Distribution by State")
            st.plotly_chart(fig5, use_container_width=True)
        else:
            st.error("Expected columns (STATE, MEASURE_NAME, SCORE, HOSPITAL_NAME) not found in Health Care data.")

cs.close()
ctx.close()
