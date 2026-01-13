from airflow import DAG
from airflow.providers.https.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

latitude = "51.5074"
longitude = "-0.1278"
postgres_conn_id="postgres_default"
api_conn_id="open_meteo_api"

default_args={
    "owner":"airflow",
    "start_date":days_ago(1)
}

# Creating DAG

with DAG(dag_id="weather_etl_pipeline",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dags:
    
    @task()
    def extract_weather_data():
        logging.info(f"Extract weather data from Open Meteo API using Airflow Connections.")

        # Use HTTP Hook to get connection details from Airflow connection
        http_hook=HttpHook(http_conn_id=api_conn_id,method="GET")

        # Build the API endpoint
        endpoint=f"/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        url= "https://api.open-meteo.com"

        # Make the request via http hook
        response=http_hook.run(endpoint)

        if response.status_code==200:
            return response.json()
        else:
            logging.error(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data):

