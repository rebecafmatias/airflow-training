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
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': latitude,
            'longitude': longitude,
            'temperature': weather_data['temperature'],
            'windspeed': weather_data['windspeed'],
            'winddirection': weather_data['winddirection'],
            'weathercode': weather_data['weathercode']
        }
        return transform_weather_data
    
    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        cursor.execute("""
        INSERT INTO weather_data (
            latitude,
            longitude,
            temperature,
            windspeed,
            winddirection,
            weathercode
            )
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

    ## DAG Workflow - ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)