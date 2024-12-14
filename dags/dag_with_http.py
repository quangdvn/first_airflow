from airflow import DAG
from airflow.decorators import task
from airflow.providers.https.hooks.http import HttpHook  # Read from API
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.date import days_ago

TOKYO_LATITUDE = '35.6762'
TOKYO_LONGITUDE = '139.6503'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_agrs = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1
}

# DAG

with DAG(dag_id='weather_etl_pipeline',
         default_args=default_agrs,
         schedule_interval='@daily',
         catchup=False) as dags:

  @task()
  def extract_weather_data():
    # Use HTTP Hook to get weather data
    api_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')

    # Build the API Endpoint and return response
    # https://api.open-meteo.com/v1
    endpoint = f'/v1/forecast?latitude={TOKYO_LATITUDE}&longitude={TOKYO_LONGITUDE}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m'
    response = api_hook.run(endpoint)
    response.raise_for_status()
    return response.json()

  @task()
  def transform_weather_data(weather_data):
    current_weather = weather_data['current_weather']
    transformed_data = {
        'latitude': TOKYO_LATITUDE,
        'longitude': TOKYO_LONGITUDE,
        'temperature': current_weather['temperature'],
        'windspeed': current_weather['windspeed'],
        'winddirection': current_weather['winddirection'],
        'weathercode': current_weather['weathercode'],
    }
    return transformed_data

  @task()
  def load_weather_data(transformed_data):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    create_sql = """
			CREATE TABLE IF NOT EXISTS weather_data (
				longtitude FLOAT,
				latitude FLOAT,
				temperature FLOAT,
				humidity FLOAT,
				windspeed FLOAT,
				winddirection FLOAT,
				weathercode FLOAT,
				timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		"""
    cursor.execute(create_sql)

    # Insert transformed data into the table
    insert_sql = """
			INSERT INTO weather_data (longtitude, latitude, temperature, humidity, windspeed, winddirection, weathercode)
			VALUES (%s, %s, %s, %s, %s, %s, %s)
		"""
    cursor.execute(insert_sql, (
        transformed_data['longitude'],
        transformed_data['latitude'],
        transformed_data['temperature'],
        transformed_data['humidity'],
        transformed_data['windspeed'],
        transformed_data['winddirection'],
        transformed_data['weathercode']
    ))

    conn.commit()
    cursor.close()
    conn.close()

  # Task flow - ETL Pipeline
  weather_data = extract_weather_data()
  transformed_data = transform_weather_data(weather_data)
  load_weather_data(transformed_data)
