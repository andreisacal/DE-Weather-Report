import json
import requests
import boto3
from s3fs import S3FileSystem
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def run_open_weather_etl():
    api_key = 'd23445bff4912e1329ebc83f1ff31da7'
    city = 'Barcelona'
    response = requests.get(f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}')
    # Set the GET response to JSON format and save it to a variable
    data = response.json()

    json_str = json.dumps(data, indent=4)
    
    file_name = 'open_weather_' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '.json'

    # Upload JSON String to an S3 Object
    client = boto3.client('s3')

    client.put_object(
    Bucket='airflow-andrei-bucket', 
    Key=f'DAGoutput/{file_name}',
    Body=json_str
    )

with DAG(
    default_args=default_args,
    dag_id='open_weather_api_dag',
    description='DAG which retreives Open Weather API data and stores the result as a JSON file in AWS S3 - runs everyday @12:00',
    start_date=datetime(2024, 4, 20)
) as dag:
    task1 = PythonOperator(
        task_id='run_open_weather_api',
        python_callable=run_open_weather_etl
    )

    task1