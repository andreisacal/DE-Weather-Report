import json
import requests
import boto3
from s3fs import S3FileSystem
from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Function to run the ETL process for retrieving OpenWeather data and storing it in S3
def run_open_weather_etl():
    # API key for accessing OpenWeather API (replace '(api_key)' with your actual API key)
    api_key = '(api_key)'
    # City for which weather data is to be retrieved
    city = 'Barcelona'
    # Send GET request to OpenWeather API
    response = requests.get(f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}')
    # Convert the response to JSON format
    data = response.json()

    # Convert JSON data to a formatted string
    json_str = json.dumps(data, indent=4)
    
    # Generate a unique file name based on current timestamp
    file_name = 'open_weather_' + datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '.json'

    # Upload the JSON string to an S3 object
    client = boto3.client('s3')
    client.put_object(
        Bucket='(s3_bucket_name)',  # Replace '(s3_bucket_name)' with your actual S3 bucket name
        Key=f'(s3_bucket_folder)/{file_name}',  # Define the S3 key for storing the file
        Body=json_str  # Set the content of the object to the JSON string
    )

# Define the DAG
with DAG(
    default_args=default_args,
    dag_id='open_weather_api_dag',
    description='DAG which retrieves Open Weather API data and stores the result as a JSON file in AWS S3 - runs everyday @12:00',
    start_date=datetime(2024, 4, 20)  # Start date of the DAG
) as dag:
    # Define a PythonOperator to run the ETL process
    task1 = PythonOperator(
        task_id='run_open_weather_api',
        python_callable=run_open_weather_etl  # Specify the function to be executed
    )

    # Define the task dependency (optional if there's only one task)
    task1  # Task dependency is implied by the order of task definition
