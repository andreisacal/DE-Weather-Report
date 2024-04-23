import sys  # Importing the sys module to access system-specific parameters and functions
import json  # Importing the json module for JSON data handling
import boto3  # Importing the boto3 library for interaction with AWS services
import s3fs  # Importing the s3fs library for interacting with Amazon S3
import pandas as pd  # Importing the pandas library for data manipulation and analysis
from datetime import datetime  # Importing the datetime class from the datetime module
from awsglue.transforms import *  # Importing AWS Glue transformations
from awsglue.utils import getResolvedOptions  # Importing utility functions from AWS Glue
from pyspark.context import SparkContext  # Importing SparkContext from PySpark
from awsglue.context import GlueContext  # Importing GlueContext from AWS Glue
from awsglue.job import Job  # Importing Job class from AWS Glue for job management
from awsglue.dynamicframe import DynamicFrame  # Importing DynamicFrame from AWS Glue for dynamic data manipulation
from pyspark.sql import SQLContext  # Importing SQLContext from PySpark for SQL operations

# Creating or getting a SparkContext
sc = SparkContext.getOrCreate()

# Creating a GlueContext using the SparkContext
glueContext = GlueContext(sc)

# Creating a SparkSession using GlueContext
spark = glueContext.spark_session

# Creating a new Job object using GlueContext
job = Job(glueContext)

# Function to flatten JSON data
def flatten_json(y):
    # Initialize an empty dictionary to store the flattened data
    out = {}
    
    # Nested function to recursively flatten the JSON structure
    def flatten(x, name=''):
        # If x is a dictionary
        if type(x) is dict:
            # Iterate over each key-value pair in the dictionary
            for a in x:
                # Recursively call flatten function with updated name (key with underscore) and value
                flatten(x[a], name + a + '_')
        # If x is a list
        elif type(x) is list:
            i = 0
            # Iterate over each item in the list
            for a in x:
                # Recursively call flatten function with updated name (index with underscore) and item
                flatten(a, name + str(i) + '_')
                i += 1
        # If x is neither a dictionary nor a list, it's a leaf node
        else:
            # Assign the value to the output dictionary with the modified name
            out[name[:-1]] = x  # Use name[:-1] to remove the trailing underscore from the name
    # Start the flattening process by calling the flatten function with the input JSON data
    flatten(y)
    # Return the flattened dictionary
    return out

# Reading JSON data from Amazon S3 bucket into a DataFrame
df = spark.read.json('s3://airflow-andrei-bucket/DAGoutput/', multiLine=True)

# Getting the type of DataFrame (it's not used further in the script)
type(df)

# Converting DataFrame to JSON format and then collecting it into a list
data_dict = df.toJSON().map(lambda x: json.loads(x)).collect()

# Flattening the collected JSON data
flat = flatten_json(data_dict)

# Normalizing the flattened JSON data into a pandas DataFrame
data_transform = pd.json_normalize(flat)

# Dropping specified columns from the DataFrame
data_transform = data_transform.drop(columns=['0_weather_0_id', '0_weather_0_icon', '0_base', '0_visibility', '0_sys_type', '0_sys_id', '0_id', '0_cod'])

# Dictionary to store new column names after replacements
new_column_names = {}

# Renaming columns in the DataFrame based on specific replacements
for column in data_transform.columns:
    new_column_name = column.replace('main_', '').replace('sys_', '').replace('0_', '').replace('all', 'percentage').replace('name', 'city').replace('coord', 'co_ords')
    print(new_column_name)
    new_column_names[column] = new_column_name
    data_transform = data_transform.rename(columns=new_column_names)

# List of columns to convert to datetime format
columns_to_convert = ['dt', 'sunrise', 'sunset', 'timezone']

# Converting specified columns to datetime format
for column in columns_to_convert:
    data_transform[column] = data_transform[column].apply(lambda x: datetime.utcfromtimestamp(x))

# List of columns containing temperature values to convert from Kelvin to Celsius
temps_to_convert = ['temp', 'feels_like', 'temp_min', 'temp_max']

# Converting temperature values from Kelvin to Celsius
for column in temps_to_convert:
    data_transform[column] = data_transform[column] - 273.15

# Setting display options for pandas DataFrame
pd.set_option('display.max_rows', 85)
pd.set_option('display.max_columns', 85)

# Displaying the transformed DataFrame
data_transform

# Initializing SQLContext with SparkContext
sqlContext = SQLContext(sc)

# Creating a Spark DataFrame from the pandas DataFrame
spark_dff = sqlContext.createDataFrame(data_transform)

# Converting Spark DataFrame to Glue DynamicFrame
dyfCustomersConvert = DynamicFrame.fromDF(spark_dff, glueContext, "convert")

# Displaying the DynamicFrame
dyfCustomersConvert.show()

# Defining connection options for writing data to Redshift
my_conn_options = {
    "dbtable": "public.weather_data",
    "database": "dev"
}

# Writing the DynamicFrame to Redshift using JDBC connection
redshift_results = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dyfCustomersConvert,
    catalog_connection="redshift_connection",
    connection_options=my_conn_options,
    redshift_tmp_dir="s3://airflow-andrei-bucket/tmp_glue_redshift/"
)

# Committing the job
job.commit()
