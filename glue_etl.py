import sys  # Importing sys module for system-specific parameters and functions
import json  # Importing json module for JSON handling
import boto3  # Importing boto3 module for AWS SDK for Python
import s3fs  # Importing s3fs module for accessing S3 filesystem
import pandas as pd  # Importing pandas module for data manipulation and analysis
from datetime import datetime  # Importing datetime module for datetime manipulation
from awsglue.transforms import *  # Importing necessary transformations from AWS Glue
from awsglue.utils import getResolvedOptions  # Importing utility function to retrieve resolved options
from pyspark.context import SparkContext  # Importing SparkContext from PySpark
from awsglue.context import GlueContext  # Importing GlueContext from AWS Glue
from awsglue.job import Job  # Importing Job class from AWS Glue for Glue job operations
from awsglue.dynamicframe import DynamicFrame  # Importing DynamicFrame for Glue dynamic frame operations
from pyspark.sql import SQLContext  # Importing SQLContext from PySpark for SQL operations
  
# Creating or retrieving a SparkContext
sc = SparkContext.getOrCreate()
# Creating a GlueContext using the SparkContext
glueContext = GlueContext(sc)
# Creating a SparkSession using the GlueContext
spark = glueContext.spark_session
# Creating a Glue job using the GlueContext
job = Job(glueContext)

# Function to flatten JSON data
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

# Reading JSON data from S3 into a DataFrame
df = spark.read.json('s3://airflow-andrei-bucket/DAGoutput/', multiLine=True)

# Checking the type of DataFrame
type(df)

# Converting DataFrame to JSON and collecting it
data_dict = df.toJSON().map(lambda x: json.loads(x)).collect()

# Flattening JSON data
flat = flatten_json(data_dict)
# Converting flattened JSON data to Pandas DataFrame
data_transform = pd.json_normalize(flat)

# Displaying data types of columns in the DataFrame
data_transform.dtypes

# Dropping unnecessary columns from the DataFrame
data_transform = data_transform.drop(columns=['0_weather_0_id', '0_weather_0_icon', '0_base', '0_visibility', '0_sys_type', '0_sys_id', '0_id', '0_cod'])

# Dictionary to store new column names after renaming
new_column_names = {}

# Renaming columns in the DataFrame
for column in data_transform.columns:
    new_column_name = column.replace('main_', '').replace('sys_', '').replace('0_', '').replace('all', 'percentage').replace('name', 'city').replace('coord', 'co-ords')
    print(new_column_name)
    new_column_names[column] = new_column_name
    data_transform = data_transform.rename(columns=new_column_names)

# Columns with timestamp values to convert
columns_to_convert = ['dt', 'sunrise', 'sunset', 'timezone']

# Converting timestamp columns to datetime objects
for column in columns_to_convert:
    data_transform[column] = data_transform[column].apply(lambda x: datetime.utcfromtimestamp(x))

# Columns with temperature values to convert
temps_to_convert = ['temp', 'feels_like', 'temp_min', 'temp_max']

# Converting temperature values from Kelvin to Celsius
for column in temps_to_convert:
    data_transform[column] = data_transform[column] - 273.15

# Creating a SQLContext using the SparkContext
sqlContext = SQLContext(sc)
# Creating a Spark DataFrame from the transformed Pandas DataFrame
spark_dff = sqlContext.createDataFrame(data_transform)

# Converting Spark DataFrame to Glue DynamicFrame
dyfCustomersConvert = DynamicFrame.fromDF(spark_dff, glueContext, "convert")
# Displaying contents of the DynamicFrame
dyfCustomersConvert.show()

# JDBC connection options for writing data to Redshift
my_conn_options = {
    "dbtable": "public.weather_data",
    "database": "dev"
}

# Writing DynamicFrame to Redshift using Glue
redshift_results = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dyfCustomersConvert,
    catalog_connection="redshift_connection",
    connection_options=my_conn_options,
    redshift_tmp_dir="s3://airflow-andrei-bucket/tmp_glue_redshift/"
)

# Committing the Glue job
job.commit()