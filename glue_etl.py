import sys
import json
import boto3
import s3fs
import pandas as pd
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
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
df= spark.read.json('s3://airflow-andrei-bucket/DAGoutput/', multiLine=True)
type(df)
data_dict = df.toJSON().map(lambda x: json.loads(x)).collect()
flat = flatten_json(data_dict)
data_transform = pd.json_normalize(flat)
data_transform.dtypes
data_transform = data_transform.drop(columns = ['0_weather_0_id', '0_weather_0_icon', '0_base', '0_visibility', '0_sys_type', '0_sys_id', '0_id', '0_cod'])
new_column_names = {}

for column in data_transform.columns:
    new_column_name = column.replace('main_', '').replace('sys_', '').replace('0_', '').replace('all', 'percentage').replace('name', 'city').replace('coord', 'co_ords')
    print(new_column_name)
    new_column_names[column] = new_column_name
    data_transform = data_transform.rename(columns=new_column_names)
columns_to_convert = ['dt', 'sunrise', 'sunset', 'timezone']

for column in columns_to_convert:
    data_transform[column] = data_transform[column].apply(lambda x: datetime.utcfromtimestamp(x))
temps_to_convert = ['temp', 'feels_like', 'temp_min', 'temp_max']

for column in temps_to_convert:
    data_transform[column] = data_transform[column] - 273.15
pd.set_option('display.max_rows', 85)
pd.set_option('display.max_columns', 85)
data_transform
# Import Dynamic DataFrame class
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
spark_dff = sqlContext.createDataFrame(data_transform)
#Convert from Spark Data Frame to Glue Dynamic Frame
dyfCustomersConvert = DynamicFrame.fromDF(spark_dff, glueContext, "convert")
dyfCustomersConvert.show()
my_conn_options = {
    "dbtable": "public.weather_data",
    "database": "dev"
}

redshift_results = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame = dyfCustomersConvert,
    catalog_connection = "redshift_connection",
    connection_options = my_conn_options,
    redshift_tmp_dir = "s3://airflow-andrei-bucket/tmp_glue_redshift/"
    )
job.commit()
