import sys
import json
import boto3
import uuid
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# 1. Context & Config
args = getResolvedOptions(sys.argv, ['config'])
config = json.loads(args['config'])

table_name      = config['table']
sql_s3_path     = config['sql']

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session

# 2. Fetch SQL from S3
s3 = boto3.client('s3')
bucket, key = sql_s3_path.replace("s3://", "").split("/", 1)
sql_query   = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

# 3. Build staging dataframe
df = spark.sql(sql_query)

target_table = f"iceberg.analytics.{table_name}"

writer = df.writeTo(target_table).using("iceberg")
writer.createOrReplace()

print(f"Completed Iceberg overwrite for {table_name}")