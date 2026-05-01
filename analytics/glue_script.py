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
raw_df = spark.sql(sql_query)
stg_df = raw_df.select([c for c in raw_df.columns if c not in ignore_columns])

if partition_keys:
    stg_df = stg_df.sortWithinPartitions(*partition_keys)

target_table = f"iceberg.transactions.{table_name}"
table_check  = spark.sql(f"SHOW TABLES IN iceberg.transactions LIKE '{table_name}'")

# 4. Table management — full overwrite each run
if table_check.count() == 0:
    # First run: create the table from the staging dataframe
    writer = stg_df.writeTo(target_table).using("iceberg")
    if partition_keys:
        writer = writer.partitionedBy(*partition_keys)
    writer.create()
else:
    # 5. Schema evolution check (same logic as the upsert variant)
    target_df = spark.table(target_table)
    new_cols  = set(stg_df.columns) - set(target_df.columns)

    if new_cols:
        if evolve_schema:
            for col in new_cols:
                col_type = stg_df.schema[col].dataType.sql
                spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col} {col_type}")
        else:
            # If we can't evolve, drop incoming columns the target doesn't have
            stg_df = stg_df.select([c for c in stg_df.columns if c in target_df.columns])

    # 6. Full overwrite — replace the entire contents of the table.
    # Iceberg's createOrReplace() rewrites the table data in a new snapshot;
    # the previous snapshot is retained until expiration so historical
    # reads via time travel still work, but the current state is whatever
    # this run produced.
    writer = stg_df.writeTo(target_table).using("iceberg")
    if partition_keys:
        writer = writer.partitionedBy(*partition_keys)
    writer.createOrReplace()

print(f"Completed Iceberg overwrite for {table_name}")