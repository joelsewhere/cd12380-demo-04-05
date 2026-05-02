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

table_name = config['table']
sql_s3_path = config['sql']
upsert_keys = config['upsert_keys']
partition_keys = config.get('partition_keys', [])
ignore_columns = config.get('ignore_columns', [])

sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session


# Unique view for parallel safety
unique_stg = f"stg_{table_name}_{str(uuid.uuid4())[:8]}"

# 2. Fetch SQL from S3
s3 = boto3.client('s3')
bucket, key = sql_s3_path.replace("s3://", "").split("/", 1)
sql_query = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')

# 3. Create Staging & Filter
raw_df = spark.sql(sql_query)
# Filter ignored columns
stg_df = raw_df.select([c for c in raw_df.columns if c not in ignore_columns])

# Drop duplicates on upsert keys
stg_df = stg_df.dropDuplicates(upsert_keys)

if partition_keys:
    stg_df = stg_df.sortWithinPartitions(*partition_keys)

# Create staging table
stg_df.createOrReplaceTempView(unique_stg)

target_table = f"iceberg.transactions.{table_name}"

table_check = spark.sql(f"SHOW TABLES IN iceberg.transactions LIKE '{table_name}'")

# 4. Table Management
if table_check.count() == 0:
    # Initialize partitioned Iceberg table
    writer = stg_df.writeTo(target_table).using("iceberg")
    
    if partition_keys:
        writer = writer.partitionedBy(*partition_keys)
    
    writer.create()
else:
    # 5. Evolution Check
    target_df = spark.table(target_table)
    new_cols = set(stg_df.columns) - set(target_df.columns)
    
    if new_cols:
        for col in new_cols:
            # Iceberg-compatible SQL type (e.g., DECIMAL(10,2))
            col_type = stg_df.schema[col].dataType.simpleString()
            spark.sql(f"ALTER TABLE {target_table} ADD COLUMN {col} {col_type}")

    # 6. Iceberg-Optimized MERGE
    # Inclusion of partition keys in ON clause enables Partition Pruning
    on_clause = " AND ".join([f"t.{k} = s.{k}" for k in upsert_keys])
    
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING {unique_stg} s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"Completed Iceberg merge for {table_name}")
