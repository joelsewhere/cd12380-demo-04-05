import json
from pathlib import Path
from airflow.sdk import DAG, Asset
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


CWD                   = Path(__file__).parent
SQL                   = CWD / "sql"
GLUE_SCRIPT           = CWD / "glue_script.py"
S3_BUCKET             = "l4-lakehouse-dev-753900908173"
S3_SQL                = "artifacts/analytics/sql/{{ dag_run.run_id }}"
WAREHOUSE_PATH        = f"s3://{S3_BUCKET}/iceberg-warehouse/transactions/"
GLUE_ROLE_NAME        = "dev-lakehouse-glue-role"
AWS_CONN_ID           = "aws_default"
REGION                = "us-east-1"
TRANSACTIONS_UPDATED  = Asset(f"s3://{S3_BUCKET}/iceberg-warehouse/transactions/")

with DAG(
    dag_id="analytics",
    schedule=TRANSACTIONS_UPDATED,
    max_active_tasks=2,
) as dag:

    for query in SQL.glob("*.sql"):

        table_name = query.stem
        SQL_KEY = S3_SQL + f"{table_name}.sql"

        @task(task_id=f"{table_name}_upload_sql")
        def upload(file, s3_key, **context):
            
            ti = context["ti"]
            sql_template = file.read_text()
            sql_rendered = ti.task.render_template(sql_template, context)

            hook = S3Hook()

            hook.load_string(
                string_data=sql_rendered,
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True
                )

        payload = {
            "sql": f"s3://{S3_BUCKET}/{SQL_KEY}",
            "table": table_name
            }

        promote = GlueJobOperator(
            task_id               = f"{table_name}",
            job_name              = f"analytics_{table_name}",
            script_location       = GLUE_SCRIPT.as_posix(),
            s3_bucket             = S3_BUCKET,
            iam_role_name         = GLUE_ROLE_NAME,
            replace_script_file   = True,
            verbose               = True,
            region_name           = "us-east-1",
            script_args           = {
                "--config"                    : json.dumps(payload),
                "--datalake-formats"          : "iceberg",
                "--enable-glue-datacatalog"   : "",
                "--conf"                      : (
                        "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
                        "--conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog "
                        "--conf spark.sql.catalog.iceberg.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                        "--conf spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
                        f"--conf spark.sql.catalog.iceberg.warehouse=s3://{S3_BUCKET}/iceberg-warehouse/"
                        ),
                },
            create_job_kwargs   = {
                "GlueVersion" : "4.0",
                },
            wait_for_completion = True,
            aws_conn_id         = AWS_CONN_ID,
            )

        upload(SQL, SQL_KEY) >> promote


