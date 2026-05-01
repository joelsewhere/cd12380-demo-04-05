import pendulum
from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook

S3_BUCKET      = "l4-lakehouse-dev-753900908173"
LANDING_PREFIX = "landing/"
GLUE_DB        = "raw"
WAREHOUSE_PATH = f"s3://{S3_BUCKET}/iceberg-warehouse/"
CRAWLER_NAME   = "raw"
GLUE_ROLE_ARN  = "dev-lakehouse-glue-role"
GLUE_ROLE_NAME = "dev-lakehouse-glue-role"
AWS_CONN_ID    = "aws_default"

REGION          = "us-east-1"
RAW_INGESTION_COMPLETE = Asset("s3://l4-lakehouse-dev-753900908173/iceberg-warehouse/raw/")


with DAG(
    dag_id="dag_02_raw_ingester",
    description="Auto-ingest landing files into raw Iceberg — triggers DAG 3 via asset",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 2, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["lakehouse", "raw", "ingestion", "iceberg", "stage-3"],
) as dag:


    @task
    def capture_landing_keys(**context) -> list[str]:
        """Discover files in landing and return the S3 prefixes to crawl."""
        interval_start = context["ds"]
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)
        
        # We want the distinct folders (schema/table) under the date prefix
        # e.g., s3://bucket/landing/2026-01-01/ecommerce/orders/
        found_prefixes = set()

        prefixes = s3.list_prefixes(
            bucket_name=S3_BUCKET,
            prefix=LANDING_PREFIX,
            delimiter='/'
            )

        for prefix in prefixes:

            ingestion_prefix = f"{prefix}ingested_date={interval_start}/"
            print(ingestion_prefix)
            for key in s3.list_keys(
                bucket_name=S3_BUCKET,
                prefix=ingestion_prefix
                ):
                parts = key.split('/')
                # Assuming structure: landing/YYYY-MM-DD/schema/table/file.json
                if len(parts) >= 4:
                    schema_table_path = f"s3://{S3_BUCKET}/{parts[0]}/{parts[1]}/"
                    found_prefixes.add(schema_table_path)
            
        return list(found_prefixes)

    @task
    def upsert_crawler(landing_prefixes: list[str]) -> None:
        if not landing_prefixes:
            return
        
        hook = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION)
        targets = [{"Path": p} for p in landing_prefixes]

        if hook.has_crawler(CRAWLER_NAME):
            # Update target to point to the LANDING folders
            hook.update_crawler(
                Name=CRAWLER_NAME,
                Targets={"S3Targets": targets}, # Replacing targets ensures we only crawl current interval
                RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
            )
        else:
            hook.create_crawler(
                Name=CRAWLER_NAME,
                Role=GLUE_ROLE_NAME,
                DatabaseName=GLUE_DB, # "raw"
                Targets={"S3Targets": targets},
                RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
            )

    @task
    def run_crawler() -> None:
        hook = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION)
        hook.start_crawler(CRAWLER_NAME)

    @task
    def wait_for_crawler() -> None:
        hook = GlueCrawlerHook(aws_conn_id=AWS_CONN_ID, region_name=REGION)
        hook.wait_for_crawler_completion(CRAWLER_NAME)

    @task(outlets=[RAW_INGESTION_COMPLETE])
    def notify_complete(prefixes: list[str], **context) -> None:
        # Logic to extract table names from prefixes for the downstream asset
        tables = [p.rstrip('/').split('/')[-1] for p in prefixes]
        context['outlet_events'][RAW_INGESTION_COMPLETE].extra = {
            "ingested_date": context["ds"],
            "tables": tables,
        }

    # Simplified Pipeline
    prefixes = capture_landing_keys()
    upsert = upsert_crawler(prefixes)
    run = run_crawler()
    wait = wait_for_crawler()
    
    prefixes >> upsert >> run >> wait >> notify_complete(prefixes)