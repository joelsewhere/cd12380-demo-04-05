## `analytics`

**Airflow DAG (`analytics`)**
- Manually triggered (no `schedule`); each run rebuilds the analytics layer from the current state of `transactions`.
- Caps concurrency to one active run and two parallel tasks so multiple analytics jobs share a single Iceberg catalog without contention.
- Iterates over every `.sql` file in the local `sql/` directory at parse time, building one **upload + Glue job** pair per query — adding a new analytics table is as simple as dropping another `.sql` file in the directory.
- For each query, runs two tasks in sequence:
  - **`<table>_upload_sql`** — reads the SQL file, renders it through Airflow's templating engine (so any `{{ ds }}`, `{{ dag_run.run_id }}`, etc. resolve), and uploads the rendered query to a run-scoped S3 path under `artifacts/analytics/sql/<run_id>/`. Run-ID scoping prevents concurrent runs from overwriting each other's templates.
  - **`<table>`** — submits a Glue job (`analytics_<table>`) running the shared promote script, with a `--config` JSON pointing at the uploaded SQL and naming the target analytics table.
- Every Glue job uses the standard Iceberg-on-Glue settings (SQL extensions, dedicated `iceberg` catalog backed by the Glue Data Catalog, S3FileIO, warehouse path) and `--enable-glue-datacatalog` so Spark's default catalog can read upstream tables.

**SQL templates (`sql/*.sql`)**
- One file per analytics table: `revenue_by_category`, `customer_segments`, `payment_method_summary`, `daily_order_volume`.
- Each query reads from `iceberg.transactions.*` and produces a fully-aggregated result set covering all history (no incremental windowing) — analytics is rebuilt in full each run.

**Glue script (shared promote runner)**
- Reads the `--config` JSON, fetches the rendered SQL from S3, executes it against the `iceberg` catalog to materialize the result, and writes to `iceberg.analytics.<table>`.
- Uses full overwrite semantics (`createOrReplace`) so each run replaces the previous snapshot of the table — appropriate because the queries return complete aggregations rather than incremental deltas.

**Net effect**
A user triggers the DAG; for every analytics SQL file present, the rendered query is uploaded and a Glue job materializes the result as an Iceberg table in `analytics.<table>`. The whole layer is recomputed from `transactions` on every run, keeping the analytics tables consistent with the current state of upstream data and queryable from Athena.