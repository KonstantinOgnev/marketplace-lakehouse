# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — Observability views
# MAGIC Creates query-ready operational views for DQ history, pipeline health,
# MAGIC table-volume trends and quarantine trends.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "marketplace_lab"
OPS = f"{CATALOG}.ops"

PIPELINE_RUNS = f"{OPS}.pipeline_runs"
TASK_RUNS = f"{OPS}.pipeline_task_runs"
DQ_RESULTS = f"{OPS}.data_quality_results"
TABLE_METRICS = f"{OPS}.table_metrics"

for required_object in [PIPELINE_RUNS, TASK_RUNS, DQ_RESULTS, TABLE_METRICS]:
    assert spark.catalog.tableExists(required_object), f"Missing object: {required_object}"

# COMMAND ----------
# 1. One row per DQ execution

spark.sql(f"""
CREATE OR REPLACE VIEW {OPS}.v_dq_run_summary
COMMENT 'Aggregated data-quality outcome by pipeline run and load date'
AS
SELECT
    run_id,
    load_date,
    COUNT(*) AS check_count,
    SUM(CASE WHEN check_status = 'PASSED' THEN 1 ELSE 0 END) AS passed_count,
    SUM(CASE WHEN check_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
    SUM(CASE WHEN check_status = 'FAILED' AND severity = 'ERROR' THEN 1 ELSE 0 END)
        AS blocking_failure_count,
    SUM(CASE WHEN check_status = 'FAILED' AND severity = 'WARNING' THEN 1 ELSE 0 END)
        AS warning_failure_count,
    MAX(failure_ratio) AS maximum_failure_ratio,
    MIN(checked_at) AS dq_started_at,
    MAX(checked_at) AS dq_completed_at,
    CASE
        WHEN SUM(CASE WHEN check_status = 'FAILED' AND severity = 'ERROR' THEN 1 ELSE 0 END) > 0
            THEN 'FAILED'
        WHEN SUM(CASE WHEN check_status = 'FAILED' AND severity = 'WARNING' THEN 1 ELSE 0 END) > 0
            THEN 'WARNING'
        ELSE 'PASSED'
    END AS dq_status
FROM {DQ_RESULTS}
GROUP BY run_id, load_date
""")

# COMMAND ----------
# 2. Row-count evolution and change from the previous observed run

spark.sql(f"""
CREATE OR REPLACE VIEW {OPS}.v_table_volume_trend
COMMENT 'Historical table row counts with absolute and percentage change'
AS
WITH row_metrics AS
(
    SELECT
        run_id,
        load_date,
        layer,
        dataset,
        table_name,
        CAST(metric_value AS BIGINT) AS row_count,
        measured_at,
        LAG(CAST(metric_value AS BIGINT)) OVER
        (
            PARTITION BY table_name
            ORDER BY measured_at, run_id
        ) AS previous_row_count
    FROM {TABLE_METRICS}
    WHERE metric_name = 'row_count'
)
SELECT
    run_id,
    load_date,
    layer,
    dataset,
    table_name,
    row_count,
    previous_row_count,
    row_count - previous_row_count AS row_count_change,
    CASE
        WHEN previous_row_count IS NULL OR previous_row_count = 0 THEN NULL
        ELSE ROUND((row_count - previous_row_count) / previous_row_count, 8)
    END AS row_count_change_ratio,
    CASE
        WHEN previous_row_count IS NULL THEN 'NO_BASELINE'
        WHEN row_count < previous_row_count THEN 'DECREASED'
        WHEN row_count > previous_row_count THEN 'INCREASED'
        ELSE 'UNCHANGED'
    END AS volume_status,
    measured_at
FROM row_metrics
""")

# COMMAND ----------
# 3. Quarantine ratio by dataset and run

spark.sql(f"""
CREATE OR REPLACE VIEW {OPS}.v_quarantine_trend
COMMENT 'Historical, resolved and active quarantine metrics by dataset and pipeline run'
AS
WITH lifecycle_metrics AS
(
    SELECT
        run_id,
        dataset,
        MAX(CASE WHEN metric_name = 'historical_quarantine_rows'
                 THEN CAST(metric_value AS BIGINT) END) AS historical_quarantine_row_count,
        MAX(CASE WHEN metric_name = 'resolved_quarantine_rows'
                 THEN CAST(metric_value AS BIGINT) END) AS resolved_quarantine_row_count,
        MAX(CASE WHEN metric_name = 'active_quarantine_rows'
                 THEN CAST(metric_value AS BIGINT) END) AS active_quarantine_row_count
    FROM {TABLE_METRICS}
    WHERE metric_name IN
    (
        'historical_quarantine_rows',
        'resolved_quarantine_rows',
        'active_quarantine_rows'
    )
    GROUP BY run_id, dataset
),
ratio_checks AS
(
    SELECT *
    FROM {DQ_RESULTS}
    WHERE check_type = 'RATIO'
      AND check_id LIKE 'QRT_%'
)
SELECT
    q.run_id,
    q.load_date,
    q.dataset,
    q.evaluated_row_count AS source_row_count,
    COALESCE(m.historical_quarantine_row_count, q.failed_row_count)
        AS historical_quarantine_row_count,
    COALESCE(m.resolved_quarantine_row_count, 0)
        AS resolved_quarantine_row_count,
    COALESCE(m.active_quarantine_row_count, q.failed_row_count)
        AS active_quarantine_row_count,
    COALESCE(m.active_quarantine_row_count, q.failed_row_count)
        AS quarantine_row_count,
    q.actual_value AS active_quarantine_ratio,
    q.actual_value AS quarantine_ratio,
    q.threshold_value AS quarantine_threshold,
    q.check_status,
    q.checked_at
FROM ratio_checks q
LEFT JOIN lifecycle_metrics m
  ON q.run_id = m.run_id
 AND q.dataset = m.dataset
""")

# COMMAND ----------
# 4. Pipeline health: run status + task status + DQ status

pipeline_columns = set(spark.table(PIPELINE_RUNS).columns)
status_column = next(
    (name for name in ["run_status", "status", "pipeline_status"] if name in pipeline_columns),
    None,
)
assert status_column, f"Pipeline status column not found: {sorted(pipeline_columns)}"

def pipeline_expr(column_name, sql_type):
    if column_name in pipeline_columns:
        return f"p.{column_name} AS {column_name}"
    return f"CAST(NULL AS {sql_type}) AS {column_name}"

environment_expr = pipeline_expr("environment", "STRING")
mode_expr = pipeline_expr("mode", "STRING")
load_date_expr = pipeline_expr("load_date", "DATE")
job_run_id_expr = pipeline_expr("job_run_id", "STRING")
started_at_expr = pipeline_expr("started_at", "TIMESTAMP")
completed_at_expr = pipeline_expr("completed_at", "TIMESTAMP")
error_message_expr = pipeline_expr("error_message", "STRING")

spark.sql(f"""
CREATE OR REPLACE VIEW {OPS}.v_pipeline_run_health
COMMENT 'Consolidated pipeline, task and data-quality health by run'
AS
WITH task_summary AS
(
    SELECT
        run_id,
        COUNT(*) AS audited_task_count,
        SUM(CASE WHEN task_status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS successful_task_count,
        SUM(CASE WHEN task_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_task_count,
        COALESCE(SUM(input_row_count), 0) AS total_input_rows,
        COALESCE(SUM(output_row_count), 0) AS total_output_rows,
        COALESCE(SUM(rejected_row_count), 0) AS total_rejected_rows,
        MIN(started_at) AS first_task_started_at,
        MAX(completed_at) AS last_task_completed_at
    FROM {TASK_RUNS}
    GROUP BY run_id
)
SELECT
    p.run_id,
    {job_run_id_expr},
    {environment_expr},
    {mode_expr},
    {load_date_expr},
    p.{status_column} AS run_status,
    {started_at_expr},
    {completed_at_expr},
    CASE
        WHEN p.started_at IS NULL OR p.completed_at IS NULL THEN NULL
        ELSE TIMESTAMPDIFF(SECOND, p.started_at, p.completed_at)
    END AS duration_seconds,
    {error_message_expr},
    COALESCE(t.audited_task_count, 0) AS audited_task_count,
    COALESCE(t.successful_task_count, 0) AS successful_task_count,
    COALESCE(t.failed_task_count, 0) AS failed_task_count,
    COALESCE(t.total_input_rows, 0) AS total_input_rows,
    COALESCE(t.total_output_rows, 0) AS total_output_rows,
    COALESCE(t.total_rejected_rows, 0) AS total_rejected_rows,
    COALESCE(d.check_count, 0) AS dq_check_count,
    COALESCE(d.passed_count, 0) AS dq_passed_count,
    COALESCE(d.failed_count, 0) AS dq_failed_count,
    COALESCE(d.blocking_failure_count, 0) AS dq_blocking_failure_count,
    COALESCE(d.warning_failure_count, 0) AS dq_warning_failure_count,
    COALESCE(d.dq_status, 'NOT_EXECUTED') AS dq_status,
    CASE
        WHEN p.{status_column} = 'FAILED' THEN 'CRITICAL'
        WHEN COALESCE(d.blocking_failure_count, 0) > 0 THEN 'CRITICAL'
        WHEN COALESCE(d.warning_failure_count, 0) > 0 THEN 'WARNING'
        WHEN p.{status_column} = 'SUCCEEDED' AND COALESCE(d.dq_status, 'NOT_EXECUTED') IN ('PASSED', 'NOT_EXECUTED')
            THEN 'HEALTHY'
        ELSE 'UNKNOWN'
    END AS health_status
FROM {PIPELINE_RUNS} p
LEFT JOIN task_summary t
    ON p.run_id = t.run_id
LEFT JOIN {OPS}.v_dq_run_summary d
    ON p.run_id = d.run_id
""")

# COMMAND ----------
# 5. Latest metric value for every monitored table and metric

spark.sql(f"""
CREATE OR REPLACE VIEW {OPS}.v_latest_table_metrics
COMMENT 'Most recent value of every captured metric for each table'
AS
SELECT * EXCEPT (metric_rank)
FROM
(
    SELECT
        run_id,
        load_date,
        layer,
        dataset,
        table_name,
        metric_name,
        metric_value,
        metric_value_string,
        metric_unit,
        measured_at,
        ROW_NUMBER() OVER
        (
            PARTITION BY table_name, metric_name
            ORDER BY measured_at DESC, run_id DESC
        ) AS metric_rank
    FROM {TABLE_METRICS}
)
WHERE metric_rank = 1
""")

# COMMAND ----------
# 6. Validation

views = [
    "v_dq_run_summary",
    "v_table_volume_trend",
    "v_quarantine_trend",
    "v_pipeline_run_health",
    "v_latest_table_metrics",
]

validation_rows = []
for view_name in views:
    full_name = f"{OPS}.{view_name}"
    exists = spark.catalog.tableExists(full_name)
    row_count = spark.table(full_name).count() if exists else None
    column_count = len(spark.table(full_name).columns) if exists else None
    validation_rows.append({
        "view_name": view_name,
        "full_name": full_name,
        "exists": exists,
        "row_count": row_count,
        "column_count": column_count,
        "validation_status": "PASSED" if exists else "FAILED",
    })

validation_df = spark.createDataFrame(validation_rows)
display(validation_df.orderBy("view_name"))

failed_views = [row["view_name"] for row in validation_rows if not row["exists"]]
assert not failed_views, f"Observability view validation failed: {failed_views}"

display(
    spark.table(f"{OPS}.v_pipeline_run_health")
    .orderBy(F.col("started_at").desc())
    .limit(20)
)
display(
    spark.table(f"{OPS}.v_table_volume_trend")
    .orderBy(F.col("measured_at").desc(), "layer", "table_name")
    .limit(100)
)

print("Stage 9 observability views created and validated")
print(f"Views created: {len(views)}")
print(f"Failed views:  {len(failed_views)}")
