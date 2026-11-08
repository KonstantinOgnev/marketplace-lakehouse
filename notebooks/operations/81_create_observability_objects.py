# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — Create observability objects
# MAGIC Creates the missing DQ/metrics tables and compatibility views without
# MAGIC duplicating the existing pipeline, task, or batch registries.

# COMMAND ----------

from pyspark.sql import functions as F

def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

ensure_widget("catalog_name", "marketplace_lab")
CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
OPS = f"{CATALOG}.ops"

PIPELINE_RUNS = f"{OPS}.pipeline_runs"
PIPELINE_TASK_RUNS = f"{OPS}.pipeline_task_runs"
BATCH_REGISTRY = f"{OPS}.batch_registry"
DQ_RESULTS = f"{OPS}.data_quality_results"
TABLE_METRICS = f"{OPS}.table_metrics"
TASK_RUNS_VIEW = f"{OPS}.task_runs"
SOURCE_MANIFEST_VIEW = f"{OPS}.source_batch_manifest"

assert spark.catalog.databaseExists(OPS), f"Missing schema: {OPS}"
for required_table in [PIPELINE_RUNS, PIPELINE_TASK_RUNS, BATCH_REGISTRY]:
    assert spark.catalog.tableExists(required_table), f"Missing source object: {required_table}"

print(f"Creating Stage 9 observability objects in {OPS}")

# COMMAND ----------
# 1. Detailed result of every data-quality rule execution

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_RESULTS}
(
    dq_result_id          STRING        NOT NULL,
    run_id                STRING        NOT NULL,
    task_run_id           STRING,
    batch_id              STRING,
    load_date             DATE,

    layer                 STRING        NOT NULL,
    dataset               STRING        NOT NULL,
    table_name            STRING        NOT NULL,
    check_id              STRING        NOT NULL,
    check_name            STRING        NOT NULL,
    check_type            STRING        NOT NULL,

    severity              STRING        NOT NULL,
    check_status          STRING        NOT NULL,
    threshold_operator    STRING,
    threshold_value       DOUBLE,
    actual_value          DOUBLE,

    evaluated_row_count   BIGINT,
    failed_row_count      BIGINT,
    failure_ratio         DECIMAL(18,8),

    error_code            STRING,
    error_message         STRING,
    sample_records_json   STRING,

    checked_at            TIMESTAMP     NOT NULL,
    created_at            TIMESTAMP     NOT NULL,
    updated_at            TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Row-level results and outcomes of Marketplace Lakehouse data-quality rules'
TBLPROPERTIES
(
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'quality' = 'gold'
)
""")

# COMMAND ----------
# 2. Time-series metrics for every managed pipeline table

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE_METRICS}
(
    metric_id             STRING        NOT NULL,
    run_id                STRING        NOT NULL,
    task_run_id           STRING,
    batch_id              STRING,
    load_date             DATE,

    layer                 STRING        NOT NULL,
    dataset               STRING        NOT NULL,
    table_name            STRING        NOT NULL,
    metric_name           STRING        NOT NULL,

    metric_value          DOUBLE,
    metric_value_string   STRING,
    metric_unit           STRING,
    dimensions            MAP<STRING, STRING>,

    measured_at           TIMESTAMP     NOT NULL,
    created_at            TIMESTAMP     NOT NULL,
    updated_at            TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Historical row-count, file, volume, quarantine and business metrics by table and pipeline run'
TBLPROPERTIES
(
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'quality' = 'gold'
)
""")

# COMMAND ----------
# 3. Compatibility views over the already operational physical registries

spark.sql(f"""
CREATE OR REPLACE VIEW {TASK_RUNS_VIEW}
COMMENT 'Canonical Stage 9 task-run interface backed by pipeline_task_runs'
AS
SELECT *
FROM {PIPELINE_TASK_RUNS}
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SOURCE_MANIFEST_VIEW}
COMMENT 'Canonical Stage 9 source-batch manifest interface backed by batch_registry'
AS
SELECT *
FROM {BATCH_REGISTRY}
""")

# COMMAND ----------
# 4. Validation

expected_objects = {
    "pipeline_runs": PIPELINE_RUNS,
    "task_runs": TASK_RUNS_VIEW,
    "data_quality_results": DQ_RESULTS,
    "source_batch_manifest": SOURCE_MANIFEST_VIEW,
    "table_metrics": TABLE_METRICS,
}

validation_rows = []
for object_name, full_name in expected_objects.items():
    exists = spark.catalog.tableExists(full_name)
    row_count = spark.table(full_name).count() if exists else None
    column_count = len(spark.table(full_name).columns) if exists else None
    validation_rows.append({
        "object_name": object_name,
        "full_name": full_name,
        "exists": exists,
        "row_count": row_count,
        "column_count": column_count,
        "validation_status": "PASSED" if exists else "FAILED",
    })

validation_df = spark.createDataFrame(validation_rows)
display(validation_df.orderBy("object_name"))

failed_objects = [
    row["object_name"]
    for row in validation_rows
    if row["validation_status"] != "PASSED"
]
assert not failed_objects, f"Observability object creation failed: {failed_objects}"

dq_columns = set(spark.table(DQ_RESULTS).columns)
metric_columns = set(spark.table(TABLE_METRICS).columns)

required_dq_columns = {
    "dq_result_id", "run_id", "dataset", "table_name", "check_id",
    "severity", "check_status", "failed_row_count", "checked_at",
}
required_metric_columns = {
    "metric_id", "run_id", "dataset", "table_name", "metric_name",
    "metric_value", "measured_at",
}

missing_dq_columns = sorted(required_dq_columns - dq_columns)
missing_metric_columns = sorted(required_metric_columns - metric_columns)
assert not missing_dq_columns, f"Missing DQ columns: {missing_dq_columns}"
assert not missing_metric_columns, f"Missing metric columns: {missing_metric_columns}"

print("Stage 9 observability objects created and validated")
print(f"DQ result table:          {DQ_RESULTS}")
print(f"Table metrics table:      {TABLE_METRICS}")
print(f"Task runs view:           {TASK_RUNS_VIEW}")
print(f"Source manifest view:     {SOURCE_MANIFEST_VIEW}")
print(f"Failed objects:           {len(failed_objects)}")
