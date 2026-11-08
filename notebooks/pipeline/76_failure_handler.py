# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Pipeline failure handler

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

PIPELINE_RUNS_TABLE = "marketplace_lab.ops.pipeline_runs"
TASK_AUDIT_TABLE = "marketplace_lab.ops.pipeline_task_runs"
TASK_KEY = "failure_handler"

def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

for name, default in {
    "run_id": "",
    "task_run_id": "MANUAL",
    "repair_count": "0",
}.items():
    ensure_widget(name, default)

RUN_ID = (dbutils.widgets.get("run_id") or "").strip()
TASK_RUN_ID = (dbutils.widgets.get("task_run_id") or "MANUAL").strip()
REPAIR_COUNT = int((dbutils.widgets.get("repair_count") or "0").strip())
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
FAILURE_MESSAGE = "One or more upstream Lakeflow tasks failed or were not completed"

assert RUN_ID, "run_id is required"
assert spark.catalog.tableExists(PIPELINE_RUNS_TABLE), f"Missing table: {PIPELINE_RUNS_TABLE}"
assert spark.catalog.tableExists(TASK_AUDIT_TABLE), f"Missing table: {TASK_AUDIT_TABLE}"

# COMMAND ----------

run_columns = set(spark.table(PIPELINE_RUNS_TABLE).columns)
status_column = next(
    (name for name in ["run_status", "status", "pipeline_status"] if name in run_columns),
    None,
)
assert status_column, f"No status column found in {PIPELINE_RUNS_TABLE}"

assignments = [f"{status_column} = 'FAILED'"]
if "completed_at" in run_columns:
    assignments.append("completed_at = current_timestamp()")
if "updated_at" in run_columns:
    assignments.append("updated_at = current_timestamp()")
escaped_message = FAILURE_MESSAGE.replace("'", "''")
for error_column in ["error_message", "failure_message"]:
    if error_column in run_columns:
        assignments.append(f"{error_column} = '{escaped_message}'")

escaped_run_id = RUN_ID.replace("'", "''")
spark.sql(f"""
UPDATE {PIPELINE_RUNS_TABLE}
SET {', '.join(assignments)}
WHERE run_id = '{escaped_run_id}'
""")

# Collect failed audit rows that were recorded before the workflow stopped.
failed_task_rows = (
    spark.table(TASK_AUDIT_TABLE)
    .filter(
        (F.col("run_id") == RUN_ID)
        & F.col("task_status").isin("FAILED", "TIMED_OUT", "CANCELLED")
    )
    .select("task_key", "task_status", "error_code", "error_message")
)
failed_task_count = failed_task_rows.count()

# COMMAND ----------

COMPLETED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
audit_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("task_key", T.StringType(), False),
    T.StructField("task_run_id", T.StringType(), True),
    T.StructField("repair_count", T.IntegerType(), False),
    T.StructField("dataset", T.StringType(), True),
    T.StructField("task_status", T.StringType(), False),
    T.StructField("started_at", T.TimestampType(), False),
    T.StructField("completed_at", T.TimestampType(), True),
    T.StructField("input_row_count", T.LongType(), True),
    T.StructField("output_row_count", T.LongType(), True),
    T.StructField("rejected_row_count", T.LongType(), True),
    T.StructField("source_file_count", T.LongType(), True),
    T.StructField("error_code", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField("metrics", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("created_at", T.TimestampType(), False),
    T.StructField("updated_at", T.TimestampType(), False),
])

metrics = {
    "handled_pipeline_status": "FAILED",
    "status_column": status_column,
    "recorded_failed_task_count": str(failed_task_count),
}
audit_df = spark.createDataFrame([(
    RUN_ID, TASK_KEY, TASK_RUN_ID, REPAIR_COUNT, "pipeline", "SUCCEEDED",
    STARTED_AT, COMPLETED_AT, failed_task_count, 1, 0, None,
    "UPSTREAM_TASK_FAILURE", FAILURE_MESSAGE, metrics, STARTED_AT, COMPLETED_AT,
)], audit_schema)
audit_df.createOrReplaceTempView("failure_handler_audit_source")

spark.sql(f"""
MERGE INTO {TASK_AUDIT_TABLE} AS target
USING failure_handler_audit_source AS source
  ON target.run_id = source.run_id
 AND target.task_key = source.task_key
 AND target.repair_count = source.repair_count
WHEN MATCHED THEN UPDATE SET
    target.task_run_id = source.task_run_id,
    target.task_status = source.task_status,
    target.started_at = source.started_at,
    target.completed_at = source.completed_at,
    target.input_row_count = source.input_row_count,
    target.output_row_count = source.output_row_count,
    target.rejected_row_count = source.rejected_row_count,
    target.error_code = source.error_code,
    target.error_message = source.error_message,
    target.metrics = source.metrics,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

dbutils.jobs.taskValues.set(key="pipeline_status", value="FAILED")
print(f"Pipeline failure registered: run_id={RUN_ID}, status=FAILED")
print(f"Failed task audit rows available: {failed_task_count}")
display(failed_task_rows)

# The operational state has already been persisted. Re-raise deliberately so
# Lakeflow keeps the whole run in a terminal FAILED state and standard failure
# notifications/monitoring are triggered.
raise RuntimeError(
    f"Pipeline marked FAILED after failure handling: run_id={RUN_ID}"
)
