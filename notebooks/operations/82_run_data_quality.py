# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — Data quality and table metrics
# MAGIC Executes blocking and non-blocking quality rules, persists every result,
# MAGIC captures historical table metrics, and records the task outcome.

# COMMAND ----------

from datetime import datetime, timezone
from decimal import Decimal
import hashlib

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "marketplace_lab"
DQ_RESULTS_TABLE = f"{CATALOG}.ops.data_quality_results"
TABLE_METRICS_TABLE = f"{CATALOG}.ops.table_metrics"
PIPELINE_RUNS_TABLE = f"{CATALOG}.ops.pipeline_runs"
TASK_AUDIT_TABLE = f"{CATALOG}.ops.pipeline_task_runs"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.invalid_records"
REMEDIATION_TABLE = f"{CATALOG}.ops.quarantine_remediation_log"
TASK_KEY = "data_quality"

def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

for name, default in {
    "run_id": "",
    "task_run_id": "MANUAL",
    "repair_count": "0",
    "load_date": "",
    "quarantine_threshold": "0.05",
}.items():
    ensure_widget(name, default)

RUN_ID = (dbutils.widgets.get("run_id") or "").strip()
TASK_RUN_ID = (dbutils.widgets.get("task_run_id") or "MANUAL").strip()
REPAIR_COUNT = int((dbutils.widgets.get("repair_count") or "0").strip())
LOAD_DATE_RAW = (dbutils.widgets.get("load_date") or "").strip()
QUARANTINE_THRESHOLD = float(
    (dbutils.widgets.get("quarantine_threshold") or "0.05").strip()
)
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

for required_table in [
    DQ_RESULTS_TABLE,
    TABLE_METRICS_TABLE,
    PIPELINE_RUNS_TABLE,
    TASK_AUDIT_TABLE,
    QUARANTINE_TABLE,
]:
    assert spark.catalog.tableExists(required_table), f"Missing table: {required_table}"

if not RUN_ID:
    latest_run = (
        spark.table(PIPELINE_RUNS_TABLE)
        .orderBy(F.col("created_at").desc())
        .select("run_id", "load_date")
        .first()
    )
    assert latest_run is not None, "run_id is required and pipeline_runs is empty"
    RUN_ID = latest_run["run_id"]

run_row = (
    spark.table(PIPELINE_RUNS_TABLE)
    .filter(F.col("run_id") == RUN_ID)
    .select("load_date")
    .limit(1)
    .first()
)
assert run_row is not None, f"Unknown run_id: {RUN_ID}"

LOAD_DATE = LOAD_DATE_RAW or str(run_row["load_date"])
LOAD_DATE_VALUE = datetime.strptime(LOAD_DATE, "%Y-%m-%d").date()
print(f"DQ run_id: {RUN_ID}")
print(f"DQ load_date: {LOAD_DATE}")
print(f"Quarantine threshold: {QUARANTINE_THRESHOLD:.2%}")

# COMMAND ----------
# Source DataFrames

sellers_df = spark.table(f"{CATALOG}.silver.sellers")
products_df = spark.table(f"{CATALOG}.silver.products")
orders_df = spark.table(f"{CATALOG}.silver.orders")
order_items_df = spark.table(f"{CATALOG}.silver.order_items")
payments_df = spark.table(f"{CATALOG}.silver.payments")
events_df = spark.table(f"{CATALOG}.silver.user_events")
quarantine_df = spark.table(QUARANTINE_TABLE)

# Quarantine is immutable history. Resolution is tracked separately so DQ can
# distinguish historical, resolved and still-active records.
if spark.catalog.tableExists(REMEDIATION_TABLE):
    resolved_remediation_df = (
        spark.table(REMEDIATION_TABLE)
        .filter(F.col("remediation_status") == "RESOLVED")
        .select("quarantine_id")
        .distinct()
    )
else:
    resolved_remediation_df = spark.createDataFrame([], "quarantine_id string")

resolved_quarantine_df = quarantine_df.join(
    resolved_remediation_df,
    "quarantine_id",
    "inner",
)
active_quarantine_df = quarantine_df.join(
    resolved_remediation_df,
    "quarantine_id",
    "left_anti",
)

ALLOWED_ORDER_STATUSES = ["CANCELLED", "CREATED", "DELIVERED", "PAID", "SHIPPED"]

def stable_id(*parts):
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()

def sample_json(df, limit=5):
    return "[" + ",".join(df.limit(limit).toJSON().collect()) + "]"

dq_rows = []

def add_rule(
    *, check_id, check_name, check_type, severity, layer, dataset,
    table_name, evaluated_count, failed_df, threshold_operator,
    threshold_value, actual_value=None, error_code=None, error_message=None,
):
    failed_count = failed_df.count()
    ratio = (failed_count / evaluated_count) if evaluated_count else 0.0
    actual = float(failed_count if actual_value is None else actual_value)

    if threshold_operator == "=":
        passed = actual == float(threshold_value)
    elif threshold_operator == "<=":
        passed = actual <= float(threshold_value)
    elif threshold_operator == ">=":
        passed = actual >= float(threshold_value)
    else:
        raise ValueError(f"Unsupported threshold operator: {threshold_operator}")

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    dq_rows.append({
        "dq_result_id": stable_id(RUN_ID, check_id, table_name),
        "run_id": RUN_ID,
        "task_run_id": TASK_RUN_ID,
        "batch_id": None,
        "load_date": LOAD_DATE_VALUE,
        "layer": layer,
        "dataset": dataset,
        "table_name": table_name,
        "check_id": check_id,
        "check_name": check_name,
        "check_type": check_type,
        "severity": severity,
        "check_status": "PASSED" if passed else "FAILED",
        "threshold_operator": threshold_operator,
        "threshold_value": float(threshold_value),
        "actual_value": actual,
        "evaluated_row_count": int(evaluated_count),
        "failed_row_count": int(failed_count),
        "failure_ratio": Decimal(str(round(ratio, 8))),
        "error_code": None if passed else error_code,
        "error_message": None if passed else error_message,
        "sample_records_json": None if passed else sample_json(failed_df),
        "checked_at": now,
        "created_at": now,
        "updated_at": now,
    })

# COMMAND ----------
# Core business and contract rules

add_rule(
    check_id="EVT_001", check_name="event_id is mandatory",
    check_type="NOT_NULL", severity="ERROR", layer="silver",
    dataset="user_events", table_name=f"{CATALOG}.silver.user_events",
    evaluated_count=events_df.count(),
    failed_df=events_df.filter(F.col("event_id").isNull() | (F.trim("event_id") == "")),
    threshold_operator="=", threshold_value=0,
    error_code="NULL_EVENT_ID", error_message="event_id must not be null or empty",
)

duplicate_orders_df = (
    orders_df.groupBy("order_id").count().filter(F.col("count") > 1)
)
add_rule(
    check_id="ORD_001", check_name="current order_id is unique",
    check_type="UNIQUENESS", severity="ERROR", layer="silver",
    dataset="orders", table_name=f"{CATALOG}.silver.orders",
    evaluated_count=orders_df.count(), failed_df=duplicate_orders_df,
    threshold_operator="=", threshold_value=0,
    error_code="DUPLICATE_ORDER_ID", error_message="order_id must be unique in current state",
)

add_rule(
    check_id="ORD_002", check_name="order amount is non-negative",
    check_type="DOMAIN", severity="ERROR", layer="silver",
    dataset="orders", table_name=f"{CATALOG}.silver.orders",
    evaluated_count=orders_df.count(),
    failed_df=orders_df.filter(F.col("order_amount") < 0),
    threshold_operator="=", threshold_value=0,
    error_code="NEGATIVE_ORDER_AMOUNT", error_message="order_amount must be greater than or equal to zero",
)

add_rule(
    check_id="ORD_003", check_name="order status is allowed",
    check_type="ACCEPTED_VALUES", severity="ERROR", layer="silver",
    dataset="orders", table_name=f"{CATALOG}.silver.orders",
    evaluated_count=orders_df.count(),
    failed_df=orders_df.filter(~F.col("order_status").isin(ALLOWED_ORDER_STATUSES)),
    threshold_operator="=", threshold_value=0,
    error_code="INVALID_ORDER_STATUS", error_message="order_status is outside the allowed domain",
)

orphan_sellers_df = (
    orders_df.select("order_id", "seller_id")
    .join(sellers_df.select("seller_id").distinct(), "seller_id", "left_anti")
)
add_rule(
    check_id="ORD_004", check_name="seller_id exists in seller dimension",
    check_type="REFERENTIAL_INTEGRITY", severity="ERROR", layer="silver",
    dataset="orders", table_name=f"{CATALOG}.silver.orders",
    evaluated_count=orders_df.count(), failed_df=orphan_sellers_df,
    threshold_operator="=", threshold_value=0,
    error_code="UNKNOWN_SELLER", error_message="orders.seller_id must exist in silver.sellers",
)

# COMMAND ----------
# Quarantine-ratio rules by source dataset

bronze_tables = {
    "sellers": f"{CATALOG}.bronze.sellers_raw",
    "products": f"{CATALOG}.bronze.products_raw",
    "orders": f"{CATALOG}.bronze.orders_raw",
    "payments": f"{CATALOG}.bronze.payments_raw",
    "user_events": f"{CATALOG}.bronze.user_events_raw",
}

for dataset, bronze_table in bronze_tables.items():
    bronze_count = spark.table(bronze_table).count()
    dataset_quarantine_df = active_quarantine_df.filter(F.col("dataset") == dataset)
    quarantine_count = dataset_quarantine_df.count()
    quarantine_ratio = (quarantine_count / bronze_count) if bronze_count else 0.0
    add_rule(
        check_id=f"QRT_{dataset.upper()}",
        check_name=f"{dataset} active quarantine ratio within threshold",
        check_type="RATIO", severity="ERROR", layer="quarantine",
        dataset=dataset, table_name=QUARANTINE_TABLE,
        evaluated_count=bronze_count, failed_df=dataset_quarantine_df,
        threshold_operator="<=", threshold_value=QUARANTINE_THRESHOLD,
        actual_value=quarantine_ratio,
        error_code="QUARANTINE_THRESHOLD_EXCEEDED",
        error_message=f"Quarantine ratio exceeds {QUARANTINE_THRESHOLD:.2%}",
    )

# COMMAND ----------
# Unexpected row-count decrease against the latest earlier metric

current_silver_tables = {
    "sellers": f"{CATALOG}.silver.sellers",
    "products": f"{CATALOG}.silver.products",
    "orders": f"{CATALOG}.silver.orders",
    "order_items": f"{CATALOG}.silver.order_items",
    "payments": f"{CATALOG}.silver.payments",
    "user_events": f"{CATALOG}.silver.user_events",
}

for dataset, table_name in current_silver_tables.items():
    current_count = spark.table(table_name).count()
    prior_metric = (
        spark.table(TABLE_METRICS_TABLE)
        .filter(
            (F.col("table_name") == table_name)
            & (F.col("metric_name") == "row_count")
            & (F.col("run_id") != RUN_ID)
        )
        .orderBy(F.col("measured_at").desc())
        .select("metric_value")
        .first()
    )
    previous_count = int(prior_metric["metric_value"]) if prior_metric else current_count
    decrease = max(previous_count - current_count, 0)
    decrease_df = spark.createDataFrame(
        [(table_name, previous_count, current_count)] if decrease else [],
        "table_name string, previous_count long, current_count long",
    )
    add_rule(
        check_id=f"VOL_{dataset.upper()}",
        check_name=f"{dataset} row count did not unexpectedly decrease",
        check_type="VOLUME", severity="ERROR", layer="silver",
        dataset=dataset, table_name=table_name,
        evaluated_count=current_count, failed_df=decrease_df,
        threshold_operator=">=", threshold_value=previous_count,
        actual_value=current_count,
        error_code="UNEXPECTED_ROW_COUNT_DECREASE",
        error_message=f"Current row count is below previous value {previous_count}",
    )

# COMMAND ----------
# Persist DQ outcomes idempotently

dq_schema = spark.table(DQ_RESULTS_TABLE).schema
dq_results_df = spark.createDataFrame(dq_rows, dq_schema)
dq_results_df.createOrReplaceTempView("incoming_data_quality_results")

spark.sql(f"""
MERGE INTO {DQ_RESULTS_TABLE} AS target
USING incoming_data_quality_results AS source
ON target.dq_result_id = source.dq_result_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

display(
    dq_results_df
    .select(
        "check_id", "dataset", "check_name", "severity", "check_status",
        "actual_value", "threshold_operator", "threshold_value",
        "evaluated_row_count", "failed_row_count", "failure_ratio",
    )
    .orderBy("check_status", "dataset", "check_id")
)

# COMMAND ----------
# Capture table metrics for Bronze, Silver, Gold and quarantine

monitored_tables = {
    "bronze": [
        "sellers_raw", "products_raw", "orders_raw", "payments_raw", "user_events_raw",
    ],
    "silver": [
        "sellers", "products", "orders", "order_items", "payments", "user_events",
    ],
    "gold": [
        "dim_seller", "dim_product", "dim_user", "fact_orders", "fact_order_items",
        "fact_payments", "fact_events", "agg_daily_seller",
        "agg_daily_marketplace", "agg_conversion_funnel",
    ],
    "quarantine": ["invalid_records"],
}

metric_rows = []
for layer, table_names in monitored_tables.items():
    for short_name in table_names:
        table_name = f"{CATALOG}.{layer}.{short_name}"
        if not spark.catalog.tableExists(table_name):
            continue

        measured_at = datetime.now(timezone.utc).replace(tzinfo=None)
        row_count = spark.table(table_name).count()
        num_files = None
        size_in_bytes = None
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first().asDict()
            num_files = detail.get("numFiles")
            size_in_bytes = detail.get("sizeInBytes")
        except Exception:
            pass

        dataset = short_name.removesuffix("_raw")
        values = {
            "row_count": (float(row_count), "rows"),
            "num_files": (float(num_files), "files") if num_files is not None else None,
            "size_in_bytes": (float(size_in_bytes), "bytes") if size_in_bytes is not None else None,
        }
        for metric_name, metric_data in values.items():
            if metric_data is None:
                continue
            metric_value, metric_unit = metric_data
            metric_rows.append({
                "metric_id": stable_id(RUN_ID, table_name, metric_name),
                "run_id": RUN_ID,
                "task_run_id": TASK_RUN_ID,
                "batch_id": None,
                "load_date": LOAD_DATE_VALUE,
                "layer": layer,
                "dataset": dataset,
                "table_name": table_name,
                "metric_name": metric_name,
                "metric_value": metric_value,
                "metric_value_string": None,
                "metric_unit": metric_unit,
                "dimensions": {"environment": "dev"},
                "measured_at": measured_at,
                "created_at": measured_at,
                "updated_at": measured_at,
            })

# Quarantine lifecycle metrics by source dataset. Historical rows are never
# deleted; resolved and active are complementary subsets of that history.
quarantine_metric_time = datetime.now(timezone.utc).replace(tzinfo=None)
for dataset in bronze_tables:
    historical_count = quarantine_df.filter(F.col("dataset") == dataset).count()
    resolved_count = resolved_quarantine_df.filter(F.col("dataset") == dataset).count()
    active_count = active_quarantine_df.filter(F.col("dataset") == dataset).count()
    assert historical_count == resolved_count + active_count, (
        f"Quarantine lifecycle mismatch for {dataset}: "
        f"historical={historical_count}, resolved={resolved_count}, active={active_count}"
    )

    for metric_name, metric_value in {
        "historical_quarantine_rows": historical_count,
        "resolved_quarantine_rows": resolved_count,
        "active_quarantine_rows": active_count,
    }.items():
        metric_rows.append({
            "metric_id": stable_id(RUN_ID, QUARANTINE_TABLE, dataset, metric_name),
            "run_id": RUN_ID,
            "task_run_id": TASK_RUN_ID,
            "batch_id": None,
            "load_date": LOAD_DATE_VALUE,
            "layer": "quarantine",
            "dataset": dataset,
            "table_name": QUARANTINE_TABLE,
            "metric_name": metric_name,
            "metric_value": float(metric_value),
            "metric_value_string": None,
            "metric_unit": "rows",
            "dimensions": {"lifecycle": metric_name.removesuffix("_quarantine_rows")},
            "measured_at": quarantine_metric_time,
            "created_at": quarantine_metric_time,
            "updated_at": quarantine_metric_time,
        })

metric_schema = spark.table(TABLE_METRICS_TABLE).schema
metrics_df = spark.createDataFrame(metric_rows, metric_schema)
metrics_df.createOrReplaceTempView("incoming_table_metrics")

spark.sql(f"""
MERGE INTO {TABLE_METRICS_TABLE} AS target
USING incoming_table_metrics AS source
ON target.metric_id = source.metric_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

display(
    metrics_df
    .groupBy("layer", "metric_name", "metric_unit")
    .agg(F.count("*").alias("table_count"), F.sum("metric_value").alias("metric_total"))
    .orderBy("layer", "metric_name")
)

# COMMAND ----------
# Task audit and blocking decision

blocking_failures = [
    row for row in dq_rows
    if row["check_status"] == "FAILED" and row["severity"] == "ERROR"
]
warning_failures = [
    row for row in dq_rows
    if row["check_status"] == "FAILED" and row["severity"] == "WARNING"
]
completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
task_status = "FAILED" if blocking_failures else "SUCCEEDED"

audit_schema = spark.table(TASK_AUDIT_TABLE).schema
audit_metrics = {
    "checks_executed": str(len(dq_rows)),
    "blocking_failures": str(len(blocking_failures)),
    "warning_failures": str(len(warning_failures)),
    "metrics_written": str(len(metric_rows)),
    "quarantine_threshold": str(QUARANTINE_THRESHOLD),
}
audit_df = spark.createDataFrame([{
    "run_id": RUN_ID,
    "task_key": TASK_KEY,
    "task_run_id": TASK_RUN_ID,
    "repair_count": REPAIR_COUNT,
    "dataset": "all",
    "task_status": task_status,
    "started_at": STARTED_AT,
    "completed_at": completed_at,
    "input_row_count": sum(spark.table(t).count() for t in current_silver_tables.values()),
    "output_row_count": len(dq_rows),
    "rejected_row_count": len(blocking_failures),
    "source_file_count": None,
    "error_code": "DATA_QUALITY_FAILED" if blocking_failures else None,
    "error_message": (
        ", ".join(row["check_id"] for row in blocking_failures)
        if blocking_failures else None
    ),
    "metrics": audit_metrics,
    "created_at": STARTED_AT,
    "updated_at": completed_at,
}], audit_schema)
audit_df.createOrReplaceTempView("data_quality_task_audit_source")

spark.sql(f"""
MERGE INTO {TASK_AUDIT_TABLE} AS target
USING data_quality_task_audit_source AS source
ON target.run_id = source.run_id
AND target.task_key = source.task_key
AND target.repair_count = source.repair_count
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Stage 9 data-quality execution completed")
print(f"Checks executed:       {len(dq_rows)}")
print(f"Blocking failures:     {len(blocking_failures)}")
print(f"Warning failures:      {len(warning_failures)}")
print(f"Metrics written:       {len(metric_rows)}")
print(f"Task status:           {task_status}")

assert not blocking_failures, (
    "Blocking data-quality checks failed: "
    + ", ".join(row["check_id"] for row in blocking_failures)
)
