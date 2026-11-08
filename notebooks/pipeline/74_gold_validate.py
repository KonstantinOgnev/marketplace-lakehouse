# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Gold validation
# MAGIC Final structural, uniqueness, referential-integrity and reconciliation checks.

# COMMAND ----------

from datetime import datetime, timezone
from decimal import Decimal
from pyspark.sql import functions as F
from pyspark.sql import types as T

TASK_KEY = "gold_validate"
TASK_AUDIT_TABLE = "marketplace_lab.ops.pipeline_task_runs"

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
assert RUN_ID, "run_id is required"

TABLES = {
    "dim_seller": "marketplace_lab.gold.dim_seller",
    "dim_product": "marketplace_lab.gold.dim_product",
    "dim_user": "marketplace_lab.gold.dim_user",
    "fact_orders": "marketplace_lab.gold.fact_orders",
    "fact_order_items": "marketplace_lab.gold.fact_order_items",
    "fact_payments": "marketplace_lab.gold.fact_payments",
    "fact_events": "marketplace_lab.gold.fact_events",
    "agg_daily_seller": "marketplace_lab.gold.agg_daily_seller",
    "agg_daily_marketplace": "marketplace_lab.gold.agg_daily_marketplace",
    "agg_conversion_funnel": "marketplace_lab.gold.agg_conversion_funnel",
}

missing_tables = [name for name, table in TABLES.items() if not spark.catalog.tableExists(table)]
assert not missing_tables, f"Missing Gold tables: {missing_tables}"

# COMMAND ----------

checks = []

def add_check(check_name, actual, expected, passed, details=""):
    checks.append({
        "check_name": check_name,
        "actual_value": str(actual),
        "expected_value": str(expected),
        "status": "PASSED" if passed else "FAILED",
        "details": details,
    })

def scalar(sql_text):
    return spark.sql(sql_text).first()[0]

row_counts = {name: spark.table(table).count() for name, table in TABLES.items()}

for name, count in row_counts.items():
    add_check(f"{name}.not_empty", count, "> 0", count > 0)

# Source-to-Gold count reconciliation.
expected_counts = {
    "dim_seller": spark.table("marketplace_lab.silver.sellers").count(),
    "dim_product": spark.table("marketplace_lab.silver.products").count(),
    "dim_user": scalar("""
        SELECT COUNT(DISTINCT user_id)
        FROM (
            SELECT user_id FROM marketplace_lab.silver.orders WHERE user_id IS NOT NULL
            UNION ALL
            SELECT user_id FROM marketplace_lab.silver.user_events WHERE user_id IS NOT NULL
        )
    """),
    "fact_orders": spark.table("marketplace_lab.silver.orders").count(),
    "fact_order_items": spark.table(
        "marketplace_lab.silver.order_items"
    ).count(),
    "fact_payments": spark.table("marketplace_lab.silver.payments").count(),
    "fact_events": spark.table("marketplace_lab.silver.user_events").count(),
}

for name, expected in expected_counts.items():
    actual = row_counts[name]
    add_check(f"{name}.source_row_count", actual, expected, actual == expected)

# COMMAND ----------

# Primary/business-key uniqueness and mandatory-key checks.
key_config = {
    "dim_seller": ["seller_key", "seller_id"],
    "dim_product": ["product_key", "product_id"],
    "dim_user": ["user_key", "user_id"],
    "fact_orders": ["order_key", "order_id"],
    "fact_order_items": ["order_item_key", "order_item_id"],
    "fact_payments": ["payment_key", "payment_id"],
    "fact_events": ["event_key", "event_id"],
    "agg_daily_seller": ["daily_seller_key"],
    "agg_daily_marketplace": ["daily_marketplace_key"],
    "agg_conversion_funnel": ["funnel_key"],
}

for name, keys in key_config.items():
    df = spark.table(TABLES[name])
    for key in keys:
        distinct_count = df.select(key).distinct().count()
        null_count = df.filter(F.col(key).isNull()).count()
        add_check(
            f"{name}.{key}.unique",
            distinct_count,
            row_counts[name],
            distinct_count == row_counts[name],
        )
        add_check(f"{name}.{key}.not_null", null_count, 0, null_count == 0)

# COMMAND ----------

# Referential integrity.
relations = [
    ("dim_product_to_seller", "dim_product", "seller_key", "dim_seller", "seller_key"),
    ("orders_to_user", "fact_orders", "user_key", "dim_user", "user_key"),
    ("orders_to_seller", "fact_orders", "seller_key", "dim_seller", "seller_key"),
    ("items_to_order", "fact_order_items", "order_key", "fact_orders", "order_key"),
    ("items_to_product", "fact_order_items", "product_key", "dim_product", "product_key"),
    ("payments_to_order", "fact_payments", "order_key", "fact_orders", "order_key"),
    ("events_to_user", "fact_events", "user_key", "dim_user", "user_key"),
]

for check_name, child_name, child_key, parent_name, parent_key in relations:
    child = spark.table(TABLES[child_name]).select(child_key).distinct()
    parent = spark.table(TABLES[parent_name]).select(
        F.col(parent_key).alias(child_key)
    ).distinct()
    orphan_count = child.join(parent, child_key, "left_anti").count()
    add_check(f"referential.{check_name}", orphan_count, 0, orphan_count == 0)

# COMMAND ----------

# Aggregate reconciliation and metric-domain checks.
fact_gmv = scalar("""
    SELECT COALESCE(SUM(recognized_gmv_amount), 0)
    FROM marketplace_lab.gold.fact_orders
""")
marketplace_gmv = scalar("""
    SELECT COALESCE(SUM(recognized_gmv), 0)
    FROM marketplace_lab.gold.agg_daily_marketplace
""")
seller_gmv = scalar("""
    SELECT COALESCE(SUM(recognized_gmv), 0)
    FROM marketplace_lab.gold.agg_daily_seller
""")

add_check("aggregate.marketplace_gmv", marketplace_gmv, fact_gmv, marketplace_gmv == fact_gmv)
add_check("aggregate.seller_gmv", seller_gmv, fact_gmv, seller_gmv == fact_gmv)

invalid_bounded_rates = scalar("""
    SELECT
        (SELECT COUNT(*) FROM marketplace_lab.gold.agg_daily_seller
         WHERE cancellation_rate < 0 OR cancellation_rate > 1
            OR successful_payment_rate < 0 OR successful_payment_rate > 1)
      + (SELECT COUNT(*) FROM marketplace_lab.gold.agg_daily_marketplace
         WHERE cancellation_rate < 0 OR cancellation_rate > 1
            OR successful_payment_rate < 0 OR successful_payment_rate > 1)
""")
add_check(
    "aggregate.bounded_rate_domains",
    invalid_bounded_rates,
    0,
    invalid_bounded_rates == 0,
)

# Funnel steps in the synthetic source are not strict nested cohorts. Therefore
# a current-step distinct-user/session count can legitimately exceed the prior
# step count. Negative conversion values remain invalid; values above 1 are
# retained as an informational diagnostic instead of failing the pipeline.
invalid_funnel_rates = scalar("""
    SELECT COUNT(*)
    FROM marketplace_lab.gold.agg_conversion_funnel
    WHERE user_conversion_from_previous < 0
       OR session_conversion_from_previous < 0
       OR user_conversion_from_view < 0
       OR session_conversion_from_view < 0
""")
funnel_rates_above_one = scalar("""
    SELECT COUNT(*)
    FROM marketplace_lab.gold.agg_conversion_funnel
    WHERE user_conversion_from_previous > 1
       OR session_conversion_from_previous > 1
       OR user_conversion_from_view > 1
       OR session_conversion_from_view > 1
""")
add_check(
    "aggregate.funnel_rates_non_negative",
    invalid_funnel_rates,
    0,
    invalid_funnel_rates == 0,
)
add_check(
    "aggregate.funnel_rates_above_one_info",
    funnel_rates_above_one,
    "informational",
    True,
    "Synthetic funnel populations are not strict nested cohorts",
)

failed_checks = [item for item in checks if item["status"] == "FAILED"]
validation_status = "PASSED" if not failed_checks else "FAILED"

checks_df = spark.createDataFrame(checks).orderBy("status", "check_name")
display(checks_df)

print("Gold consolidated validation completed")
print(f"Gold tables:       {len(TABLES)}")
print(f"Checks executed:   {len(checks)}")
print(f"Failed checks:     {len(failed_checks)}")
print(f"Validation status: {validation_status}")

assert not failed_checks, f"Gold validation failed: {failed_checks}"

# COMMAND ----------

COMPLETED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
total_rows = sum(row_counts.values())
metrics = {f"{name}.row_count": str(count) for name, count in row_counts.items()}
metrics["checks_executed"] = str(len(checks))
metrics["failed_checks"] = str(len(failed_checks))
metrics["validation_status"] = validation_status

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

audit_df = spark.createDataFrame([(
    RUN_ID, TASK_KEY, TASK_RUN_ID, REPAIR_COUNT, "gold", "SUCCEEDED",
    STARTED_AT, COMPLETED_AT, total_rows, total_rows, 0, None,
    None, None, metrics, STARTED_AT, COMPLETED_AT,
)], audit_schema)
audit_df.createOrReplaceTempView("gold_validation_audit_source")

spark.sql(f"""
MERGE INTO {TASK_AUDIT_TABLE} AS target
USING gold_validation_audit_source AS source
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
    target.metrics = source.metrics,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

print(f"Gold validation audit recorded for run_id={RUN_ID}")
display(audit_df)
