# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Consolidated Silver validation

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG = "marketplace_lab"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.invalid_records"
SILVER_RUN_AUDIT_TABLE = f"{CATALOG}.ops.silver_run_audit"
TASK_AUDIT_TABLE = f"{CATALOG}.ops.pipeline_task_runs"


def ensure_widget(name: str, default_value: str = "") -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value)


for widget_name, widget_default in {
    "run_id": "",
    "task_run_id": "MANUAL",
    "repair_count": "0",
}.items():
    ensure_widget(widget_name, widget_default)

RUN_ID = (dbutils.widgets.get("run_id") or "").strip()
TASK_RUN_ID = (dbutils.widgets.get("task_run_id") or "MANUAL").strip()
REPAIR_COUNT = int((dbutils.widgets.get("repair_count") or "0").strip())
TASK_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

assert RUN_ID, "run_id is required"

VALIDATION_CONFIG = {
    "sellers": {
        "bronze_table": f"{CATALOG}.bronze.sellers_raw",
        "silver_table": f"{CATALOG}.silver.sellers",
        "business_key": "seller_id",
    },
    "products": {
        "bronze_table": f"{CATALOG}.bronze.products_raw",
        "silver_table": f"{CATALOG}.silver.products",
        "business_key": "product_id",
    },
    "orders": {
        "bronze_table": f"{CATALOG}.bronze.orders_raw",
        "silver_table": f"{CATALOG}.silver.orders",
        "business_key": "order_id",
    },
    "payments": {
        "bronze_table": f"{CATALOG}.bronze.payments_raw",
        "silver_table": f"{CATALOG}.silver.payments",
        "business_key": "payment_id",
    },
    "user_events": {
        "bronze_table": f"{CATALOG}.bronze.user_events_raw",
        "silver_table": f"{CATALOG}.silver.user_events",
        "business_key": "event_id",
    },
}


# COMMAND ----------
# MAGIC %md
# MAGIC ## Dataset reconciliation

# COMMAND ----------

validation_results = []

for dataset, config in VALIDATION_CONFIG.items():
    assert spark.catalog.tableExists(config["bronze_table"]), config["bronze_table"]
    assert spark.catalog.tableExists(config["silver_table"]), config["silver_table"]

    bronze_df = spark.table(config["bronze_table"])
    silver_df = spark.table(config["silver_table"])
    key_column = config["business_key"]

    bronze_count = bronze_df.count()
    non_null_key_df = bronze_df.filter(
        F.col(key_column).isNotNull()
        & (F.trim(F.col(key_column)) != "")
    )
    non_null_key_count = non_null_key_df.count()
    distinct_non_null_key_count = (
        non_null_key_df
        .select(F.upper(F.trim(F.col(key_column))).alias("normalized_key"))
        .distinct()
        .count()
    )
    superseded_rows = non_null_key_count - distinct_non_null_key_count

    silver_count = silver_df.count()
    distinct_silver_keys = silver_df.select(key_column).distinct().count()
    missing_silver_keys = silver_df.filter(
        F.col(key_column).isNull()
        | (F.trim(F.col(key_column)) == "")
    ).count()
    # Quarantine is an immutable history. A historical rejected version stops
    # being active when the same normalized business key is successfully
    # remediated into Silver. Keeping the source quarantine row preserves
    # lineage without double-counting it in reconciliation.
    silver_normalized_keys_df = (
        silver_df
        .filter(
            F.col(key_column).isNotNull()
            & (F.trim(F.col(key_column)) != "")
        )
        .select(
            F.upper(F.trim(F.col(key_column))).alias(
                "normalized_business_key"
            )
        )
        .distinct()
    )
    active_quarantine_df = (
        spark.table(QUARANTINE_TABLE)
        .filter(F.col("dataset") == dataset)
        .withColumn(
            "normalized_business_key",
            F.upper(F.trim(F.col("business_key"))),
        )
        .join(
            silver_normalized_keys_df,
            "normalized_business_key",
            "left_anti",
        )
    )
    quarantine_count = active_quarantine_df.count()
    reconciled_count = silver_count + quarantine_count + superseded_rows

    validation_status = (
        "PASSED"
        if (
            bronze_count == reconciled_count
            and silver_count == distinct_silver_keys
            and missing_silver_keys == 0
        )
        else "FAILED"
    )

    validation_results.append({
        "run_id": RUN_ID,
        "dataset": dataset,
        "bronze_table": config["bronze_table"],
        "silver_table": config["silver_table"],
        "bronze_row_count": bronze_count,
        "superseded_row_count": superseded_rows,
        "quarantine_row_count": quarantine_count,
        "silver_row_count": silver_count,
        "distinct_silver_keys": distinct_silver_keys,
        "missing_silver_keys": missing_silver_keys,
        "reconciled_row_count": reconciled_count,
        "validation_status": validation_status,
    })

silver_validation_df = (
    spark.createDataFrame(validation_results)
    .withColumn("completed_at", F.current_timestamp())
)

failed_datasets = [
    row["dataset"]
    for row in validation_results
    if row["validation_status"] != "PASSED"
]


# COMMAND ----------
# MAGIC %md
# MAGIC ## Referential integrity and order-item checks

# COMMAND ----------

sellers_df = spark.table(f"{CATALOG}.silver.sellers")
products_df = spark.table(f"{CATALOG}.silver.products")
orders_df = spark.table(f"{CATALOG}.silver.orders")
items_df = spark.table(f"{CATALOG}.silver.order_items")
payments_df = spark.table(f"{CATALOG}.silver.payments")
events_df = spark.table(f"{CATALOG}.silver.user_events")

order_items_count = items_df.count()
distinct_order_item_ids = items_df.select("order_item_id").distinct().count()


def orphan_count(child_df, child_key: str, parent_df, parent_key: str) -> int:
    return (
        child_df
        .filter(F.col(child_key).isNotNull())
        .select(F.col(child_key).alias("join_key"))
        .distinct()
        .join(
            parent_df.select(F.col(parent_key).alias("join_key")).distinct(),
            "join_key",
            "left_anti",
        )
        .count()
    )


referential_results = {
    "products_without_seller": orphan_count(
        products_df, "seller_id", sellers_df, "seller_id"
    ),
    "orders_without_seller": orphan_count(
        orders_df, "seller_id", sellers_df, "seller_id"
    ),
    "items_without_order": orphan_count(
        items_df, "order_id", orders_df, "order_id"
    ),
    "items_without_product": orphan_count(
        items_df, "product_id", products_df, "product_id"
    ),
    "payments_without_order": orphan_count(
        payments_df, "order_id", orders_df, "order_id"
    ),
    "events_without_product": orphan_count(
        events_df, "product_id", products_df, "product_id"
    ),
    "events_without_order": orphan_count(
        events_df, "order_id", orders_df, "order_id"
    ),
}

failed_references = {
    key: value
    for key, value in referential_results.items()
    if value != 0
}

assert not failed_datasets, f"Silver reconciliation failed: {failed_datasets}"
assert distinct_order_item_ids == order_items_count, (
    "Duplicate order_item_id detected"
)
assert not failed_references, (
    f"Referential-integrity validation failed: {failed_references}"
)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Audit publication

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_RUN_AUDIT_TABLE}
(
    run_id                  STRING    NOT NULL,
    dataset                 STRING    NOT NULL,
    bronze_table            STRING    NOT NULL,
    silver_table            STRING    NOT NULL,
    bronze_row_count        BIGINT    NOT NULL,
    superseded_row_count    BIGINT    NOT NULL,
    quarantine_row_count    BIGINT    NOT NULL,
    silver_row_count        BIGINT    NOT NULL,
    distinct_silver_keys    BIGINT    NOT NULL,
    missing_silver_keys     BIGINT    NOT NULL,
    reconciled_row_count    BIGINT    NOT NULL,
    validation_status       STRING    NOT NULL,
    completed_at            TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Audit results for Marketplace Silver processing'
TBLPROPERTIES ('quality' = 'ops')
""")

silver_validation_df.createOrReplaceTempView("incoming_silver_run_audit")

spark.sql(f"""
MERGE INTO {SILVER_RUN_AUDIT_TABLE} AS target
USING incoming_silver_run_audit AS source
  ON target.run_id = source.run_id
 AND target.dataset = source.dataset
WHEN MATCHED THEN UPDATE SET
    target.bronze_table = source.bronze_table,
    target.silver_table = source.silver_table,
    target.bronze_row_count = source.bronze_row_count,
    target.superseded_row_count = source.superseded_row_count,
    target.quarantine_row_count = source.quarantine_row_count,
    target.silver_row_count = source.silver_row_count,
    target.distinct_silver_keys = source.distinct_silver_keys,
    target.missing_silver_keys = source.missing_silver_keys,
    target.reconciled_row_count = source.reconciled_row_count,
    target.validation_status = source.validation_status,
    target.completed_at = source.completed_at
WHEN NOT MATCHED THEN INSERT *
""")

completed = datetime.now(timezone.utc).replace(tzinfo=None)
task_audit_schema = T.StructType([
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

total_bronze = sum(row["bronze_row_count"] for row in validation_results)
total_silver = sum(row["silver_row_count"] for row in validation_results)
total_quarantine = sum(row["quarantine_row_count"] for row in validation_results)
total_superseded = sum(row["superseded_row_count"] for row in validation_results)

task_audit_df = spark.createDataFrame([(
    RUN_ID,
    "silver_validate",
    TASK_RUN_ID,
    REPAIR_COUNT,
    None,
    "SUCCEEDED",
    TASK_STARTED_AT,
    completed,
    total_bronze,
    total_silver,
    total_quarantine,
    None,
    None,
    None,
    {
        "validated_datasets": str(len(validation_results)),
        "superseded_rows": str(total_superseded),
        "order_items_rows": str(order_items_count),
        "referential_checks": str(len(referential_results)),
    },
    TASK_STARTED_AT,
    completed,
)], task_audit_schema)
task_audit_df.createOrReplaceTempView("silver_validate_task_audit")

spark.sql(f"""
MERGE INTO {TASK_AUDIT_TABLE} AS target
USING silver_validate_task_audit AS source
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

print("Silver consolidated validation passed")
print(f"Run ID:                {RUN_ID}")
print(f"Bronze rows:           {total_bronze}")
print(f"Silver entity rows:    {total_silver}")
print(f"Order item rows:       {order_items_count}")
print(f"Superseded rows:       {total_superseded}")
print(f"Quarantine rows:       {total_quarantine}")
print("Referential integrity: PASSED")

display(silver_validation_df.orderBy("dataset"))
display(spark.createDataFrame([referential_results]))
display(task_audit_df)
