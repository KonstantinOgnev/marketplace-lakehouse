# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — idempotency test: rerun with the same parameters
# MAGIC
# MAGIC Run this notebook with `action=baseline`, rerun the Lakeflow Job with
# MAGIC exactly the same parameters and no new source files, then run this
# MAGIC notebook again with `action=validate` and the same `test_id`.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F, types as T

CATALOG = "marketplace_lab"
OPS_SCHEMA = "ops"
SNAPSHOT_TABLE = f"{CATALOG}.{OPS_SCHEMA}.idempotency_test_snapshots"

dbutils.widgets.dropdown("action", "baseline", ["baseline", "validate"], "Action")
dbutils.widgets.text("test_id", "same_parameters_rerun_01", "Test ID")

action = dbutils.widgets.get("action").strip().lower()
test_id = dbutils.widgets.get("test_id").strip()

assert action in {"baseline", "validate"}
assert test_id, "test_id must not be empty"

print(f"Action: {action}")
print(f"Test ID: {test_id}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
      test_id STRING NOT NULL,
      phase STRING NOT NULL,
      captured_at TIMESTAMP NOT NULL,
      layer STRING NOT NULL,
      table_name STRING NOT NULL,
      row_count BIGINT NOT NULL,
      distinct_business_key_count BIGINT,
      business_key STRING,
      duplicate_business_key_count BIGINT,
      null_business_key_count BIGINT
    ) USING DELTA
    """
)

# Business keys used for the duplicate checks. Other tables are still checked
# for stable row counts.
business_keys = {
    "bronze.user_events_raw": "event_id",
    "bronze.orders_raw": "order_id",
    "bronze.payments_raw": "payment_id",
    "bronze.products_raw": "product_id",
    "bronze.sellers_raw": "seller_id",
    "silver.user_events": "event_id",
    "silver.orders": "order_id",
    "silver.payments": "payment_id",
    "silver.products": "product_id",
    "silver.sellers": "seller_id",
    "gold.dim_user": "user_id",
    "gold.dim_product": "product_id",
    "gold.dim_seller": "seller_id",
    "gold.fact_events": "event_id",
    "gold.fact_orders": "order_id",
    "gold.fact_payments": "payment_id",
}

# COMMAND ----------

table_inventory_df = spark.sql(
    f"""
    SELECT LOWER(table_schema) AS layer, LOWER(table_name) AS table_name
    FROM {CATALOG}.information_schema.tables
    WHERE LOWER(table_schema) IN ('bronze', 'silver', 'gold')
      AND table_type = 'MANAGED'
    ORDER BY layer, table_name
    """
)

# Some runtimes expose Delta tables as BASE TABLE rather than MANAGED.
if table_inventory_df.limit(1).count() == 0:
    table_inventory_df = spark.sql(
        f"""
        SELECT LOWER(table_schema) AS layer, LOWER(table_name) AS table_name
        FROM {CATALOG}.information_schema.tables
        WHERE LOWER(table_schema) IN ('bronze', 'silver', 'gold')
          AND table_type = 'BASE TABLE'
        ORDER BY layer, table_name
        """
    )

inventory = [(r.layer, r.table_name) for r in table_inventory_df.collect()]
assert inventory, "No Bronze/Silver/Gold base tables were found"
print(f"Tables included: {len(inventory)}")

# COMMAND ----------

snapshot_rows = []
captured_at = datetime.now(timezone.utc).replace(tzinfo=None)

for layer, table_name in inventory:
    full_name = f"{CATALOG}.{layer}.{table_name}"
    key_name = business_keys.get(f"{layer}.{table_name}")

    if key_name and key_name in spark.table(full_name).columns:
        stats = (
            spark.table(full_name)
            .agg(
                F.count(F.lit(1)).cast("long").alias("row_count"),
                F.countDistinct(F.col(key_name)).cast("long").alias("distinct_key_count"),
                F.sum(F.when(F.col(key_name).isNull(), 1).otherwise(0)).cast("long").alias("null_key_count"),
            )
            .first()
        )
        duplicate_count = max(
            0,
            stats.row_count - stats.distinct_key_count - (stats.null_key_count or 0),
        )
        distinct_count = stats.distinct_key_count
        null_count = stats.null_key_count or 0
    else:
        stats = spark.table(full_name).agg(F.count(F.lit(1)).cast("long").alias("row_count")).first()
        distinct_count = None
        duplicate_count = None
        null_count = None

    snapshot_rows.append(
        (
            test_id,
            action,
            captured_at,
            layer,
            table_name,
            stats.row_count,
            distinct_count,
            key_name,
            duplicate_count,
            null_count,
        )
    )

snapshot_schema = T.StructType([
    T.StructField("test_id", T.StringType(), False),
    T.StructField("phase", T.StringType(), False),
    T.StructField("captured_at", T.TimestampType(), False),
    T.StructField("layer", T.StringType(), False),
    T.StructField("table_name", T.StringType(), False),
    T.StructField("row_count", T.LongType(), False),
    T.StructField("distinct_business_key_count", T.LongType(), True),
    T.StructField("business_key", T.StringType(), True),
    T.StructField("duplicate_business_key_count", T.LongType(), True),
    T.StructField("null_business_key_count", T.LongType(), True),
])

snapshot_df = spark.createDataFrame(snapshot_rows, snapshot_schema)

# Makes each phase repeatable: replace only the snapshot for this test/phase.
spark.sql(
    f"DELETE FROM {SNAPSHOT_TABLE} "
    f"WHERE test_id = '{test_id.replace(chr(39), chr(39) * 2)}' AND phase = '{action}'"
)
snapshot_df.write.mode("append").saveAsTable(SNAPSHOT_TABLE)

print(f"Snapshot written: {action}")
display(snapshot_df.orderBy("layer", "table_name"))

# COMMAND ----------

if action == "baseline":
    print("BASELINE READY")
    print("Now rerun the Lakeflow Job with exactly the same parameters and no new files.")
    print("After it succeeds, rerun this notebook with action=validate and the same test_id.")
else:
    baseline_df = (
        spark.table(SNAPSHOT_TABLE)
        .filter((F.col("test_id") == test_id) & (F.col("phase") == "baseline"))
        .select(
            "layer",
            "table_name",
            F.col("row_count").alias("baseline_rows"),
            F.col("duplicate_business_key_count").alias("baseline_duplicates"),
            F.col("null_business_key_count").alias("baseline_null_keys"),
        )
    )
    assert baseline_df.limit(1).count() == 1, (
        f"Baseline not found for test_id={test_id}. Run action=baseline first."
    )

    current_df = snapshot_df.select(
        "layer",
        "table_name",
        F.col("row_count").alias("current_rows"),
        F.col("duplicate_business_key_count").alias("current_duplicates"),
        F.col("null_business_key_count").alias("current_null_keys"),
    )

    comparison_df = (
        baseline_df.join(current_df, ["layer", "table_name"], "full")
        .withColumn("row_delta", F.col("current_rows") - F.col("baseline_rows"))
        .withColumn(
            "validation_status",
            F.when(F.col("baseline_rows").isNull() | F.col("current_rows").isNull(), "FAILED_TABLE_SET_CHANGED")
             .when(F.col("row_delta") != 0, "FAILED_ROW_COUNT_CHANGED")
             .when(
                 F.coalesce(F.col("current_duplicates"), F.lit(0))
                 > F.coalesce(F.col("baseline_duplicates"), F.lit(0)),
                 "FAILED_DUPLICATES_INCREASED",
             )
             .when(
                 F.coalesce(F.col("current_null_keys"), F.lit(0))
                 > F.coalesce(F.col("baseline_null_keys"), F.lit(0)),
                 "FAILED_NULL_KEYS_INCREASED",
             )
             .otherwise("PASSED"),
        )
        .orderBy("layer", "table_name")
    )

    failed_count = comparison_df.filter(F.col("validation_status") != "PASSED").count()
    display(comparison_df)

    print(f"Compared tables: {comparison_df.count()}")
    print(f"Failed checks: {failed_count}")
    assert failed_count == 0, "Idempotency validation failed; inspect the comparison table"
    print("IDEMPOTENCY TEST PASSED: no row counts or business-key quality metrics changed.")
