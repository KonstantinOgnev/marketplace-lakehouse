# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — gold_order_facts

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

TASK_AUDIT_TABLE = "marketplace_lab.ops.pipeline_task_runs"
TASK_KEY = 'gold_order_facts'
TARGET_TABLES = ['marketplace_lab.gold.fact_orders', 'marketplace_lab.gold.fact_order_items']
SOURCE_TABLES = ['marketplace_lab.silver.orders', 'marketplace_lab.silver.order_items']

def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

for name, default in {
    "run_id": "", "task_run_id": "MANUAL", "repair_count": "0"
}.items():
    ensure_widget(name, default)

RUN_ID = (dbutils.widgets.get("run_id") or "").strip()
TASK_RUN_ID = (dbutils.widgets.get("task_run_id") or "MANUAL").strip()
REPAIR_COUNT = int((dbutils.widgets.get("repair_count") or "0").strip())
GOLD_RUN_ID = RUN_ID
TASK_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
assert RUN_ID, "run_id is required"

for table_name in SOURCE_TABLES:
    assert spark.catalog.tableExists(table_name), f"Missing source: {table_name}"

rows_before = {
    table_name: spark.table(table_name).count()
    if spark.catalog.tableExists(table_name) else 0
    for table_name in TARGET_TABLES
}
input_rows = sum(spark.table(table_name).count() for table_name in SOURCE_TABLES)

# COMMAND ----------
# Этап 7. Gold Model
# Ячейка 3. Создание fact_orders и fact_order_items

from pyspark.sql import functions as F


FACT_ORDERS_TABLE = (
    "marketplace_lab.gold.fact_orders"
)

FACT_ORDER_ITEMS_TABLE = (
    "marketplace_lab.gold.fact_order_items"
)

DIM_USER_TABLE = (
    "marketplace_lab.gold.dim_user"
)

DIM_SELLER_TABLE = (
    "marketplace_lab.gold.dim_seller"
)

DIM_PRODUCT_TABLE = (
    "marketplace_lab.gold.dim_product"
)


RECOGNIZED_GMV_STATUSES = [
    "PAID",
    "SHIPPED",
    "DELIVERED"
]


# =========================================================
# 1. FACT_ORDERS
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_ORDERS_TABLE}
(
    order_key               STRING        NOT NULL,
    order_id                STRING        NOT NULL,

    user_key                STRING        NOT NULL,
    user_id                 STRING        NOT NULL,

    seller_key              STRING        NOT NULL,
    seller_id               STRING        NOT NULL,

    order_date              DATE          NOT NULL,
    created_at              TIMESTAMP     NOT NULL,
    updated_at              TIMESTAMP     NOT NULL,

    order_status            STRING        NOT NULL,
    currency                STRING        NOT NULL,

    item_line_count         BIGINT        NOT NULL,
    distinct_product_count  BIGINT        NOT NULL,
    total_quantity          BIGINT        NOT NULL,
    order_amount            DECIMAL(18,2) NOT NULL,

    recognized_gmv_amount   DECIMAL(18,2) NOT NULL,
    cancelled_order_amount  DECIMAL(18,2) NOT NULL,

    is_recognized_gmv       BOOLEAN       NOT NULL,
    is_cancelled            BOOLEAN       NOT NULL,

    sales_channel           STRING,
    coupon_code             STRING,
    delivery_priority       STRING,
    schema_version          INT           NOT NULL,

    record_hash             STRING        NOT NULL,
    gold_created_at         TIMESTAMP     NOT NULL,
    gold_updated_at         TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Gold order fact at one row per order_id'
TBLPROPERTIES
(
    'quality' = 'gold',
    'fact_grain' = 'one row per order_id',
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ---------------------------------------------------------
# 1.1. Агрегаты строк заказа
# ---------------------------------------------------------

order_item_aggregates_df = (
    spark.table(
        "marketplace_lab.silver.order_items"
    )
    .groupBy("order_id")
    .agg(
        F.count("*").alias(
            "item_line_count"
        ),
        F.countDistinct("product_id").alias(
            "distinct_product_count"
        ),
        F.sum("quantity").cast("long").alias(
            "calculated_quantity"
        ),
        F.sum("line_amount").cast(
            "decimal(18,2)"
        ).alias("calculated_order_amount")
    )
)


# ---------------------------------------------------------
# 1.2. Ключи измерений
# ---------------------------------------------------------

user_keys_df = (
    spark.table(DIM_USER_TABLE)
    .select(
        "user_id",
        "user_key"
    )
)

seller_keys_df = (
    spark.table(DIM_SELLER_TABLE)
    .select(
        "seller_id",
        "seller_key"
    )
)


# ---------------------------------------------------------
# 1.3. Источник fact_orders
# ---------------------------------------------------------

fact_orders_source_df = (
    spark.table(
        "marketplace_lab.silver.orders"
    )
    .join(
        user_keys_df,
        "user_id",
        "left"
    )
    .join(
        seller_keys_df,
        "seller_id",
        "left"
    )
    .join(
        order_item_aggregates_df,
        "order_id",
        "left"
    )
    .withColumn(
        "order_key",
        F.sha2(
            F.concat(
                F.lit("ORDER|"),
                F.col("order_id")
            ),
            256
        )
    )
    .withColumn(
        "order_date",
        F.to_date("created_at")
    )
    .withColumn(
        "is_recognized_gmv",
        F.col("order_status").isin(
            RECOGNIZED_GMV_STATUSES
        )
    )
    .withColumn(
        "is_cancelled",
        F.col("order_status") == "CANCELLED"
    )
    .withColumn(
        "recognized_gmv_amount",
        F.when(
            F.col("is_recognized_gmv"),
            F.col("order_amount")
        )
        .otherwise(
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "cancelled_order_amount",
        F.when(
            F.col("is_cancelled"),
            F.col("order_amount")
        )
        .otherwise(
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("order_id"),
                F.col("user_key"),
                F.col("seller_key"),
                F.col("created_at").cast("string"),
                F.col("updated_at").cast("string"),
                F.col("order_status"),
                F.col("currency"),
                F.col("item_line_count").cast("string"),
                F.col(
                    "distinct_product_count"
                ).cast("string"),
                F.col("total_quantity").cast("string"),
                F.col("order_amount").cast("string"),
                F.coalesce(
                    F.col("sales_channel"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("coupon_code"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("delivery_priority"),
                    F.lit("<NULL>")
                ),
                F.col("schema_version").cast("string")
            ),
            256
        )
    )
    .select(
        "order_key",
        "order_id",
        "user_key",
        "user_id",
        "seller_key",
        "seller_id",
        "order_date",
        "created_at",
        "updated_at",
        "order_status",
        "currency",
        "item_line_count",
        "distinct_product_count",
        "total_quantity",
        "order_amount",
        "recognized_gmv_amount",
        "cancelled_order_amount",
        "is_recognized_gmv",
        "is_cancelled",
        "sales_channel",
        "coupon_code",
        "delivery_priority",
        "schema_version",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)


# ---------------------------------------------------------
# 1.4. Проверка перед MERGE
# ---------------------------------------------------------

invalid_order_dimension_keys = (
    fact_orders_source_df
    .filter(
        F.col("user_key").isNull()
        | F.col("seller_key").isNull()
    )
    .count()
)

quantity_mismatches = (
    fact_orders_source_df
    .filter(
        F.col("total_quantity")
        != F.col("calculated_quantity")
    )
    .count()
)

amount_mismatches = (
    fact_orders_source_df
    .filter(
        F.abs(
            F.col("order_amount")
            - F.col("calculated_order_amount")
        ) > F.lit(0.01)
    )
    .count()
)

assert invalid_order_dimension_keys == 0
assert quantity_mismatches == 0
assert amount_mismatches == 0


fact_orders_source_df.createOrReplaceTempView(
    "incoming_fact_orders"
)

spark.sql(f"""
MERGE INTO {FACT_ORDERS_TABLE} AS target
USING incoming_fact_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.order_key =
        source.order_key,
    target.user_key =
        source.user_key,
    target.user_id =
        source.user_id,
    target.seller_key =
        source.seller_key,
    target.seller_id =
        source.seller_id,
    target.order_date =
        source.order_date,
    target.created_at =
        source.created_at,
    target.updated_at =
        source.updated_at,
    target.order_status =
        source.order_status,
    target.currency =
        source.currency,
    target.item_line_count =
        source.item_line_count,
    target.distinct_product_count =
        source.distinct_product_count,
    target.total_quantity =
        source.total_quantity,
    target.order_amount =
        source.order_amount,
    target.recognized_gmv_amount =
        source.recognized_gmv_amount,
    target.cancelled_order_amount =
        source.cancelled_order_amount,
    target.is_recognized_gmv =
        source.is_recognized_gmv,
    target.is_cancelled =
        source.is_cancelled,
    target.sales_channel =
        source.sales_channel,
    target.coupon_code =
        source.coupon_code,
    target.delivery_priority =
        source.delivery_priority,
    target.schema_version =
        source.schema_version,
    target.record_hash =
        source.record_hash,
    target.gold_updated_at =
        current_timestamp()

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 2. FACT_ORDER_ITEMS
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_ORDER_ITEMS_TABLE}
(
    order_item_key          STRING        NOT NULL,
    order_item_id           STRING        NOT NULL,

    order_key               STRING        NOT NULL,
    order_id                STRING        NOT NULL,

    user_key                STRING        NOT NULL,
    seller_key              STRING        NOT NULL,
    product_key             STRING        NOT NULL,

    user_id                 STRING        NOT NULL,
    seller_id               STRING        NOT NULL,
    product_id              STRING        NOT NULL,

    order_date              DATE          NOT NULL,
    order_status            STRING        NOT NULL,
    currency                STRING        NOT NULL,
    line_number             INT           NOT NULL,

    quantity                BIGINT        NOT NULL,
    unit_price              DECIMAL(18,2) NOT NULL,
    line_amount             DECIMAL(18,2) NOT NULL,

    recognized_gmv_amount   DECIMAL(18,2) NOT NULL,
    cancelled_line_amount   DECIMAL(18,2) NOT NULL,

    record_hash             STRING        NOT NULL,
    gold_created_at         TIMESTAMP     NOT NULL,
    gold_updated_at         TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Gold order-item fact at one row per order_item_id'
TBLPROPERTIES
(
    'quality' = 'gold',
    'fact_grain' = 'one row per order_item_id',
    'delta.enableChangeDataFeed' = 'true'
)
""")


product_keys_df = (
    spark.table(DIM_PRODUCT_TABLE)
    .select(
        "product_id",
        "product_key"
    )
)


order_context_df = (
    spark.table(FACT_ORDERS_TABLE)
    .select(
        "order_id",
        "order_key",
        "user_key",
        "seller_key",
        "user_id",
        "seller_id",
        "order_date",
        "order_status",
        "currency",
        "is_recognized_gmv",
        "is_cancelled"
    )
)


fact_order_items_source_df = (
    spark.table(
        "marketplace_lab.silver.order_items"
    )
    .drop("currency")
    .join(
        order_context_df,
        "order_id",
        "left"
    )
    .join(
        product_keys_df,
        "product_id",
        "left"
    )
    .withColumn(
        "order_item_key",
        F.sha2(
            F.concat(
                F.lit("ORDER_ITEM|"),
                F.col("order_item_id")
            ),
            256
        )
    )
    .withColumn(
        "recognized_gmv_amount",
        F.when(
            F.col("is_recognized_gmv"),
            F.col("line_amount")
        )
        .otherwise(
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "cancelled_line_amount",
        F.when(
            F.col("is_cancelled"),
            F.col("line_amount")
        )
        .otherwise(
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("order_item_id"),
                F.col("order_key"),
                F.col("user_key"),
                F.col("seller_key"),
                F.col("product_key"),
                F.col("order_date").cast("string"),
                F.col("order_status"),
                F.col("currency"),
                F.col("line_number").cast("string"),
                F.col("quantity").cast("string"),
                F.col("unit_price").cast("string"),
                F.col("line_amount").cast("string")
            ),
            256
        )
    )
    .select(
        "order_item_key",
        "order_item_id",
        "order_key",
        "order_id",
        "user_key",
        "seller_key",
        "product_key",
        "user_id",
        "seller_id",
        "product_id",
        "order_date",
        "order_status",
        "currency",
        "line_number",
        "quantity",
        "unit_price",
        "line_amount",
        "recognized_gmv_amount",
        "cancelled_line_amount",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)


invalid_item_dimension_keys = (
    fact_order_items_source_df
    .filter(
        F.col("order_key").isNull()
        | F.col("user_key").isNull()
        | F.col("seller_key").isNull()
        | F.col("product_key").isNull()
    )
    .count()
)

assert invalid_item_dimension_keys == 0


fact_order_items_source_df.createOrReplaceTempView(
    "incoming_fact_order_items"
)

spark.sql(f"""
MERGE INTO {FACT_ORDER_ITEMS_TABLE} AS target
USING incoming_fact_order_items AS source
ON target.order_item_id = source.order_item_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.order_item_key =
        source.order_item_key,
    target.order_key =
        source.order_key,
    target.order_id =
        source.order_id,
    target.user_key =
        source.user_key,
    target.seller_key =
        source.seller_key,
    target.product_key =
        source.product_key,
    target.user_id =
        source.user_id,
    target.seller_id =
        source.seller_id,
    target.product_id =
        source.product_id,
    target.order_date =
        source.order_date,
    target.order_status =
        source.order_status,
    target.currency =
        source.currency,
    target.line_number =
        source.line_number,
    target.quantity =
        source.quantity,
    target.unit_price =
        source.unit_price,
    target.line_amount =
        source.line_amount,
    target.recognized_gmv_amount =
        source.recognized_gmv_amount,
    target.cancelled_line_amount =
        source.cancelled_line_amount,
    target.record_hash =
        source.record_hash,
    target.gold_updated_at =
        current_timestamp()

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 3. Проверка фактов
# =========================================================

fact_orders_df = spark.table(
    FACT_ORDERS_TABLE
)

fact_order_items_df = spark.table(
    FACT_ORDER_ITEMS_TABLE
)

fact_order_count = fact_orders_df.count()
fact_item_count = fact_order_items_df.count()

distinct_order_keys = (
    fact_orders_df
    .select("order_key")
    .distinct()
    .count()
)

distinct_item_keys = (
    fact_order_items_df
    .select("order_item_key")
    .distinct()
    .count()
)

silver_order_amount = (
    spark.table("marketplace_lab.silver.orders")
    .agg(
        F.sum("order_amount").alias("amount")
    )
    .first()["amount"]
)

fact_order_amount = (
    fact_orders_df
    .agg(
        F.sum("order_amount").alias("amount")
    )
    .first()["amount"]
)

fact_item_amount = (
    fact_order_items_df
    .agg(
        F.sum("line_amount").alias("amount")
    )
    .first()["amount"]
)

assert distinct_order_keys == fact_order_count
assert distinct_item_keys == fact_item_count
assert silver_order_amount == fact_order_amount
assert fact_order_amount == fact_item_amount


print("Gold order facts completed")
print(f"fact_orders rows:              {fact_order_count}")
print(f"fact_order_items rows:         {fact_item_count}")
print(f"Invalid order dimension keys:  {invalid_order_dimension_keys}")
print(f"Invalid item dimension keys:   {invalid_item_dimension_keys}")
print(f"Quantity mismatches:           {quantity_mismatches}")
print(f"Amount mismatches:             {amount_mismatches}")
print(f"Total order amount:            {fact_order_amount}")
print("Reconciliation status:         PASSED")

display(
    fact_orders_df
    .groupBy(
        "order_status",
        "currency"
    )
    .agg(
        F.count("*").alias("order_count"),
        F.sum("order_amount").alias(
            "gross_order_value"
        ),
        F.sum("recognized_gmv_amount").alias(
            "recognized_gmv"
        ),
        F.sum("cancelled_order_amount").alias(
            "cancelled_value"
        )
    )
    .orderBy(
        "order_status",
        "currency"
    )
)

display(
    fact_order_items_df
    .join(
        spark.table(DIM_PRODUCT_TABLE)
        .select(
            "product_key",
            "category"
        ),
        "product_key"
    )
    .groupBy("category")
    .agg(
        F.sum("quantity").alias(
            "units_ordered"
        ),
        F.sum("recognized_gmv_amount").alias(
            "recognized_gmv"
        )
    )
    .orderBy(
        F.desc("recognized_gmv")
    )
)

# COMMAND ----------

rows_after = {table_name: spark.table(table_name).count() for table_name in TARGET_TABLES}
rows_added = sum(max(rows_after[t] - rows_before[t], 0) for t in TARGET_TABLES)
completed = datetime.now(timezone.utc).replace(tzinfo=None)

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

metrics = {}
for table_name in TARGET_TABLES:
    metrics[f"{table_name}.rows_before"] = str(rows_before[table_name])
    metrics[f"{table_name}.rows_after"] = str(rows_after[table_name])

audit_df = spark.createDataFrame([(
    RUN_ID, TASK_KEY, TASK_RUN_ID, REPAIR_COUNT, None, "SUCCEEDED",
    TASK_STARTED_AT, completed, input_rows, rows_added, 0, None,
    None, None, metrics, TASK_STARTED_AT, completed,
)], audit_schema)
audit_df.createOrReplaceTempView("gold_task_audit_source")

spark.sql(f"""
MERGE INTO {TASK_AUDIT_TABLE} AS target
USING gold_task_audit_source AS source
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

print(f"Gold task completed: {TASK_KEY}")
display(audit_df)
