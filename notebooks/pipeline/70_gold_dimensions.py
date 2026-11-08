# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — gold_dimensions

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

TASK_AUDIT_TABLE = "marketplace_lab.ops.pipeline_task_runs"
TASK_KEY = 'gold_dimensions'
TARGET_TABLES = ['marketplace_lab.gold.dim_seller', 'marketplace_lab.gold.dim_product', 'marketplace_lab.gold.dim_user']
SOURCE_TABLES = ['marketplace_lab.silver.sellers', 'marketplace_lab.silver.products', 'marketplace_lab.silver.orders', 'marketplace_lab.silver.user_events']

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
# Ячейка 2. Создание dim_seller, dim_product, dim_user

from pyspark.sql import functions as F
from pyspark.sql.window import Window


DIM_SELLER_TABLE = "marketplace_lab.gold.dim_seller"
DIM_PRODUCT_TABLE = "marketplace_lab.gold.dim_product"
DIM_USER_TABLE = "marketplace_lab.gold.dim_user"


# =========================================================
# 1. DIM_SELLER
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_SELLER_TABLE}
(
    seller_key          STRING    NOT NULL,
    seller_id           STRING    NOT NULL,

    country_code        STRING    NOT NULL,
    segment             STRING    NOT NULL,
    registration_date   DATE      NOT NULL,
    seller_status       STRING    NOT NULL,

    record_hash         STRING    NOT NULL,
    gold_created_at     TIMESTAMP NOT NULL,
    gold_updated_at     TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Current Gold seller dimension'
TBLPROPERTIES
(
    'quality' = 'gold',
    'dimension_type' = 'SCD Type 1',
    'delta.enableChangeDataFeed' = 'true'
)
""")


dim_seller_source_df = (
    spark.table("marketplace_lab.silver.sellers")
    .withColumn(
        "seller_key",
        F.sha2(
            F.concat(
                F.lit("SELLER|"),
                F.col("seller_id")
            ),
            256
        )
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("seller_id"),
                F.col("country_code"),
                F.col("segment"),
                F.col(
                    "registration_date"
                ).cast("string"),
                F.col("seller_status")
            ),
            256
        )
    )
    .select(
        "seller_key",
        "seller_id",
        "country_code",
        "segment",
        "registration_date",
        "seller_status",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)

dim_seller_source_df.createOrReplaceTempView(
    "incoming_dim_seller"
)

spark.sql(f"""
MERGE INTO {DIM_SELLER_TABLE} AS target
USING incoming_dim_seller AS source
ON target.seller_id = source.seller_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.seller_key =
        source.seller_key,
    target.country_code =
        source.country_code,
    target.segment =
        source.segment,
    target.registration_date =
        source.registration_date,
    target.seller_status =
        source.seller_status,
    target.record_hash =
        source.record_hash,
    target.gold_updated_at =
        current_timestamp()

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 2. DIM_PRODUCT
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_PRODUCT_TABLE}
(
    product_key         STRING        NOT NULL,
    product_id          STRING        NOT NULL,

    seller_key          STRING        NOT NULL,
    seller_id           STRING        NOT NULL,

    category            STRING        NOT NULL,
    product_name        STRING        NOT NULL,
    current_price       DECIMAL(18,2) NOT NULL,
    is_active           BOOLEAN       NOT NULL,

    record_hash         STRING        NOT NULL,
    gold_created_at     TIMESTAMP     NOT NULL,
    gold_updated_at     TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Current Gold product dimension'
TBLPROPERTIES
(
    'quality' = 'gold',
    'dimension_type' = 'SCD Type 1',
    'delta.enableChangeDataFeed' = 'true'
)
""")


seller_keys_df = (
    spark.table(DIM_SELLER_TABLE)
    .select(
        "seller_id",
        "seller_key"
    )
)

dim_product_source_df = (
    spark.table("marketplace_lab.silver.products")
    .join(
        seller_keys_df,
        "seller_id",
        "left"
    )
    .withColumn(
        "product_key",
        F.sha2(
            F.concat(
                F.lit("PRODUCT|"),
                F.col("product_id")
            ),
            256
        )
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("product_id"),
                F.col("seller_id"),
                F.col("seller_key"),
                F.col("category"),
                F.col("product_name"),
                F.col("price").cast("string"),
                F.col("is_active").cast("string")
            ),
            256
        )
    )
    .select(
        "product_key",
        "product_id",
        "seller_key",
        "seller_id",
        "category",
        "product_name",
        F.col("price").alias("current_price"),
        "is_active",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)

missing_product_seller_keys = (
    dim_product_source_df
    .filter(F.col("seller_key").isNull())
    .count()
)

assert missing_product_seller_keys == 0, (
    f"Products without seller_key: "
    f"{missing_product_seller_keys}"
)

dim_product_source_df.createOrReplaceTempView(
    "incoming_dim_product"
)

spark.sql(f"""
MERGE INTO {DIM_PRODUCT_TABLE} AS target
USING incoming_dim_product AS source
ON target.product_id = source.product_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.product_key =
        source.product_key,
    target.seller_key =
        source.seller_key,
    target.seller_id =
        source.seller_id,
    target.category =
        source.category,
    target.product_name =
        source.product_name,
    target.current_price =
        source.current_price,
    target.is_active =
        source.is_active,
    target.record_hash =
        source.record_hash,
    target.gold_updated_at =
        current_timestamp()

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 3. DIM_USER
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DIM_USER_TABLE}
(
    user_key             STRING    NOT NULL,
    user_id              STRING    NOT NULL,

    registered_at        TIMESTAMP,
    first_event_at       TIMESTAMP,
    last_event_at        TIMESTAMP,
    first_order_at       TIMESTAMP,
    last_order_at        TIMESTAMP,

    latest_country_code  STRING,
    latest_device_type   STRING,

    user_origin          STRING    NOT NULL,
    has_events           BOOLEAN   NOT NULL,
    has_orders           BOOLEAN   NOT NULL,

    record_hash          STRING    NOT NULL,
    gold_created_at      TIMESTAMP NOT NULL,
    gold_updated_at      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Current Gold user dimension derived from events and orders'
TBLPROPERTIES
(
    'quality' = 'gold',
    'dimension_type' = 'SCD Type 1',
    'delta.enableChangeDataFeed' = 'true'
)
""")


silver_events_df = spark.table(
    "marketplace_lab.silver.user_events"
)

silver_orders_df = spark.table(
    "marketplace_lab.silver.orders"
)


# ---------------------------------------------------------
# 3.1. Агрегаты событий пользователя
# ---------------------------------------------------------

user_event_activity_df = (
    silver_events_df
    .groupBy("user_id")
    .agg(
        F.min("event_time_utc").alias(
            "first_event_at"
        ),
        F.max("event_time_utc").alias(
            "last_event_at"
        ),
        F.min(
            F.when(
                F.col("event_type")
                == "user_registered",
                F.col("event_time_utc")
            )
        ).alias("registered_at")
    )
    .withColumn(
        "has_events",
        F.lit(True)
    )
)


# ---------------------------------------------------------
# 3.2. Последние доступные country/device
# ---------------------------------------------------------

latest_profile_window = (
    Window
    .partitionBy("user_id")
    .orderBy(
        F.col("event_time_utc").desc(),
        F.col("ingestion_time_utc").desc(),
        F.col("event_id").desc()
    )
)

latest_user_attributes_df = (
    silver_events_df
    .withColumn(
        "_profile_rank",
        F.row_number().over(
            latest_profile_window
        )
    )
    .filter(F.col("_profile_rank") == 1)
    .select(
        "user_id",
        F.col("country_code").alias(
            "latest_country_code"
        ),
        F.col("device_type").alias(
            "latest_device_type"
        )
    )
)


# ---------------------------------------------------------
# 3.3. Агрегаты заказов пользователя
# ---------------------------------------------------------

user_order_activity_df = (
    silver_orders_df
    .groupBy("user_id")
    .agg(
        F.min("created_at").alias(
            "first_order_at"
        ),
        F.max("created_at").alias(
            "last_order_at"
        )
    )
    .withColumn(
        "has_orders",
        F.lit(True)
    )
)


# ---------------------------------------------------------
# 3.4. Полный список пользователей
# ---------------------------------------------------------

all_user_ids_df = (
    silver_events_df
    .select("user_id")
    .union(
        silver_orders_df.select("user_id")
    )
    .distinct()
)


dim_user_source_df = (
    all_user_ids_df
    .join(
        user_event_activity_df,
        "user_id",
        "left"
    )
    .join(
        latest_user_attributes_df,
        "user_id",
        "left"
    )
    .join(
        user_order_activity_df,
        "user_id",
        "left"
    )
    .withColumn(
        "has_events",
        F.coalesce(
            F.col("has_events"),
            F.lit(False)
        )
    )
    .withColumn(
        "has_orders",
        F.coalesce(
            F.col("has_orders"),
            F.lit(False)
        )
    )
    .withColumn(
        "user_origin",
        F.when(
            F.col("registered_at").isNotNull(),
            F.lit("REGISTRATION_EVENT")
        )
        .when(
            F.col("first_event_at").isNotNull(),
            F.lit("INFERRED_FROM_EVENT")
        )
        .otherwise(
            F.lit("INFERRED_FROM_ORDER")
        )
    )
    .withColumn(
        "user_key",
        F.sha2(
            F.concat(
                F.lit("USER|"),
                F.col("user_id")
            ),
            256
        )
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("user_id"),

                F.coalesce(
                    F.col("registered_at").cast("string"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("first_event_at").cast("string"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("last_event_at").cast("string"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("first_order_at").cast("string"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("last_order_at").cast("string"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("latest_country_code"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("latest_device_type"),
                    F.lit("<NULL>")
                ),
                F.col("user_origin"),
                F.col("has_events").cast("string"),
                F.col("has_orders").cast("string")
            ),
            256
        )
    )
    .select(
        "user_key",
        "user_id",
        "registered_at",
        "first_event_at",
        "last_event_at",
        "first_order_at",
        "last_order_at",
        "latest_country_code",
        "latest_device_type",
        "user_origin",
        "has_events",
        "has_orders",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)

dim_user_source_df.createOrReplaceTempView(
    "incoming_dim_user"
)

spark.sql(f"""
MERGE INTO {DIM_USER_TABLE} AS target
USING incoming_dim_user AS source
ON target.user_id = source.user_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.user_key =
        source.user_key,
    target.registered_at =
        source.registered_at,
    target.first_event_at =
        source.first_event_at,
    target.last_event_at =
        source.last_event_at,
    target.first_order_at =
        source.first_order_at,
    target.last_order_at =
        source.last_order_at,
    target.latest_country_code =
        source.latest_country_code,
    target.latest_device_type =
        source.latest_device_type,
    target.user_origin =
        source.user_origin,
    target.has_events =
        source.has_events,
    target.has_orders =
        source.has_orders,
    target.record_hash =
        source.record_hash,
    target.gold_updated_at =
        current_timestamp()

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 4. Проверка измерений
# =========================================================

dimension_config = {
    "dim_seller": {
        "table": DIM_SELLER_TABLE,
        "business_key": "seller_id",
        "surrogate_key": "seller_key",
        "expected_rows": spark.table("marketplace_lab.silver.sellers").count()
    },
    "dim_product": {
        "table": DIM_PRODUCT_TABLE,
        "business_key": "product_id",
        "surrogate_key": "product_key",
        "expected_rows": spark.table("marketplace_lab.silver.products").count()
    },
    "dim_user": {
        "table": DIM_USER_TABLE,
        "business_key": "user_id",
        "surrogate_key": "user_key",
        "expected_rows": None
    }
}

dimension_results = []

for dimension_name, config in dimension_config.items():
    dimension_df = spark.table(
        config["table"]
    )

    row_count = dimension_df.count()

    distinct_business_keys = (
        dimension_df
        .select(config["business_key"])
        .distinct()
        .count()
    )

    distinct_surrogate_keys = (
        dimension_df
        .select(config["surrogate_key"])
        .distinct()
        .count()
    )

    missing_keys = (
        dimension_df
        .filter(
            F.col(config["business_key"]).isNull()
            | F.col(config["surrogate_key"]).isNull()
        )
        .count()
    )

    expected_rows_valid = (
        config["expected_rows"] is None
        or row_count == config["expected_rows"]
    )

    validation_status = (
        "PASSED"
        if (
            row_count == distinct_business_keys
            and row_count == distinct_surrogate_keys
            and missing_keys == 0
            and expected_rows_valid
        )
        else "FAILED"
    )

    dimension_results.append({
        "dimension": dimension_name,
        "row_count": row_count,
        "distinct_business_keys": distinct_business_keys,
        "distinct_surrogate_keys": distinct_surrogate_keys,
        "missing_keys": missing_keys,
        "validation_status": validation_status
    })


failed_dimensions = [
    result["dimension"]
    for result in dimension_results
    if result["validation_status"] != "PASSED"
]

assert len(failed_dimensions) == 0, (
    f"Gold dimension validation failed: "
    f"{failed_dimensions}"
)

assert missing_product_seller_keys == 0


print("Gold dimensions completed")
print(f"Gold run ID:                 {GOLD_RUN_ID}")
print(f"Dimensions created:          3")
print(f"Failed dimensions:           {len(failed_dimensions)}")
print(f"Products without seller_key: {missing_product_seller_keys}")
print("Overall status:              PASSED")

display(
    spark.createDataFrame(dimension_results)
    .orderBy("dimension")
)

print("dim_user origin distribution:")
display(
    spark.table(DIM_USER_TABLE)
    .groupBy(
        "user_origin",
        "has_events",
        "has_orders"
    )
    .count()
    .orderBy(
        "user_origin",
        "has_events",
        "has_orders"
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
