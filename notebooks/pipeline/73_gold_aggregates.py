# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — gold_aggregates

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

TASK_AUDIT_TABLE = "marketplace_lab.ops.pipeline_task_runs"
TASK_KEY = 'gold_aggregates'
TARGET_TABLES = ['marketplace_lab.gold.agg_daily_seller', 'marketplace_lab.gold.agg_daily_marketplace', 'marketplace_lab.gold.agg_conversion_funnel']
SOURCE_TABLES = ['marketplace_lab.gold.fact_orders', 'marketplace_lab.gold.fact_order_items', 'marketplace_lab.gold.fact_payments', 'marketplace_lab.gold.fact_events']

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
# Ячейка 5. Ежедневные агрегаты seller и marketplace

from pyspark.sql import functions as F


AGG_DAILY_SELLER_TABLE = (
    "marketplace_lab.gold.agg_daily_seller"
)

AGG_DAILY_MARKETPLACE_TABLE = (
    "marketplace_lab.gold.agg_daily_marketplace"
)

FACT_ORDERS_TABLE = (
    "marketplace_lab.gold.fact_orders"
)

FACT_PAYMENTS_TABLE = (
    "marketplace_lab.gold.fact_payments"
)

FACT_EVENTS_TABLE = (
    "marketplace_lab.gold.fact_events"
)


# =========================================================
# 1. AGG_DAILY_SELLER
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AGG_DAILY_SELLER_TABLE}
(
    daily_seller_key          STRING        NOT NULL,
    metric_date               DATE          NOT NULL,

    seller_key                STRING        NOT NULL,
    seller_id                 STRING        NOT NULL,
    currency                  STRING        NOT NULL,

    order_count               BIGINT        NOT NULL,
    recognized_order_count    BIGINT        NOT NULL,
    cancelled_order_count     BIGINT        NOT NULL,
    unique_customers          BIGINT        NOT NULL,
    units_ordered             BIGINT        NOT NULL,

    gross_order_value         DECIMAL(18,2) NOT NULL,
    recognized_gmv            DECIMAL(18,2) NOT NULL,
    cancelled_order_value     DECIMAL(18,2) NOT NULL,
    average_order_value       DECIMAL(18,2) NOT NULL,
    cancellation_rate         DECIMAL(18,6) NOT NULL,

    payment_count             BIGINT        NOT NULL,
    successful_payment_count  BIGINT        NOT NULL,
    failed_payment_count      BIGINT        NOT NULL,
    refunded_payment_count    BIGINT        NOT NULL,

    payment_amount            DECIMAL(18,2) NOT NULL,
    successful_payment_amount DECIMAL(18,2) NOT NULL,
    refunded_payment_amount   DECIMAL(18,2) NOT NULL,
    successful_payment_rate   DECIMAL(18,6) NOT NULL,

    record_hash               STRING        NOT NULL,
    gold_created_at           TIMESTAMP     NOT NULL,
    gold_updated_at           TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Daily seller metrics separated by currency'
TBLPROPERTIES
(
    'quality' = 'gold',
    'aggregate_grain' =
        'metric_date + seller_key + currency',
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ---------------------------------------------------------
# 1.1. Заказы по продавцу
# ---------------------------------------------------------

daily_seller_orders_df = (
    spark.table(FACT_ORDERS_TABLE)
    .groupBy(
        F.col("order_date").alias("metric_date"),
        "seller_key",
        "seller_id",
        "currency"
    )
    .agg(
        F.count("*").cast("long").alias(
            "order_count"
        ),

        F.sum(
            F.when(
                F.col("is_recognized_gmv"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "recognized_order_count"
        ),

        F.sum(
            F.when(
                F.col("is_cancelled"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "cancelled_order_count"
        ),

        F.countDistinct("user_key").cast("long").alias(
            "unique_customers"
        ),

        F.sum("total_quantity").cast("long").alias(
            "units_ordered"
        ),

        F.sum("order_amount")
        .cast("decimal(18,2)")
        .alias("gross_order_value"),

        F.sum("recognized_gmv_amount")
        .cast("decimal(18,2)")
        .alias("recognized_gmv"),

        F.sum("cancelled_order_amount")
        .cast("decimal(18,2)")
        .alias("cancelled_order_value")
    )
)


# ---------------------------------------------------------
# 1.2. Платежи по продавцу
# ---------------------------------------------------------

daily_seller_payments_df = (
    spark.table(FACT_PAYMENTS_TABLE)
    .groupBy(
        F.col("payment_date").alias("metric_date"),
        "seller_key",
        "seller_id",
        "currency"
    )
    .agg(
        F.count("*").cast("long").alias(
            "payment_count"
        ),

        F.sum(
            F.when(
                F.col("is_successful"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "successful_payment_count"
        ),

        F.sum(
            F.when(
                F.col("is_failed"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "failed_payment_count"
        ),

        F.sum(
            F.when(
                F.col("is_refunded"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "refunded_payment_count"
        ),

        F.sum("amount")
        .cast("decimal(18,2)")
        .alias("payment_amount"),

        F.sum("successful_amount")
        .cast("decimal(18,2)")
        .alias("successful_payment_amount"),

        F.sum("refunded_amount")
        .cast("decimal(18,2)")
        .alias("refunded_payment_amount")
    )
)


# ---------------------------------------------------------
# 1.3. Объединение заказов и платежей
# ---------------------------------------------------------

daily_seller_source_df = (
    daily_seller_orders_df
    .join(
        daily_seller_payments_df,
        [
            "metric_date",
            "seller_key",
            "seller_id",
            "currency"
        ],
        "full"
    )
)


seller_long_metrics = [
    "order_count",
    "recognized_order_count",
    "cancelled_order_count",
    "unique_customers",
    "units_ordered",
    "payment_count",
    "successful_payment_count",
    "failed_payment_count",
    "refunded_payment_count"
]

seller_decimal_metrics = [
    "gross_order_value",
    "recognized_gmv",
    "cancelled_order_value",
    "payment_amount",
    "successful_payment_amount",
    "refunded_payment_amount"
]

for column_name in seller_long_metrics:
    daily_seller_source_df = (
        daily_seller_source_df
        .withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.lit(0)
            ).cast("long")
        )
    )

for column_name in seller_decimal_metrics:
    daily_seller_source_df = (
        daily_seller_source_df
        .withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.lit(0)
            ).cast("decimal(18,2)")
        )
    )


daily_seller_source_df = (
    daily_seller_source_df
    .withColumn(
        "average_order_value",
        F.when(
            F.col("recognized_order_count") > 0,
            F.col("recognized_gmv")
            / F.col("recognized_order_count")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,2)")
    )
    .withColumn(
        "cancellation_rate",
        F.when(
            F.col("order_count") > 0,
            F.col("cancelled_order_count")
            / F.col("order_count")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "successful_payment_rate",
        F.when(
            F.col("payment_count") > 0,
            F.col("successful_payment_count")
            / F.col("payment_count")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "daily_seller_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("DAILY_SELLER"),
                F.col("metric_date").cast("string"),
                F.col("seller_key"),
                F.col("currency")
            ),
            256
        )
    )
)


seller_hash_columns = (
    seller_long_metrics
    + seller_decimal_metrics
    + [
        "average_order_value",
        "cancellation_rate",
        "successful_payment_rate"
    ]
)

daily_seller_source_df = (
    daily_seller_source_df
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("daily_seller_key"),
                *[
                    F.col(column_name).cast("string")
                    for column_name in seller_hash_columns
                ]
            ),
            256
        )
    )
    .select(
        "daily_seller_key",
        "metric_date",
        "seller_key",
        "seller_id",
        "currency",
        "order_count",
        "recognized_order_count",
        "cancelled_order_count",
        "unique_customers",
        "units_ordered",
        "gross_order_value",
        "recognized_gmv",
        "cancelled_order_value",
        "average_order_value",
        "cancellation_rate",
        "payment_count",
        "successful_payment_count",
        "failed_payment_count",
        "refunded_payment_count",
        "payment_amount",
        "successful_payment_amount",
        "refunded_payment_amount",
        "successful_payment_rate",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)

daily_seller_source_df.createOrReplaceTempView(
    "incoming_agg_daily_seller"
)

spark.sql(f"""
MERGE INTO {AGG_DAILY_SELLER_TABLE} AS target
USING incoming_agg_daily_seller AS source
ON target.daily_seller_key = source.daily_seller_key

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 2. AGG_DAILY_MARKETPLACE
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AGG_DAILY_MARKETPLACE_TABLE}
(
    daily_marketplace_key     STRING        NOT NULL,
    metric_date               DATE          NOT NULL,
    currency                  STRING        NOT NULL,

    order_count               BIGINT        NOT NULL,
    recognized_order_count    BIGINT        NOT NULL,
    cancelled_order_count     BIGINT        NOT NULL,
    unique_customers          BIGINT        NOT NULL,
    units_ordered             BIGINT        NOT NULL,

    gross_order_value         DECIMAL(18,2) NOT NULL,
    recognized_gmv            DECIMAL(18,2) NOT NULL,
    cancelled_order_value     DECIMAL(18,2) NOT NULL,
    average_order_value       DECIMAL(18,2) NOT NULL,
    cancellation_rate         DECIMAL(18,6) NOT NULL,

    payment_count             BIGINT        NOT NULL,
    successful_payment_count  BIGINT        NOT NULL,
    failed_payment_count      BIGINT        NOT NULL,
    refunded_payment_count    BIGINT        NOT NULL,

    payment_amount            DECIMAL(18,2) NOT NULL,
    successful_payment_amount DECIMAL(18,2) NOT NULL,
    refunded_payment_amount   DECIMAL(18,2) NOT NULL,
    successful_payment_rate   DECIMAL(18,6) NOT NULL,

    event_count               BIGINT        NOT NULL,
    active_users              BIGINT        NOT NULL,
    active_sessions           BIGINT        NOT NULL,
    late_event_count          BIGINT        NOT NULL,

    record_hash               STRING        NOT NULL,
    gold_created_at           TIMESTAMP     NOT NULL,
    gold_updated_at           TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Daily Marketplace metrics separated by currency'
TBLPROPERTIES
(
    'quality' = 'gold',
    'aggregate_grain' = 'metric_date + currency',
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ---------------------------------------------------------
# 2.1. Marketplace orders
# ---------------------------------------------------------

daily_market_orders_df = (
    spark.table(FACT_ORDERS_TABLE)
    .groupBy(
        F.col("order_date").alias("metric_date"),
        "currency"
    )
    .agg(
        F.count("*").cast("long").alias(
            "order_count"
        ),
        F.sum(
            F.when(
                F.col("is_recognized_gmv"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "recognized_order_count"
        ),
        F.sum(
            F.when(
                F.col("is_cancelled"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "cancelled_order_count"
        ),
        F.countDistinct("user_key").cast("long").alias(
            "unique_customers"
        ),
        F.sum("total_quantity").cast("long").alias(
            "units_ordered"
        ),
        F.sum("order_amount")
        .cast("decimal(18,2)")
        .alias("gross_order_value"),
        F.sum("recognized_gmv_amount")
        .cast("decimal(18,2)")
        .alias("recognized_gmv"),
        F.sum("cancelled_order_amount")
        .cast("decimal(18,2)")
        .alias("cancelled_order_value")
    )
)


# ---------------------------------------------------------
# 2.2. Marketplace payments
# ---------------------------------------------------------

daily_market_payments_df = (
    spark.table(FACT_PAYMENTS_TABLE)
    .groupBy(
        F.col("payment_date").alias("metric_date"),
        "currency"
    )
    .agg(
        F.count("*").cast("long").alias(
            "payment_count"
        ),
        F.sum(
            F.when(
                F.col("is_successful"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "successful_payment_count"
        ),
        F.sum(
            F.when(
                F.col("is_failed"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "failed_payment_count"
        ),
        F.sum(
            F.when(
                F.col("is_refunded"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "refunded_payment_count"
        ),
        F.sum("amount")
        .cast("decimal(18,2)")
        .alias("payment_amount"),
        F.sum("successful_amount")
        .cast("decimal(18,2)")
        .alias("successful_payment_amount"),
        F.sum("refunded_amount")
        .cast("decimal(18,2)")
        .alias("refunded_payment_amount")
    )
)


# ---------------------------------------------------------
# 2.3. Marketplace events
#
# События без валюты сохраняются в отдельной группе N/A.
# ---------------------------------------------------------

daily_market_events_df = (
    spark.table(FACT_EVENTS_TABLE)
    .withColumn(
        "currency",
        F.coalesce(
            F.col("currency"),
            F.lit("N/A")
        )
    )
    .groupBy(
        F.col("event_date").alias("metric_date"),
        "currency"
    )
    .agg(
        F.count("*").cast("long").alias(
            "event_count"
        ),
        F.countDistinct("user_key").cast("long").alias(
            "active_users"
        ),
        F.countDistinct("session_id").cast("long").alias(
            "active_sessions"
        ),
        F.sum(
            F.when(
                F.col("is_late"),
                1
            ).otherwise(0)
        ).cast("long").alias(
            "late_event_count"
        )
    )
)


# ---------------------------------------------------------
# 2.4. Объединение Marketplace-метрик
# ---------------------------------------------------------

daily_marketplace_source_df = (
    daily_market_orders_df
    .join(
        daily_market_payments_df,
        ["metric_date", "currency"],
        "full"
    )
    .join(
        daily_market_events_df,
        ["metric_date", "currency"],
        "full"
    )
)


market_long_metrics = [
    "order_count",
    "recognized_order_count",
    "cancelled_order_count",
    "unique_customers",
    "units_ordered",
    "payment_count",
    "successful_payment_count",
    "failed_payment_count",
    "refunded_payment_count",
    "event_count",
    "active_users",
    "active_sessions",
    "late_event_count"
]

market_decimal_metrics = [
    "gross_order_value",
    "recognized_gmv",
    "cancelled_order_value",
    "payment_amount",
    "successful_payment_amount",
    "refunded_payment_amount"
]

for column_name in market_long_metrics:
    daily_marketplace_source_df = (
        daily_marketplace_source_df
        .withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.lit(0)
            ).cast("long")
        )
    )

for column_name in market_decimal_metrics:
    daily_marketplace_source_df = (
        daily_marketplace_source_df
        .withColumn(
            column_name,
            F.coalesce(
                F.col(column_name),
                F.lit(0)
            ).cast("decimal(18,2)")
        )
    )


daily_marketplace_source_df = (
    daily_marketplace_source_df
    .withColumn(
        "average_order_value",
        F.when(
            F.col("recognized_order_count") > 0,
            F.col("recognized_gmv")
            / F.col("recognized_order_count")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,2)")
    )
    .withColumn(
        "cancellation_rate",
        F.when(
            F.col("order_count") > 0,
            F.col("cancelled_order_count")
            / F.col("order_count")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "successful_payment_rate",
        F.when(
            F.col("payment_count") > 0,
            F.col("successful_payment_count")
            / F.col("payment_count")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "daily_marketplace_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("DAILY_MARKETPLACE"),
                F.col("metric_date").cast("string"),
                F.col("currency")
            ),
            256
        )
    )
)


market_hash_columns = (
    market_long_metrics
    + market_decimal_metrics
    + [
        "average_order_value",
        "cancellation_rate",
        "successful_payment_rate"
    ]
)

daily_marketplace_source_df = (
    daily_marketplace_source_df
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("daily_marketplace_key"),
                *[
                    F.col(column_name).cast("string")
                    for column_name in market_hash_columns
                ]
            ),
            256
        )
    )
    .select(
        "daily_marketplace_key",
        "metric_date",
        "currency",
        "order_count",
        "recognized_order_count",
        "cancelled_order_count",
        "unique_customers",
        "units_ordered",
        "gross_order_value",
        "recognized_gmv",
        "cancelled_order_value",
        "average_order_value",
        "cancellation_rate",
        "payment_count",
        "successful_payment_count",
        "failed_payment_count",
        "refunded_payment_count",
        "payment_amount",
        "successful_payment_amount",
        "refunded_payment_amount",
        "successful_payment_rate",
        "event_count",
        "active_users",
        "active_sessions",
        "late_event_count",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)

daily_marketplace_source_df.createOrReplaceTempView(
    "incoming_agg_daily_marketplace"
)

spark.sql(f"""
MERGE INTO {AGG_DAILY_MARKETPLACE_TABLE} AS target
USING incoming_agg_daily_marketplace AS source
ON target.daily_marketplace_key =
   source.daily_marketplace_key

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 3. Reconciliation
# =========================================================

agg_seller_df = spark.table(
    AGG_DAILY_SELLER_TABLE
)

agg_marketplace_df = spark.table(
    AGG_DAILY_MARKETPLACE_TABLE
)

fact_orders_df = spark.table(
    FACT_ORDERS_TABLE
)

fact_payments_df = spark.table(
    FACT_PAYMENTS_TABLE
)

seller_order_count = (
    agg_seller_df
    .agg(F.sum("order_count").alias("value"))
    .first()["value"]
)

seller_payment_count = (
    agg_seller_df
    .agg(F.sum("payment_count").alias("value"))
    .first()["value"]
)

seller_recognized_gmv = (
    agg_seller_df
    .agg(F.sum("recognized_gmv").alias("value"))
    .first()["value"]
)

market_order_count = (
    agg_marketplace_df
    .agg(F.sum("order_count").alias("value"))
    .first()["value"]
)

market_payment_count = (
    agg_marketplace_df
    .agg(F.sum("payment_count").alias("value"))
    .first()["value"]
)

market_recognized_gmv = (
    agg_marketplace_df
    .agg(F.sum("recognized_gmv").alias("value"))
    .first()["value"]
)

fact_order_count = fact_orders_df.count()
fact_payment_count = fact_payments_df.count()

fact_recognized_gmv = (
    fact_orders_df
    .agg(
        F.sum("recognized_gmv_amount").alias("value")
    )
    .first()["value"]
)

assert seller_order_count == fact_order_count
assert seller_payment_count == fact_payment_count
assert market_order_count == fact_order_count
assert market_payment_count == fact_payment_count
assert seller_recognized_gmv == fact_recognized_gmv
assert market_recognized_gmv == fact_recognized_gmv


print("Gold daily aggregates completed")
print(f"agg_daily_seller rows:       {agg_seller_df.count()}")
print(f"agg_daily_marketplace rows:  {agg_marketplace_df.count()}")
print(f"Reconciled orders:           {fact_order_count}")
print(f"Reconciled payments:         {fact_payment_count}")
print(f"Reconciled recognized GMV:   {fact_recognized_gmv}")
print("Overall status:              PASSED")

display(
    agg_marketplace_df
    .orderBy(
        F.desc("metric_date"),
        "currency"
    )
)

display(
    agg_seller_df
    .orderBy(
        F.desc("recognized_gmv")
    )
    .limit(25)
)


# Этап 7. Gold Model
# Ячейка 6. Conversion funnel

from pyspark.sql import functions as F
from pyspark.sql.window import Window


AGG_FUNNEL_TABLE = (
    "marketplace_lab.gold.agg_conversion_funnel"
)

FACT_EVENTS_TABLE = (
    "marketplace_lab.gold.fact_events"
)


# ---------------------------------------------------------
# 1. Создание таблицы
# ---------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AGG_FUNNEL_TABLE}
(
    funnel_key                    STRING        NOT NULL,
    metric_date                   DATE          NOT NULL,

    funnel_step                   STRING        NOT NULL,
    funnel_step_order             INT           NOT NULL,

    event_count                   BIGINT        NOT NULL,
    unique_users                  BIGINT        NOT NULL,
    unique_sessions               BIGINT        NOT NULL,

    previous_step_users           BIGINT        NOT NULL,
    previous_step_sessions        BIGINT        NOT NULL,

    user_conversion_from_previous DECIMAL(18,6) NOT NULL,
    session_conversion_from_previous
                                  DECIMAL(18,6) NOT NULL,

    user_conversion_from_view     DECIMAL(18,6) NOT NULL,
    session_conversion_from_view  DECIMAL(18,6) NOT NULL,

    record_hash                   STRING        NOT NULL,
    gold_created_at               TIMESTAMP     NOT NULL,
    gold_updated_at               TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Daily Marketplace conversion funnel'
TBLPROPERTIES
(
    'quality' = 'gold',
    'aggregate_grain' =
        'metric_date + funnel_step',
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ---------------------------------------------------------
# 2. Справочник шагов
# ---------------------------------------------------------

funnel_steps = [
    (1, "PRODUCT_VIEW"),
    (2, "ADD_TO_CART"),
    (3, "ORDER_CREATED"),
    (4, "PAYMENT_SUCCEEDED")
]

funnel_steps_df = spark.createDataFrame(
    funnel_steps,
    [
        "funnel_step_order",
        "funnel_step"
    ]
)


# ---------------------------------------------------------
# 3. Полный календарный диапазон событий
# ---------------------------------------------------------

event_date_range = (
    spark.table(FACT_EVENTS_TABLE)
    .agg(
        F.min("event_date").alias("min_date"),
        F.max("event_date").alias("max_date")
    )
    .first()
)

min_event_date = event_date_range["min_date"]
max_event_date = event_date_range["max_date"]

assert min_event_date is not None
assert max_event_date is not None


funnel_dates_df = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(min_event_date),
                F.lit(max_event_date),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("metric_date")
    )
)


# ---------------------------------------------------------
# 4. Матрица date × funnel step
#
# Благодаря crossJoin каждый день содержит все четыре шага,
# даже если для какого-либо шага нет событий.
# ---------------------------------------------------------

funnel_grid_df = (
    funnel_dates_df
    .crossJoin(
        F.broadcast(funnel_steps_df)
    )
)


# ---------------------------------------------------------
# 5. Фактические показатели шагов
# ---------------------------------------------------------

funnel_event_metrics_df = (
    spark.table(FACT_EVENTS_TABLE)
    .filter(
        F.col("funnel_step_order").between(1, 4)
    )
    .groupBy(
        F.col("event_date").alias("metric_date"),
        "funnel_step_order",
        "funnel_step"
    )
    .agg(
        F.count("*").cast("long").alias(
            "event_count"
        ),
        F.countDistinct("user_key").cast("long").alias(
            "unique_users"
        ),
        F.countDistinct("session_id").cast("long").alias(
            "unique_sessions"
        )
    )
)


# ---------------------------------------------------------
# 6. Добавление нулевых шагов
# ---------------------------------------------------------

funnel_base_df = (
    funnel_grid_df
    .join(
        funnel_event_metrics_df,
        [
            "metric_date",
            "funnel_step_order",
            "funnel_step"
        ],
        "left"
    )
    .fillna({
        "event_count": 0,
        "unique_users": 0,
        "unique_sessions": 0
    })
    .withColumn(
        "event_count",
        F.col("event_count").cast("long")
    )
    .withColumn(
        "unique_users",
        F.col("unique_users").cast("long")
    )
    .withColumn(
        "unique_sessions",
        F.col("unique_sessions").cast("long")
    )
)


# ---------------------------------------------------------
# 7. Расчёт conversion rates
# ---------------------------------------------------------

funnel_window = (
    Window
    .partitionBy("metric_date")
    .orderBy("funnel_step_order")
)

full_funnel_window = (
    Window
    .partitionBy("metric_date")
    .orderBy("funnel_step_order")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )
)


funnel_source_df = (
    funnel_base_df
    .withColumn(
        "previous_step_users_raw",
        F.lag("unique_users").over(
            funnel_window
        )
    )
    .withColumn(
        "previous_step_sessions_raw",
        F.lag("unique_sessions").over(
            funnel_window
        )
    )
    .withColumn(
        "view_users",
        F.first("unique_users").over(
            full_funnel_window
        )
    )
    .withColumn(
        "view_sessions",
        F.first("unique_sessions").over(
            full_funnel_window
        )
    )
    .withColumn(
        "previous_step_users",
        F.when(
            F.col("funnel_step_order") == 1,
            F.col("unique_users")
        )
        .otherwise(
            F.coalesce(
                F.col("previous_step_users_raw"),
                F.lit(0)
            )
        )
        .cast("long")
    )
    .withColumn(
        "previous_step_sessions",
        F.when(
            F.col("funnel_step_order") == 1,
            F.col("unique_sessions")
        )
        .otherwise(
            F.coalesce(
                F.col("previous_step_sessions_raw"),
                F.lit(0)
            )
        )
        .cast("long")
    )
    .withColumn(
        "user_conversion_from_previous",
        F.when(
            F.col("funnel_step_order") == 1,
            F.when(
                F.col("unique_users") > 0,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .when(
            F.col("previous_step_users") > 0,
            F.col("unique_users")
            / F.col("previous_step_users")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "session_conversion_from_previous",
        F.when(
            F.col("funnel_step_order") == 1,
            F.when(
                F.col("unique_sessions") > 0,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .when(
            F.col("previous_step_sessions") > 0,
            F.col("unique_sessions")
            / F.col("previous_step_sessions")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "user_conversion_from_view",
        F.when(
            F.col("view_users") > 0,
            F.col("unique_users")
            / F.col("view_users")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "session_conversion_from_view",
        F.when(
            F.col("view_sessions") > 0,
            F.col("unique_sessions")
            / F.col("view_sessions")
        )
        .otherwise(F.lit(0))
        .cast("decimal(18,6)")
    )
    .withColumn(
        "funnel_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("FUNNEL"),
                F.col("metric_date").cast("string"),
                F.col("funnel_step_order").cast("string")
            ),
            256
        )
    )
)


# ---------------------------------------------------------
# 8. Record hash и финальная структура
# ---------------------------------------------------------

funnel_source_df = (
    funnel_source_df
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("funnel_key"),
                F.col("event_count").cast("string"),
                F.col("unique_users").cast("string"),
                F.col("unique_sessions").cast("string"),
                F.col(
                    "previous_step_users"
                ).cast("string"),
                F.col(
                    "previous_step_sessions"
                ).cast("string"),
                F.col(
                    "user_conversion_from_previous"
                ).cast("string"),
                F.col(
                    "session_conversion_from_previous"
                ).cast("string"),
                F.col(
                    "user_conversion_from_view"
                ).cast("string"),
                F.col(
                    "session_conversion_from_view"
                ).cast("string")
            ),
            256
        )
    )
    .select(
        "funnel_key",
        "metric_date",
        "funnel_step",
        F.col("funnel_step_order").cast("int").alias(
            "funnel_step_order"
        ),
        "event_count",
        "unique_users",
        "unique_sessions",
        "previous_step_users",
        "previous_step_sessions",
        "user_conversion_from_previous",
        "session_conversion_from_previous",
        "user_conversion_from_view",
        "session_conversion_from_view",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)


funnel_source_df.createOrReplaceTempView(
    "incoming_agg_conversion_funnel"
)


# ---------------------------------------------------------
# 9. Идемпотентный MERGE
# ---------------------------------------------------------

spark.sql(f"""
MERGE INTO {AGG_FUNNEL_TABLE} AS target
USING incoming_agg_conversion_funnel AS source
ON target.funnel_key = source.funnel_key

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET *

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# ---------------------------------------------------------
# 10. Проверка результата
# ---------------------------------------------------------

funnel_df = spark.table(
    AGG_FUNNEL_TABLE
)

funnel_date_count = (
    funnel_dates_df.count()
)

expected_funnel_rows = (
    funnel_date_count * len(funnel_steps)
)

actual_funnel_rows = funnel_df.count()

distinct_funnel_keys = (
    funnel_df
    .select("funnel_key")
    .distinct()
    .count()
)

source_funnel_event_count = (
    spark.table(FACT_EVENTS_TABLE)
    .filter(
        F.col("funnel_step_order").between(1, 4)
    )
    .count()
)

aggregate_funnel_event_count = (
    funnel_df
    .agg(
        F.sum("event_count").alias("event_count")
    )
    .first()["event_count"]
)

invalid_first_step_rates = (
    funnel_df
    .filter(
        (F.col("funnel_step_order") == 1)
        & ~F.col(
            "user_conversion_from_previous"
        ).isin(
            F.lit(0).cast("decimal(18,6)"),
            F.lit(1).cast("decimal(18,6)")
        )
    )
    .count()
)

assert actual_funnel_rows == expected_funnel_rows
assert distinct_funnel_keys == actual_funnel_rows
assert aggregate_funnel_event_count == source_funnel_event_count
assert invalid_first_step_rates == 0


print("Gold conversion funnel completed")
print(f"Date range:             {min_event_date} -> {max_event_date}")
print(f"Calendar dates:         {funnel_date_count}")
print(f"Funnel steps per date:  {len(funnel_steps)}")
print(f"Expected funnel rows:   {expected_funnel_rows}")
print(f"Actual funnel rows:     {actual_funnel_rows}")
print(
    f"Reconciled events:      "
    f"{aggregate_funnel_event_count}"
)
print("Overall status:         PASSED")

display(
    funnel_df
    .orderBy(
        F.desc("metric_date"),
        "funnel_step_order"
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
