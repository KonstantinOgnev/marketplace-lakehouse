# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — gold_other_facts

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import types as T

TASK_AUDIT_TABLE = "marketplace_lab.ops.pipeline_task_runs"
TASK_KEY = 'gold_other_facts'
TARGET_TABLES = ['marketplace_lab.gold.fact_payments', 'marketplace_lab.gold.fact_events']
SOURCE_TABLES = ['marketplace_lab.silver.payments', 'marketplace_lab.silver.user_events']

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
# Ячейка 4. Создание fact_payments и fact_events

from pyspark.sql import functions as F


FACT_PAYMENTS_TABLE = (
    "marketplace_lab.gold.fact_payments"
)

FACT_EVENTS_TABLE = (
    "marketplace_lab.gold.fact_events"
)

FACT_ORDERS_TABLE = (
    "marketplace_lab.gold.fact_orders"
)

DIM_USER_TABLE = (
    "marketplace_lab.gold.dim_user"
)

DIM_PRODUCT_TABLE = (
    "marketplace_lab.gold.dim_product"
)


# =========================================================
# 1. FACT_PAYMENTS
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_PAYMENTS_TABLE}
(
    payment_key              STRING        NOT NULL,
    payment_id               STRING        NOT NULL,

    order_key                STRING        NOT NULL,
    order_id                 STRING        NOT NULL,

    user_key                 STRING        NOT NULL,
    seller_key               STRING        NOT NULL,
    user_id                  STRING        NOT NULL,
    seller_id                STRING        NOT NULL,

    payment_date             DATE          NOT NULL,
    payment_time             TIMESTAMP     NOT NULL,

    payment_status           STRING        NOT NULL,
    payment_method           STRING        NOT NULL,
    currency                 STRING        NOT NULL,
    amount                   DECIMAL(18,2) NOT NULL,

    is_successful            BOOLEAN       NOT NULL,
    is_failed                BOOLEAN       NOT NULL,
    is_pending               BOOLEAN       NOT NULL,
    is_refunded              BOOLEAN       NOT NULL,

    successful_amount        DECIMAL(18,2) NOT NULL,
    failed_amount            DECIMAL(18,2) NOT NULL,
    refunded_amount          DECIMAL(18,2) NOT NULL,

    was_recovered            BOOLEAN       NOT NULL,

    record_hash              STRING        NOT NULL,
    gold_created_at          TIMESTAMP     NOT NULL,
    gold_updated_at          TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Gold payment fact at one row per payment_id'
TBLPROPERTIES
(
    'quality' = 'gold',
    'fact_grain' = 'one row per payment_id',
    'delta.enableChangeDataFeed' = 'true'
)
""")


payment_order_context_df = (
    spark.table(FACT_ORDERS_TABLE)
    .select(
        "order_id",
        "order_key",
        "user_key",
        "seller_key",
        "user_id",
        "seller_id",
        F.col("currency").alias(
            "order_currency"
        ),
        "order_amount"
    )
)


fact_payments_source_df = (
    spark.table(
        "marketplace_lab.silver.payments"
    )
    .join(
        payment_order_context_df,
        "order_id",
        "left"
    )
    .withColumn(
        "payment_key",
        F.sha2(
            F.concat(
                F.lit("PAYMENT|"),
                F.col("payment_id")
            ),
            256
        )
    )
    .withColumn(
        "payment_date",
        F.to_date("payment_time")
    )
    .withColumn(
        "is_successful",
        F.col("payment_status") == "SUCCEEDED"
    )
    .withColumn(
        "is_failed",
        F.col("payment_status") == "FAILED"
    )
    .withColumn(
        "is_pending",
        F.col("payment_status") == "PENDING"
    )
    .withColumn(
        "is_refunded",
        F.col("payment_status") == "REFUNDED"
    )
    .withColumn(
        "successful_amount",
        F.when(
            F.col("is_successful"),
            F.col("amount")
        )
        .otherwise(
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "failed_amount",
        F.when(
            F.col("is_failed"),
            F.col("amount")
        )
        .otherwise(
            F.lit(0).cast("decimal(18,2)")
        )
    )
    .withColumn(
        "refunded_amount",
        F.when(
            F.col("is_refunded"),
            F.col("amount")
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
                F.col("payment_id"),
                F.col("order_key"),
                F.col("user_key"),
                F.col("seller_key"),
                F.col("payment_time").cast("string"),
                F.col("payment_status"),
                F.col("payment_method"),
                F.col("currency"),
                F.col("amount").cast("string"),
                F.col("was_recovered").cast("string")
            ),
            256
        )
    )
    .select(
        "payment_key",
        "payment_id",
        "order_key",
        "order_id",
        "user_key",
        "seller_key",
        "user_id",
        "seller_id",
        "payment_date",
        "payment_time",
        "payment_status",
        "payment_method",
        "currency",
        "amount",
        "is_successful",
        "is_failed",
        "is_pending",
        "is_refunded",
        "successful_amount",
        "failed_amount",
        "refunded_amount",
        "was_recovered",
        "record_hash",
        F.current_timestamp().alias(
            "gold_created_at"
        ),
        F.current_timestamp().alias(
            "gold_updated_at"
        )
    )
)


invalid_payment_keys = (
    fact_payments_source_df
    .filter(
        F.col("order_key").isNull()
        | F.col("user_key").isNull()
        | F.col("seller_key").isNull()
    )
    .count()
)

currency_mismatch_count = (
    spark.table("marketplace_lab.silver.payments")
    .join(
        payment_order_context_df,
        "order_id"
    )
    .filter(
        F.col("currency")
        != F.col("order_currency")
    )
    .count()
)

assert invalid_payment_keys == 0
assert currency_mismatch_count == 0


fact_payments_source_df.createOrReplaceTempView(
    "incoming_fact_payments"
)

spark.sql(f"""
MERGE INTO {FACT_PAYMENTS_TABLE} AS target
USING incoming_fact_payments AS source
ON target.payment_id = source.payment_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.payment_key =
        source.payment_key,
    target.order_key =
        source.order_key,
    target.order_id =
        source.order_id,
    target.user_key =
        source.user_key,
    target.seller_key =
        source.seller_key,
    target.user_id =
        source.user_id,
    target.seller_id =
        source.seller_id,
    target.payment_date =
        source.payment_date,
    target.payment_time =
        source.payment_time,
    target.payment_status =
        source.payment_status,
    target.payment_method =
        source.payment_method,
    target.currency =
        source.currency,
    target.amount =
        source.amount,
    target.is_successful =
        source.is_successful,
    target.is_failed =
        source.is_failed,
    target.is_pending =
        source.is_pending,
    target.is_refunded =
        source.is_refunded,
    target.successful_amount =
        source.successful_amount,
    target.failed_amount =
        source.failed_amount,
    target.refunded_amount =
        source.refunded_amount,
    target.was_recovered =
        source.was_recovered,
    target.record_hash =
        source.record_hash,
    target.gold_updated_at =
        current_timestamp()

WHEN NOT MATCHED THEN INSERT *

WHEN NOT MATCHED BY SOURCE THEN DELETE
""")


# =========================================================
# 2. FACT_EVENTS
# =========================================================

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FACT_EVENTS_TABLE}
(
    event_key               STRING        NOT NULL,
    event_id                STRING        NOT NULL,

    user_key                STRING        NOT NULL,
    user_id                 STRING        NOT NULL,

    product_key             STRING,
    product_id              STRING,

    order_key               STRING,
    order_id                STRING,

    seller_key              STRING,

    event_date              DATE          NOT NULL,
    event_time_utc          TIMESTAMP     NOT NULL,
    ingestion_time_utc      TIMESTAMP     NOT NULL,

    event_type              STRING        NOT NULL,
    funnel_step             STRING        NOT NULL,
    funnel_step_order       INT           NOT NULL,

    session_id              STRING        NOT NULL,
    country_code            STRING        NOT NULL,
    device_type             STRING        NOT NULL,

    price                   DECIMAL(18,2),
    currency                STRING,
    quantity                BIGINT,

    arrival_delay_seconds   BIGINT        NOT NULL,
    is_late                 BOOLEAN       NOT NULL,

    record_hash             STRING        NOT NULL,
    gold_created_at         TIMESTAMP     NOT NULL,
    gold_updated_at         TIMESTAMP     NOT NULL
)
USING DELTA
COMMENT 'Gold event fact at one row per event_id'
TBLPROPERTIES
(
    'quality' = 'gold',
    'fact_grain' = 'one row per event_id',
    'delta.enableChangeDataFeed' = 'true'
)
""")


event_user_keys_df = (
    spark.table(DIM_USER_TABLE)
    .select(
        "user_id",
        "user_key"
    )
)

event_product_context_df = (
    spark.table(DIM_PRODUCT_TABLE)
    .select(
        "product_id",
        "product_key",
        F.col("seller_key").alias(
            "product_seller_key"
        )
    )
)

event_order_context_df = (
    spark.table(FACT_ORDERS_TABLE)
    .select(
        "order_id",
        "order_key",
        F.col("seller_key").alias(
            "order_seller_key"
        )
    )
)


fact_events_source_df = (
    spark.table(
        "marketplace_lab.silver.user_events"
    )
    .join(
        event_user_keys_df,
        "user_id",
        "left"
    )
    .join(
        event_product_context_df,
        "product_id",
        "left"
    )
    .join(
        event_order_context_df,
        "order_id",
        "left"
    )
    .withColumn(
        "event_key",
        F.sha2(
            F.concat(
                F.lit("EVENT|"),
                F.col("event_id")
            ),
            256
        )
    )
    .withColumn(
        "event_date",
        F.to_date("event_time_utc")
    )
    .withColumn(
        "seller_key",
        F.coalesce(
            F.col("order_seller_key"),
            F.col("product_seller_key")
        )
    )
    .withColumn(
        "funnel_step",
        F.when(
            F.col("event_type") == "product_viewed",
            F.lit("PRODUCT_VIEW")
        )
        .when(
            F.col("event_type") == "added_to_cart",
            F.lit("ADD_TO_CART")
        )
        .when(
            F.col("event_type") == "order_created",
            F.lit("ORDER_CREATED")
        )
        .when(
            F.col("event_type")
            == "payment_succeeded",
            F.lit("PAYMENT_SUCCEEDED")
        )
        .otherwise(
            F.lit("OTHER")
        )
    )
    .withColumn(
        "funnel_step_order",
        F.when(
            F.col("event_type") == "product_viewed",
            F.lit(1)
        )
        .when(
            F.col("event_type") == "added_to_cart",
            F.lit(2)
        )
        .when(
            F.col("event_type") == "order_created",
            F.lit(3)
        )
        .when(
            F.col("event_type")
            == "payment_succeeded",
            F.lit(4)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("event_id"),
                F.col("user_key"),
                F.coalesce(
                    F.col("product_key"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("order_key"),
                    F.lit("<NULL>")
                ),
                F.col("event_time_utc").cast("string"),
                F.col("ingestion_time_utc").cast("string"),
                F.col("event_type"),
                F.col("session_id"),
                F.col("country_code"),
                F.col("device_type"),
                F.coalesce(
                    F.col("price").cast("string"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("currency"),
                    F.lit("<NULL>")
                ),
                F.coalesce(
                    F.col("quantity").cast("string"),
                    F.lit("<NULL>")
                ),
                F.col(
                    "arrival_delay_seconds"
                ).cast("string"),
                F.col("is_late").cast("string")
            ),
            256
        )
    )
    .select(
        "event_key",
        "event_id",
        "user_key",
        "user_id",
        "product_key",
        "product_id",
        "order_key",
        "order_id",
        "seller_key",
        "event_date",
        "event_time_utc",
        "ingestion_time_utc",
        "event_type",
        "funnel_step",
        "funnel_step_order",
        "session_id",
        "country_code",
        "device_type",
        "price",
        "currency",
        "quantity",
        "arrival_delay_seconds",
        "is_late",
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
# 2.1. Проверка dimension keys событий
# ---------------------------------------------------------

missing_event_user_keys = (
    fact_events_source_df
    .filter(F.col("user_key").isNull())
    .count()
)

missing_required_product_keys = (
    fact_events_source_df
    .filter(
        F.col("product_id").isNotNull()
        & F.col("product_key").isNull()
    )
    .count()
)

missing_required_order_keys = (
    fact_events_source_df
    .filter(
        F.col("order_id").isNotNull()
        & F.col("order_key").isNull()
    )
    .count()
)

assert missing_event_user_keys == 0
assert missing_required_product_keys == 0
assert missing_required_order_keys == 0


fact_events_source_df.createOrReplaceTempView(
    "incoming_fact_events"
)

spark.sql(f"""
MERGE INTO {FACT_EVENTS_TABLE} AS target
USING incoming_fact_events AS source
ON target.event_id = source.event_id

WHEN MATCHED
 AND target.record_hash <> source.record_hash
THEN UPDATE SET
    target.event_key =
        source.event_key,
    target.user_key =
        source.user_key,
    target.user_id =
        source.user_id,
    target.product_key =
        source.product_key,
    target.product_id =
        source.product_id,
    target.order_key =
        source.order_key,
    target.order_id =
        source.order_id,
    target.seller_key =
        source.seller_key,
    target.event_date =
        source.event_date,
    target.event_time_utc =
        source.event_time_utc,
    target.ingestion_time_utc =
        source.ingestion_time_utc,
    target.event_type =
        source.event_type,
    target.funnel_step =
        source.funnel_step,
    target.funnel_step_order =
        source.funnel_step_order,
    target.session_id =
        source.session_id,
    target.country_code =
        source.country_code,
    target.device_type =
        source.device_type,
    target.price =
        source.price,
    target.currency =
        source.currency,
    target.quantity =
        source.quantity,
    target.arrival_delay_seconds =
        source.arrival_delay_seconds,
    target.is_late =
        source.is_late,
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

fact_payments_df = spark.table(
    FACT_PAYMENTS_TABLE
)

fact_events_df = spark.table(
    FACT_EVENTS_TABLE
)

payment_count = fact_payments_df.count()
event_count = fact_events_df.count()

distinct_payment_keys = (
    fact_payments_df
    .select("payment_key")
    .distinct()
    .count()
)

distinct_event_keys = (
    fact_events_df
    .select("event_key")
    .distinct()
    .count()
)

successful_payment_count = (
    fact_payments_df
    .filter(F.col("is_successful"))
    .count()
)

successful_payment_rate = (
    successful_payment_count
    / payment_count
    if payment_count > 0
    else 0.0
)

late_event_count = (
    fact_events_df
    .filter(F.col("is_late"))
    .count()
)

assert distinct_payment_keys == payment_count
assert distinct_event_keys == event_count


print("Gold payment and event facts completed")
print(f"fact_payments rows:          {payment_count}")
print(f"fact_events rows:            {event_count}")
print(f"Invalid payment keys:        {invalid_payment_keys}")
print(f"Currency mismatches:         {currency_mismatch_count}")
print(f"Missing event user keys:     {missing_event_user_keys}")
print(f"Missing event product keys:  {missing_required_product_keys}")
print(f"Missing event order keys:    {missing_required_order_keys}")
print(f"Successful payment rate:     {successful_payment_rate:.2%}")
print(f"Late events:                 {late_event_count}")
print("Overall status:              PASSED")

display(
    fact_payments_df
    .groupBy(
        "payment_status",
        "payment_method",
        "currency"
    )
    .agg(
        F.count("*").alias("payment_count"),
        F.sum("amount").alias("payment_amount"),
        F.sum("successful_amount").alias(
            "successful_amount"
        )
    )
    .orderBy(
        "payment_status",
        "payment_method",
        "currency"
    )
)

display(
    fact_events_df
    .groupBy(
        "event_type",
        "funnel_step",
        "funnel_step_order"
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("user_key").alias(
            "unique_users"
        ),
        F.countDistinct("session_id").alias(
            "unique_sessions"
        )
    )
    .orderBy(
        "funnel_step_order",
        "event_type"
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
