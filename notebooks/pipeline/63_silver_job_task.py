# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Parameterized Silver task
# MAGIC
# MAGIC Reused by the five dataset-specific Silver tasks.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG = "marketplace_lab"
TASK_AUDIT_TABLE = f"{CATALOG}.ops.pipeline_task_runs"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.invalid_records"


def ensure_widget(name: str, default_value: str = "") -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value)


for widget_name, widget_default in {
    "run_id": "",
    "dataset": "",
    "environment": "dev",
    "mode": "incremental",
    "load_date": "",
    "start_date": "",
    "end_date": "",
    "effective_source_set": "all",
    "task_run_id": "MANUAL",
    "repair_count": "0",
}.items():
    ensure_widget(widget_name, widget_default)


def widget_value(name: str) -> str:
    return (dbutils.widgets.get(name) or "").strip()


RUN_ID = widget_value("run_id")
DATASET = widget_value("dataset").lower()
ENVIRONMENT = widget_value("environment").lower()
MODE = widget_value("mode").lower().replace("_", "-")
LOAD_DATE = widget_value("load_date")
START_DATE = widget_value("start_date")
END_DATE = widget_value("end_date")
TASK_RUN_ID = widget_value("task_run_id") or "MANUAL"
REPAIR_COUNT = int(widget_value("repair_count") or "0")
EFFECTIVE_SOURCE_SET_RAW = widget_value("effective_source_set").lower()

ALLOWED_DATASETS = {"sellers", "products", "orders", "payments", "user_events"}

assert RUN_ID, "run_id is required"
assert DATASET in ALLOWED_DATASETS, f"Unsupported Silver dataset: {DATASET}"
assert MODE in {"incremental", "backfill", "replay", "full-refresh"}

BACKFILL_START_DATE = None
BACKFILL_END_DATE = None
if MODE == "backfill":
    assert START_DATE and END_DATE, (
        "start_date and end_date are required when mode=backfill"
    )
    BACKFILL_START_DATE = datetime.strptime(START_DATE, "%Y-%m-%d").date()
    BACKFILL_END_DATE = datetime.strptime(END_DATE, "%Y-%m-%d").date()
    assert BACKFILL_START_DATE <= BACKFILL_END_DATE, (
        f"Invalid backfill range: {START_DATE} > {END_DATE}"
    )

if EFFECTIVE_SOURCE_SET_RAW in {"", "all"}:
    EFFECTIVE_SOURCE_SET = ALLOWED_DATASETS.copy()
else:
    EFFECTIVE_SOURCE_SET = {
        item.strip()
        for item in EFFECTIVE_SOURCE_SET_RAW.split(",")
        if item.strip()
    }

TASK_KEY = f"silver_{DATASET}"
TASK_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

TARGET_TABLES = {
    "sellers": "marketplace_lab.silver.sellers",
    "products": "marketplace_lab.silver.products",
    "orders": "marketplace_lab.silver.orders",
    "payments": "marketplace_lab.silver.payments",
    "user_events": "marketplace_lab.silver.user_events",
}

BRONZE_TABLES = {
    "sellers": "marketplace_lab.bronze.sellers_raw",
    "products": "marketplace_lab.bronze.products_raw",
    "orders": "marketplace_lab.bronze.orders_raw",
    "payments": "marketplace_lab.bronze.payments_raw",
    "user_events": "marketplace_lab.bronze.user_events_raw",
}


def write_task_audit(
    status: str,
    completed_at=None,
    input_rows=None,
    output_rows=None,
    rejected_rows=None,
    error_code=None,
    error_message=None,
    metrics=None,
) -> None:
    now_value = datetime.now(timezone.utc).replace(tzinfo=None)
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
        RUN_ID, TASK_KEY, TASK_RUN_ID, REPAIR_COUNT, DATASET, status,
        TASK_STARTED_AT, completed_at, input_rows, output_rows,
        rejected_rows, None, error_code, error_message, metrics,
        TASK_STARTED_AT, now_value,
    )], audit_schema)
    audit_df.createOrReplaceTempView("silver_task_audit_source")

    spark.sql(f"""
    MERGE INTO {TASK_AUDIT_TABLE} AS target
    USING silver_task_audit_source AS source
      ON target.run_id = source.run_id
     AND target.task_key = source.task_key
     AND target.repair_count = source.repair_count
    WHEN MATCHED THEN UPDATE SET
        target.task_run_id = source.task_run_id,
        target.dataset = source.dataset,
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


if DATASET not in EFFECTIVE_SOURCE_SET:
    completed = datetime.now(timezone.utc).replace(tzinfo=None)
    write_task_audit(
        "SKIPPED",
        completed_at=completed,
        metrics={"reason": "dataset_not_in_effective_source_set"},
    )
    dbutils.notebook.exit(f"SKIPPED:{DATASET}")


assert spark.catalog.tableExists(BRONZE_TABLES[DATASET]), (
    f"Missing Bronze source: {BRONZE_TABLES[DATASET]}"
)


def read_bronze_source(table_name: str):
    """Read all Bronze rows, or only the requested inclusive backfill range."""
    source_df = spark.table(table_name)
    if MODE == "backfill":
        assert "_load_date" in source_df.columns, (
            f"Backfill source {table_name} has no _load_date column"
        )
        source_df = source_df.filter(
            F.to_date(F.col("_load_date")).between(
                F.lit(BACKFILL_START_DATE),
                F.lit(BACKFILL_END_DATE),
            )
        )
    return source_df

rows_before = (
    spark.table(TARGET_TABLES[DATASET]).count()
    if spark.catalog.tableExists(TARGET_TABLES[DATASET])
    else 0
)
quarantine_before = (
    spark.table(QUARANTINE_TABLE)
    .filter(F.col("dataset") == DATASET)
    .count()
)
input_rows = read_bronze_source(BRONZE_TABLES[DATASET]).count()

write_task_audit(
    "RUNNING",
    input_rows=input_rows,
    metrics={
        "environment": ENVIRONMENT,
        "mode": MODE,
        "load_date": LOAD_DATE,
        "start_date": START_DATE,
        "end_date": END_DATE,
    },
)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Dataset transformation

# COMMAND ----------

try:

    if DATASET == 'sellers':

        # Этап 6. Silver Layer
        # Ячейка 4. Bronze sellers -> Silver sellers + Quarantine

        from datetime import datetime, timezone

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window


        CURRENT_RUN_ID = globals().get(
            "RUN_ID",
            datetime.now(timezone.utc).strftime(
                "%Y%m%d_%H%M%S"
            )
        )

        BRONZE_SELLERS_TABLE = (
            "marketplace_lab.bronze.sellers_raw"
        )

        SILVER_SELLERS_TABLE = (
            "marketplace_lab.silver.sellers"
        )

        QUARANTINE_TABLE = (
            "marketplace_lab.quarantine.invalid_records"
        )

        DQ_RULE_CATALOG_TABLE = (
            "marketplace_lab.ops.data_quality_rule_catalog"
        )


        # ---------------------------------------------------------
        # 1. Явный Silver-контракт
        # ---------------------------------------------------------

        ALLOWED_SELLER_STATUSES = [
            "ACTIVE",
            "SUSPENDED",
            "CLOSED"
        ]

        ALLOWED_SELLER_SEGMENTS = [
            "SMALL",
            "MEDIUM",
            "ENTERPRISE"
        ]

        ALLOWED_SELLER_COUNTRIES = [
            "ES",
            "PL",
            "AT",
            "BE",
            "FR",
            "IT",
            "DE",
            "NL",
            "CZ",
            "RS"
        ]


        # ---------------------------------------------------------
        # 2. Добавляем специфичные правила sellers в DQ-каталог
        # ---------------------------------------------------------

        seller_specific_rules = [
            {
                "error_code": "INVALID_COUNTRY",
                "rule_category": "DOMAIN",
                "severity": "ERROR",
                "description": (
                    "Код страны продавца отсутствует "
                    "в разрешённом перечне"
                ),
                "quarantine_required": True
            },
            {
                "error_code": "INVALID_SEGMENT",
                "rule_category": "DOMAIN",
                "severity": "ERROR",
                "description": (
                    "Сегмент продавца отсутствует "
                    "в разрешённом перечне"
                ),
                "quarantine_required": True
            },
            {
                "error_code": "FUTURE_REGISTRATION_DATE",
                "rule_category": "BUSINESS",
                "severity": "ERROR",
                "description": (
                    "Дата регистрации продавца находится в будущем"
                ),
                "quarantine_required": True
            }
        ]

        seller_specific_rules_df = (
            spark.createDataFrame(seller_specific_rules)
            .withColumn(
                "created_at",
                F.current_timestamp()
            )
            .select(
                "error_code",
                "rule_category",
                "severity",
                "description",
                "quarantine_required",
                "created_at"
            )
        )

        seller_specific_rules_df.createOrReplaceTempView(
            "incoming_seller_dq_rules"
        )

        spark.sql(f"""
        MERGE INTO {DQ_RULE_CATALOG_TABLE} AS target
        USING incoming_seller_dq_rules AS source
        ON target.error_code = source.error_code

        WHEN MATCHED THEN UPDATE SET
            target.rule_category = source.rule_category,
            target.severity = source.severity,
            target.description = source.description,
            target.quarantine_required =
                source.quarantine_required,
            target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT
        (
            error_code,
            rule_category,
            severity,
            description,
            quarantine_required,
            created_at,
            updated_at
        )
        VALUES
        (
            source.error_code,
            source.rule_category,
            source.severity,
            source.description,
            source.quarantine_required,
            source.created_at,
            NULL
        )
        """)


        # ---------------------------------------------------------
        # 3. Создание Silver-таблицы
        # ---------------------------------------------------------

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_SELLERS_TABLE}
        (
            seller_id             STRING    NOT NULL,
            country_code          STRING    NOT NULL,
            segment               STRING    NOT NULL,
            registration_date     DATE      NOT NULL,
            seller_status         STRING    NOT NULL,

            record_hash           STRING    NOT NULL,

            source_batch_id       STRING,
            source_load_date      DATE,
            source_file           STRING,
            bronze_ingest_ts      TIMESTAMP,

            silver_created_at     TIMESTAMP NOT NULL,
            silver_updated_at     TIMESTAMP NOT NULL
        )
        USING DELTA
        COMMENT 'Cleaned and deduplicated Marketplace sellers'
        TBLPROPERTIES
        (
            'quality' = 'silver',
            'delta.enableChangeDataFeed' = 'true'
        )
        """)


        # ---------------------------------------------------------
        # 4. Чтение Bronze и сохранение raw_record
        # ---------------------------------------------------------

        bronze_sellers_df = read_bronze_source(
            BRONZE_SELLERS_TABLE
        )

        bronze_columns = bronze_sellers_df.columns

        staged_sellers_df = (
            bronze_sellers_df
            .withColumn(
                "raw_record",
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name in bronze_columns
                        ]
                    )
                )
            )

            # Нормализация бизнес-полей
            .withColumn(
                "normalized_seller_id",
                F.upper(
                    F.trim(F.col("seller_id"))
                )
            )
            .withColumn(
                "normalized_country",
                F.upper(
                    F.trim(F.col("country"))
                )
            )
            .withColumn(
                "normalized_segment",
                F.upper(
                    F.trim(F.col("segment"))
                )
            )
            .withColumn(
                "normalized_status",
                F.upper(
                    F.trim(F.col("status"))
                )
            )
            .withColumn(
                "normalized_registration_date",
                F.col("registration_date").cast("date")
            )
        )


        # ---------------------------------------------------------
        # 5. Назначение одной основной ошибки каждой записи
        #
        # Порядок WHEN определяет приоритет ошибок.
        # ---------------------------------------------------------

        staged_sellers_df = (
            staged_sellers_df
            .withColumn(
                "error_code",
                F.when(
                    F.col("_rescued_data").isNotNull(),
                    F.lit("RESCUED_DATA")
                )
                .when(
                    F.col("normalized_seller_id").isNull()
                    | (F.col("normalized_seller_id") == ""),
                    F.lit("MISSING_BUSINESS_KEY")
                )
                .when(
                    F.col("normalized_country").isNull()
                    | (F.col("normalized_country") == "")
                    | F.col("normalized_segment").isNull()
                    | (F.col("normalized_segment") == "")
                    | F.col(
                        "normalized_registration_date"
                    ).isNull()
                    | F.col("normalized_status").isNull()
                    | (F.col("normalized_status") == ""),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    ~F.col("normalized_country").isin(
                        ALLOWED_SELLER_COUNTRIES
                    ),
                    F.lit("INVALID_COUNTRY")
                )
                .when(
                    ~F.col("normalized_segment").isin(
                        ALLOWED_SELLER_SEGMENTS
                    ),
                    F.lit("INVALID_SEGMENT")
                )
                .when(
                    ~F.col("normalized_status").isin(
                        ALLOWED_SELLER_STATUSES
                    ),
                    F.lit("INVALID_STATUS")
                )
                .when(
                    F.col("normalized_registration_date")
                    > F.current_date(),
                    F.lit("FUTURE_REGISTRATION_DATE")
                )
            )
            .withColumn(
                "error_message",
                F.when(
                    F.col("error_code") == "RESCUED_DATA",
                    F.lit(
                        "Bronze record contains rescued data"
                    )
                )
                .when(
                    F.col("error_code")
                    == "MISSING_BUSINESS_KEY",
                    F.lit(
                        "seller_id is null or empty"
                    )
                )
                .when(
                    F.col("error_code")
                    == "MISSING_REQUIRED_FIELD",
                    F.lit(
                        "country, segment, registration_date "
                        "or status is missing"
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_COUNTRY",
                    F.concat(
                        F.lit("Unsupported seller country: "),
                        F.coalesce(
                            F.col("normalized_country"),
                            F.lit("<NULL>")
                        )
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_SEGMENT",
                    F.concat(
                        F.lit("Unsupported seller segment: "),
                        F.coalesce(
                            F.col("normalized_segment"),
                            F.lit("<NULL>")
                        )
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_STATUS",
                    F.concat(
                        F.lit("Unsupported seller status: "),
                        F.coalesce(
                            F.col("normalized_status"),
                            F.lit("<NULL>")
                        )
                    )
                )
                .when(
                    F.col("error_code")
                    == "FUTURE_REGISTRATION_DATE",
                    F.concat(
                        F.lit("Future registration date: "),
                        F.col(
                            "normalized_registration_date"
                        ).cast("string")
                    )
                )
            )
        )


        # ---------------------------------------------------------
        # 6. Разделение валидных и невалидных записей
        # ---------------------------------------------------------

        invalid_sellers_df = (
            staged_sellers_df
            .filter(
                F.col("error_code").isNotNull()
            )
        )

        valid_seller_candidates_df = (
            staged_sellers_df
            .filter(
                F.col("error_code").isNull()
            )
        )


        # ---------------------------------------------------------
        # 7. Дедупликация валидных sellers
        #
        # Выбираем наиболее поздно загруженную запись.
        # ---------------------------------------------------------

        seller_dedup_window = (
            Window
            .partitionBy("normalized_seller_id")
            .orderBy(
                F.col("_ingest_ts").desc(),
                F.col("_source_file").desc()
            )
        )

        ranked_valid_sellers_df = (
            valid_seller_candidates_df
            .withColumn(
                "_dedup_rank",
                F.row_number().over(
                    seller_dedup_window
                )
            )
        )

        duplicate_sellers_df = (
            ranked_valid_sellers_df
            .filter(
                F.col("_dedup_rank") > 1
            )
        )

        valid_sellers_latest_df = (
            ranked_valid_sellers_df
            .filter(
                F.col("_dedup_rank") == 1
            )
        )


        # ---------------------------------------------------------
        # 8. Подготовка quarantine-записей
        # ---------------------------------------------------------

        seller_quarantine_df = (
            invalid_sellers_df
            .withColumn(
                "business_key",
                F.col("normalized_seller_id")
            )
            .withColumn(
                "quarantine_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("sellers"),
                        F.coalesce(
                            F.col("normalized_seller_id"),
                            F.lit("<NULL>")
                        ),
                        F.col("error_code"),
                        F.coalesce(
                            F.col("_source_file"),
                            F.lit("<NO_FILE>")
                        ),
                        F.coalesce(
                            F.col("_batch_id"),
                            F.lit("<NO_BATCH>")
                        ),
                        F.col("raw_record")
                    ),
                    256
                )
            )
            .select(
                "quarantine_id",
                F.lit("sellers").alias("dataset"),
                "business_key",
                "error_code",
                "error_message",
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_batch_id").alias(
                    "batch_id"
                ),
                F.col("_load_date").alias(
                    "load_date"
                ),
                F.col("_ingest_ts").alias(
                    "original_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "quarantined_at"
                ),
                F.lit(CURRENT_RUN_ID).alias(
                    "processing_run_id"
                ),
                "raw_record"
            )
        )

        seller_quarantine_df.createOrReplaceTempView(
            "incoming_seller_quarantine"
        )


        # ---------------------------------------------------------
        # 9. Идемпотентный MERGE в quarantine
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS target
        USING incoming_seller_quarantine AS source
        ON target.quarantine_id = source.quarantine_id

        WHEN MATCHED THEN UPDATE SET
            target.error_message = source.error_message,
            target.quarantined_at = source.quarantined_at,
            target.processing_run_id =
                source.processing_run_id

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 10. Подготовка валидных Silver-записей
        # ---------------------------------------------------------

        silver_sellers_source_df = (
            valid_sellers_latest_df
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_seller_id"),
                        F.col("normalized_country"),
                        F.col("normalized_segment"),
                        F.col(
                            "normalized_registration_date"
                        ).cast("string"),
                        F.col("normalized_status")
                    ),
                    256
                )
            )
            .select(
                F.col("normalized_seller_id").alias(
                    "seller_id"
                ),
                F.col("normalized_country").alias(
                    "country_code"
                ),
                F.col("normalized_segment").alias(
                    "segment"
                ),
                F.col(
                    "normalized_registration_date"
                ).alias("registration_date"),
                F.col("normalized_status").alias(
                    "seller_status"
                ),
                "record_hash",
                F.col("_batch_id").alias(
                    "source_batch_id"
                ),
                F.col("_load_date").alias(
                    "source_load_date"
                ),
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_ingest_ts").alias(
                    "bronze_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "silver_created_at"
                ),
                F.current_timestamp().alias(
                    "silver_updated_at"
                )
            )
        )

        silver_sellers_source_df.createOrReplaceTempView(
            "incoming_silver_sellers"
        )


        # ---------------------------------------------------------
        # 11. Идемпотентный MERGE в Silver
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {SILVER_SELLERS_TABLE} AS target
        USING incoming_silver_sellers AS source
        ON target.seller_id = source.seller_id

        WHEN MATCHED
         AND source.bronze_ingest_ts >= target.bronze_ingest_ts
         AND source.record_hash <> target.record_hash
        THEN UPDATE SET
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
            target.source_batch_id =
                source.source_batch_id,
            target.source_load_date =
                source.source_load_date,
            target.source_file =
                source.source_file,
            target.bronze_ingest_ts =
                source.bronze_ingest_ts,
            target.silver_updated_at =
                current_timestamp()

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 12. Reconciliation
        # ---------------------------------------------------------

        bronze_count = bronze_sellers_df.count()
        invalid_count = invalid_sellers_df.count()
        duplicate_count = duplicate_sellers_df.count()
        valid_latest_count = valid_sellers_latest_df.count()

        silver_count = spark.table(
            SILVER_SELLERS_TABLE
        ).count()

        seller_quarantine_count = (
            spark.table(QUARANTINE_TABLE)
            .filter(
                F.col("dataset") == "sellers"
            )
            .count()
        )

        reconciled_count = (
            invalid_count
            + duplicate_count
            + valid_latest_count
        )

        assert bronze_count == reconciled_count, (
            f"Sellers reconciliation failed: "
            f"Bronze={bronze_count}, "
            f"valid={valid_latest_count}, "
            f"invalid={invalid_count}, "
            f"duplicates={duplicate_count}"
        )

        assert silver_count == valid_latest_count, (
            f"Expected {valid_latest_count} Silver sellers, "
            f"found {silver_count}"
        )

        assert seller_quarantine_count == invalid_count, (
            f"Expected {invalid_count} seller quarantine rows, "
            f"found {seller_quarantine_count}"
        )


        # ---------------------------------------------------------
        # 13. Результат
        # ---------------------------------------------------------

        print("Silver sellers pipeline completed")
        print(f"Run ID:                  {CURRENT_RUN_ID}")
        print(f"Bronze rows:             {bronze_count}")
        print(f"Valid Silver rows:       {valid_latest_count}")
        print(f"Invalid rows:            {invalid_count}")
        print(f"Duplicate rows removed:  {duplicate_count}")
        print(f"Silver table rows:       {silver_count}")
        print(f"Seller quarantine rows:  {seller_quarantine_count}")
        print("Reconciliation status:   PASSED")

        display(
            spark.table(SILVER_SELLERS_TABLE)
            .orderBy("seller_id")
        )

        display(
            spark.table(QUARANTINE_TABLE)
            .filter(
                F.col("dataset") == "sellers"
            )
            .groupBy(
                "error_code",
                "error_message"
            )
            .count()
            .orderBy(
                F.desc("count"),
                "error_code"
            )
        )



    elif DATASET == 'products':

        # Этап 6. Silver Layer
        # Ячейка 5. Bronze products -> Silver products + Quarantine

        from datetime import datetime, timezone

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window


        CURRENT_RUN_ID = globals().get(
            "RUN_ID",
            datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        )

        BRONZE_PRODUCTS_TABLE = (
            "marketplace_lab.bronze.products_raw"
        )

        SILVER_PRODUCTS_TABLE = (
            "marketplace_lab.silver.products"
        )

        SILVER_SELLERS_TABLE = (
            "marketplace_lab.silver.sellers"
        )

        QUARANTINE_TABLE = (
            "marketplace_lab.quarantine.invalid_records"
        )


        # ---------------------------------------------------------
        # 1. Создание Silver-таблицы products
        # ---------------------------------------------------------

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_PRODUCTS_TABLE}
        (
            product_id            STRING        NOT NULL,
            seller_id             STRING        NOT NULL,
            category              STRING        NOT NULL,
            product_name          STRING        NOT NULL,
            price                 DECIMAL(18,2) NOT NULL,
            is_active             BOOLEAN       NOT NULL,

            record_hash           STRING        NOT NULL,

            source_batch_id       STRING,
            source_load_date      DATE,
            source_file           STRING,
            bronze_ingest_ts      TIMESTAMP,

            silver_created_at     TIMESTAMP     NOT NULL,
            silver_updated_at     TIMESTAMP     NOT NULL
        )
        USING DELTA
        COMMENT 'Cleaned and deduplicated Marketplace products'
        TBLPROPERTIES
        (
            'quality' = 'silver',
            'delta.enableChangeDataFeed' = 'true'
        )
        """)


        # ---------------------------------------------------------
        # 2. Чтение Bronze и подготовка raw_record
        # ---------------------------------------------------------

        bronze_products_df = read_bronze_source(
            BRONZE_PRODUCTS_TABLE
        )

        bronze_product_columns = bronze_products_df.columns

        staged_products_df = (
            bronze_products_df
            .withColumn(
                "raw_record",
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name
                            in bronze_product_columns
                        ]
                    )
                )
            )
            .withColumn(
                "normalized_product_id",
                F.upper(
                    F.trim(F.col("product_id"))
                )
            )
            .withColumn(
                "normalized_seller_id",
                F.upper(
                    F.trim(F.col("seller_id"))
                )
            )
            .withColumn(
                "normalized_category",
                F.upper(
                    F.trim(F.col("category"))
                )
            )
            .withColumn(
                "normalized_product_name",
                F.trim(F.col("product_name"))
            )
            .withColumn(
                "normalized_price",
                F.col("price").cast("decimal(18,2)")
            )
            .withColumn(
                "normalized_is_active",
                F.col("is_active").cast("boolean")
            )
        )


        # ---------------------------------------------------------
        # 3. Проверка ссылки product -> seller
        # ---------------------------------------------------------

        silver_seller_keys_df = (
            spark.table(SILVER_SELLERS_TABLE)
            .select(
                F.col("seller_id").alias(
                    "reference_seller_id"
                )
            )
            .distinct()
        )

        staged_products_df = (
            staged_products_df
            .join(
                silver_seller_keys_df,
                staged_products_df[
                    "normalized_seller_id"
                ] == silver_seller_keys_df[
                    "reference_seller_id"
                ],
                "left"
            )
        )


        # ---------------------------------------------------------
        # 4. Проверки качества
        # ---------------------------------------------------------

        staged_products_df = (
            staged_products_df
            .withColumn(
                "error_code",
                F.when(
                    F.col("_rescued_data").isNotNull(),
                    F.lit("RESCUED_DATA")
                )
                .when(
                    F.col("normalized_product_id").isNull()
                    | (F.col("normalized_product_id") == ""),
                    F.lit("MISSING_BUSINESS_KEY")
                )
                .when(
                    F.col("normalized_seller_id").isNull()
                    | (F.col("normalized_seller_id") == "")
                    | F.col("normalized_category").isNull()
                    | (F.col("normalized_category") == "")
                    | F.col(
                        "normalized_product_name"
                    ).isNull()
                    | (
                        F.col("normalized_product_name")
                        == ""
                    )
                    | F.col("normalized_price").isNull()
                    | F.col(
                        "normalized_is_active"
                    ).isNull(),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    F.col("normalized_price") < F.lit(0),
                    F.lit("NEGATIVE_AMOUNT")
                )
                .when(
                    F.col("reference_seller_id").isNull(),
                    F.lit("UNKNOWN_REFERENCE")
                )
            )
            .withColumn(
                "error_message",
                F.when(
                    F.col("error_code") == "RESCUED_DATA",
                    F.lit(
                        "Bronze product contains rescued data"
                    )
                )
                .when(
                    F.col("error_code")
                    == "MISSING_BUSINESS_KEY",
                    F.lit(
                        "product_id is null or empty"
                    )
                )
                .when(
                    F.col("error_code")
                    == "MISSING_REQUIRED_FIELD",
                    F.lit(
                        "seller_id, category, product_name, "
                        "price or is_active is missing"
                    )
                )
                .when(
                    F.col("error_code")
                    == "NEGATIVE_AMOUNT",
                    F.concat(
                        F.lit("Product price is negative: "),
                        F.col(
                            "normalized_price"
                        ).cast("string")
                    )
                )
                .when(
                    F.col("error_code")
                    == "UNKNOWN_REFERENCE",
                    F.concat(
                        F.lit(
                            "Seller does not exist in Silver: "
                        ),
                        F.coalesce(
                            F.col("normalized_seller_id"),
                            F.lit("<NULL>")
                        )
                    )
                )
            )
        )


        # ---------------------------------------------------------
        # 5. Разделение валидных и невалидных записей
        # ---------------------------------------------------------

        invalid_products_df = (
            staged_products_df
            .filter(
                F.col("error_code").isNotNull()
            )
        )

        valid_product_candidates_df = (
            staged_products_df
            .filter(
                F.col("error_code").isNull()
            )
        )


        # ---------------------------------------------------------
        # 6. Дедупликация по product_id
        # ---------------------------------------------------------

        product_dedup_window = (
            Window
            .partitionBy("normalized_product_id")
            .orderBy(
                F.col("_ingest_ts").desc(),
                F.col("_source_file").desc()
            )
        )

        ranked_valid_products_df = (
            valid_product_candidates_df
            .withColumn(
                "_dedup_rank",
                F.row_number().over(
                    product_dedup_window
                )
            )
        )

        duplicate_products_df = (
            ranked_valid_products_df
            .filter(
                F.col("_dedup_rank") > 1
            )
        )

        valid_products_latest_df = (
            ranked_valid_products_df
            .filter(
                F.col("_dedup_rank") == 1
            )
        )


        # ---------------------------------------------------------
        # 7. Подготовка quarantine-записей
        # ---------------------------------------------------------

        product_quarantine_df = (
            invalid_products_df
            .withColumn(
                "business_key",
                F.col("normalized_product_id")
            )
            .withColumn(
                "quarantine_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("products"),
                        F.coalesce(
                            F.col("normalized_product_id"),
                            F.lit("<NULL>")
                        ),
                        F.col("error_code"),
                        F.coalesce(
                            F.col("_source_file"),
                            F.lit("<NO_FILE>")
                        ),
                        F.coalesce(
                            F.col("_batch_id"),
                            F.lit("<NO_BATCH>")
                        ),
                        F.col("raw_record")
                    ),
                    256
                )
            )
            .select(
                "quarantine_id",
                F.lit("products").alias("dataset"),
                "business_key",
                "error_code",
                "error_message",
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_batch_id").alias(
                    "batch_id"
                ),
                F.col("_load_date").alias(
                    "load_date"
                ),
                F.col("_ingest_ts").alias(
                    "original_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "quarantined_at"
                ),
                F.lit(CURRENT_RUN_ID).alias(
                    "processing_run_id"
                ),
                "raw_record"
            )
        )

        product_quarantine_df.createOrReplaceTempView(
            "incoming_product_quarantine"
        )


        # ---------------------------------------------------------
        # 8. Идемпотентный MERGE в quarantine
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS target
        USING incoming_product_quarantine AS source
        ON target.quarantine_id = source.quarantine_id

        WHEN MATCHED THEN UPDATE SET
            target.error_message =
                source.error_message,
            target.quarantined_at =
                source.quarantined_at,
            target.processing_run_id =
                source.processing_run_id

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 9. Подготовка Silver-записей
        # ---------------------------------------------------------

        silver_products_source_df = (
            valid_products_latest_df
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_product_id"),
                        F.col("normalized_seller_id"),
                        F.col("normalized_category"),
                        F.col("normalized_product_name"),
                        F.col(
                            "normalized_price"
                        ).cast("string"),
                        F.col(
                            "normalized_is_active"
                        ).cast("string")
                    ),
                    256
                )
            )
            .select(
                F.col("normalized_product_id").alias(
                    "product_id"
                ),
                F.col("normalized_seller_id").alias(
                    "seller_id"
                ),
                F.col("normalized_category").alias(
                    "category"
                ),
                F.col(
                    "normalized_product_name"
                ).alias("product_name"),
                F.col("normalized_price").alias(
                    "price"
                ),
                F.col("normalized_is_active").alias(
                    "is_active"
                ),
                "record_hash",
                F.col("_batch_id").alias(
                    "source_batch_id"
                ),
                F.col("_load_date").alias(
                    "source_load_date"
                ),
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_ingest_ts").alias(
                    "bronze_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "silver_created_at"
                ),
                F.current_timestamp().alias(
                    "silver_updated_at"
                )
            )
        )

        silver_products_source_df.createOrReplaceTempView(
            "incoming_silver_products"
        )


        # ---------------------------------------------------------
        # 10. Идемпотентный MERGE в Silver
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {SILVER_PRODUCTS_TABLE} AS target
        USING incoming_silver_products AS source
        ON target.product_id = source.product_id

        WHEN MATCHED
         AND source.bronze_ingest_ts >= target.bronze_ingest_ts
         AND source.record_hash <> target.record_hash
        THEN UPDATE SET
            target.seller_id =
                source.seller_id,
            target.category =
                source.category,
            target.product_name =
                source.product_name,
            target.price =
                source.price,
            target.is_active =
                source.is_active,
            target.record_hash =
                source.record_hash,
            target.source_batch_id =
                source.source_batch_id,
            target.source_load_date =
                source.source_load_date,
            target.source_file =
                source.source_file,
            target.bronze_ingest_ts =
                source.bronze_ingest_ts,
            target.silver_updated_at =
                current_timestamp()

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 11. Reconciliation
        # ---------------------------------------------------------

        bronze_count = bronze_products_df.count()
        invalid_count = invalid_products_df.count()
        duplicate_count = duplicate_products_df.count()
        valid_latest_count = valid_products_latest_df.count()

        silver_count = spark.table(
            SILVER_PRODUCTS_TABLE
        ).count()

        current_quarantine_count = (
            spark.table(QUARANTINE_TABLE)
            .select("quarantine_id")
            .join(
                product_quarantine_df.select(
                    "quarantine_id"
                ).distinct(),
                "quarantine_id",
                "inner"
            )
            .count()
        )

        reconciled_count = (
            invalid_count
            + duplicate_count
            + valid_latest_count
        )

        assert bronze_count == reconciled_count, (
            f"Products reconciliation failed: "
            f"Bronze={bronze_count}, "
            f"valid={valid_latest_count}, "
            f"invalid={invalid_count}, "
            f"duplicates={duplicate_count}"
        )

        assert silver_count == valid_latest_count, (
            f"Expected {valid_latest_count} Silver products, "
            f"found {silver_count}"
        )

        assert current_quarantine_count == invalid_count, (
            f"Expected {invalid_count} current quarantine rows, "
            f"found {current_quarantine_count}"
        )


        # ---------------------------------------------------------
        # 12. Результат
        # ---------------------------------------------------------

        print("Silver products pipeline completed")
        print(f"Run ID:                  {CURRENT_RUN_ID}")
        print(f"Bronze rows:             {bronze_count}")
        print(f"Valid Silver rows:       {valid_latest_count}")
        print(f"Invalid rows:            {invalid_count}")
        print(f"Duplicate rows removed:  {duplicate_count}")
        print(f"Silver table rows:       {silver_count}")
        print(f"Current quarantine rows: {current_quarantine_count}")
        print("Reconciliation status:   PASSED")

        display(
            spark.table(SILVER_PRODUCTS_TABLE)
            .orderBy("product_id")
        )

        display(
            product_quarantine_df
            .groupBy(
                "error_code",
                "error_message"
            )
            .count()
            .orderBy(
                F.desc("count"),
                "error_code"
            )
        )



    elif DATASET == 'orders':

        # Этап 6. Silver Layer
        # Ячейка 7A. Нормализация, дедупликация и DQ для orders

        from datetime import datetime, timezone

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window


        CURRENT_RUN_ID = globals().get(
            "RUN_ID",
            datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        )

        BRONZE_ORDERS_TABLE = (
            "marketplace_lab.bronze.orders_raw"
        )

        SILVER_SELLERS_TABLE = (
            "marketplace_lab.silver.sellers"
        )

        SILVER_PRODUCTS_TABLE = (
            "marketplace_lab.silver.products"
        )

        DQ_RULE_CATALOG_TABLE = (
            "marketplace_lab.ops.data_quality_rule_catalog"
        )


        ALLOWED_ORDER_STATUSES = [
            "CANCELLED",
            "CREATED",
            "DELIVERED",
            "PAID",
            "SHIPPED"
        ]

        ALLOWED_CURRENCIES = [
            "EUR",
            "GBP",
            "USD"
        ]

        ALLOWED_SALES_CHANNELS = [
            "MARKETPLACE_API",
            "MOBILE_APP",
            "WEB"
        ]

        ALLOWED_DELIVERY_PRIORITIES = [
            "EXPRESS",
            "STANDARD"
        ]

        ALLOWED_SCHEMA_VERSIONS = [1, 2]


        # ---------------------------------------------------------
        # 1. Специфичные DQ-правила orders
        # ---------------------------------------------------------

        order_specific_rules = [
            {
                "error_code": "INVALID_SCHEMA_VERSION",
                "rule_category": "SCHEMA",
                "severity": "ERROR",
                "description": "Unsupported order schema version",
                "quarantine_required": True
            },
            {
                "error_code": "INVALID_SALES_CHANNEL",
                "rule_category": "DOMAIN",
                "severity": "ERROR",
                "description": "Unsupported sales channel",
                "quarantine_required": True
            },
            {
                "error_code": "INVALID_DELIVERY_PRIORITY",
                "rule_category": "DOMAIN",
                "severity": "ERROR",
                "description": "Unsupported delivery priority",
                "quarantine_required": True
            },
            {
                "error_code": "INVALID_ORDER_ITEM",
                "rule_category": "BUSINESS",
                "severity": "ERROR",
                "description": "Order contains an invalid item",
                "quarantine_required": True
            },
            {
                "error_code": "INVALID_TIME_SEQUENCE",
                "rule_category": "BUSINESS",
                "severity": "ERROR",
                "description": "updated_at is earlier than created_at",
                "quarantine_required": True
            }
        ]

        order_rules_df = (
            spark.createDataFrame(order_specific_rules)
            .withColumn("created_at", F.current_timestamp())
        )

        order_rules_df.createOrReplaceTempView(
            "incoming_order_dq_rules"
        )

        spark.sql(f"""
        MERGE INTO {DQ_RULE_CATALOG_TABLE} AS target
        USING incoming_order_dq_rules AS source
        ON target.error_code = source.error_code

        WHEN MATCHED THEN UPDATE SET
            target.rule_category = source.rule_category,
            target.severity = source.severity,
            target.description = source.description,
            target.quarantine_required =
                source.quarantine_required,
            target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT
        (
            error_code,
            rule_category,
            severity,
            description,
            quarantine_required,
            created_at,
            updated_at
        )
        VALUES
        (
            source.error_code,
            source.rule_category,
            source.severity,
            source.description,
            source.quarantine_required,
            source.created_at,
            NULL
        )
        """)


        # ---------------------------------------------------------
        # 2. Нормализация Bronze
        # ---------------------------------------------------------

        bronze_orders_df = read_bronze_source(
            BRONZE_ORDERS_TABLE
        )

        bronze_order_columns = bronze_orders_df.columns

        normalized_orders_df = (
            bronze_orders_df
            .withColumn(
                "raw_record",
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name in bronze_order_columns
                        ]
                    )
                )
            )
            .withColumn(
                "normalized_order_id",
                F.upper(F.trim(F.col("order_id")))
            )
            .withColumn(
                "normalized_user_id",
                F.upper(F.trim(F.col("user_id")))
            )
            .withColumn(
                "normalized_seller_id",
                F.upper(F.trim(F.col("seller_id")))
            )
            .withColumn(
                "normalized_order_status",
                F.upper(F.trim(F.col("order_status")))
            )
            .withColumn(
                "normalized_currency",
                F.upper(F.trim(F.col("currency")))
            )
            .withColumn(
                "normalized_sales_channel",
                F.upper(F.trim(F.col("sales_channel")))
            )
            .withColumn(
                "normalized_delivery_priority",
                F.upper(F.trim(F.col("delivery_priority")))
            )
            .withColumn(
                "normalized_coupon_code",
                F.upper(F.trim(F.col("coupon_code")))
            )
            .withColumn(
                "normalized_created_at",
                F.col("created_at").cast("timestamp")
            )
            .withColumn(
                "normalized_updated_at",
                F.col("updated_at").cast("timestamp")
            )
            .withColumn(
                "normalized_schema_version",
                F.coalesce(
                    F.col("schema_version").cast("integer"),
                    F.lit(1)
                )
            )
            .withColumn(
                "normalized_total_quantity",
                F.col("total_quantity").cast("long")
            )
            .withColumn(
                "calculated_total_quantity",
                F.expr("""
                    aggregate(
                        items,
                        CAST(0 AS BIGINT),
                        (acc, item) ->
                            acc + COALESCE(
                                CAST(item.quantity AS BIGINT),
                                CAST(0 AS BIGINT)
                            )
                    )
                """)
            )
            .withColumn(
                "calculated_order_amount",
                F.expr("""
                    aggregate(
                        items,
                        CAST(0 AS DECIMAL(18,2)),
                        (acc, item) ->
                            CAST(
                                acc
                                +
                                COALESCE(
                                    CAST(
                                        item.line_amount
                                        AS DECIMAL(18,2)
                                    ),
                                    CAST(0 AS DECIMAL(18,2))
                                )
                                AS DECIMAL(18,2)
                            )
                    )
                """)
            )
            .withColumn(
                "has_invalid_item",
                F.expr("""
                    exists(
                        items,
                        item ->
                            item.product_id IS NULL
                            OR trim(item.product_id) = ''
                            OR item.quantity IS NULL
                            OR item.quantity <= 0
                            OR item.unit_price IS NULL
                            OR item.unit_price < 0
                            OR item.line_amount IS NULL
                            OR item.line_amount < 0
                    )
                """)
            )
            .withColumn(
                "has_line_amount_mismatch",
                F.expr("""
                    exists(
                        items,
                        item ->
                            ABS(
                                CAST(
                                    item.line_amount
                                    AS DECIMAL(18,2)
                                )
                                -
                                (
                                    CAST(
                                        item.quantity
                                        AS DECIMAL(18,2)
                                    )
                                    *
                                    CAST(
                                        item.unit_price
                                        AS DECIMAL(18,2)
                                    )
                                )
                            ) > CAST(0.01 AS DECIMAL(18,2))
                    )
                """)
            )
        )


        # ---------------------------------------------------------
        # 3. Выбор актуальной версии каждого order_id
        # ---------------------------------------------------------

        normalized_orders_df = (
            normalized_orders_df
            .withColumn(
                "_dedup_partition_key",
                F.coalesce(
                    F.col("normalized_order_id"),
                    F.sha2(F.col("raw_record"), 256)
                )
            )
        )

        order_version_window = (
            Window
            .partitionBy("_dedup_partition_key")
            .orderBy(
                F.col("normalized_updated_at")
                .desc_nulls_last(),
                F.col("normalized_schema_version")
                .desc_nulls_last(),
                F.col("_ingest_ts").desc(),
                F.col("_source_file").desc()
            )
        )

        ranked_orders_df = (
            normalized_orders_df
            .withColumn(
                "_version_rank",
                F.row_number().over(order_version_window)
            )
        )

        superseded_orders_df = (
            ranked_orders_df
            .filter(F.col("_version_rank") > 1)
        )

        latest_orders_df = (
            ranked_orders_df
            .filter(F.col("_version_rank") == 1)
        )


        # ---------------------------------------------------------
        # 4. Referential integrity: seller
        # ---------------------------------------------------------

        seller_reference_df = (
            spark.table(SILVER_SELLERS_TABLE)
            .select(
                F.col("seller_id").alias(
                    "reference_seller_id"
                )
            )
            .distinct()
        )

        latest_orders_df = (
            latest_orders_df
            .join(
                seller_reference_df,
                latest_orders_df["normalized_seller_id"]
                == seller_reference_df["reference_seller_id"],
                "left"
            )
        )


        # ---------------------------------------------------------
        # 5. Referential integrity: products inside items
        # ---------------------------------------------------------

        product_reference_df = (
            spark.table(SILVER_PRODUCTS_TABLE)
            .select(
                F.col("product_id").alias(
                    "reference_product_id"
                )
            )
            .distinct()
        )

        unknown_product_orders_df = (
            latest_orders_df
            .select(
                "normalized_order_id",
                F.explode_outer("items").alias("item")
            )
            .select(
                "normalized_order_id",
                F.upper(
                    F.trim(F.col("item.product_id"))
                ).alias("normalized_product_id")
            )
            .filter(
                F.col("normalized_product_id").isNotNull()
                & (F.col("normalized_product_id") != "")
            )
            .join(
                product_reference_df,
                F.col("normalized_product_id")
                == F.col("reference_product_id"),
                "left_anti"
            )
            .select("normalized_order_id")
            .distinct()
            .withColumn(
                "has_unknown_product",
                F.lit(True)
            )
        )

        latest_orders_df = (
            latest_orders_df
            .join(
                unknown_product_orders_df,
                "normalized_order_id",
                "left"
            )
            .withColumn(
                "has_unknown_product",
                F.coalesce(
                    F.col("has_unknown_product"),
                    F.lit(False)
                )
            )
        )


        # ---------------------------------------------------------
        # 6. Назначение основной ошибки
        # ---------------------------------------------------------

        validated_orders_df = (
            latest_orders_df
            .withColumn(
                "error_code",
                F.when(
                    F.col("_rescued_data").isNotNull(),
                    F.lit("RESCUED_DATA")
                )
                .when(
                    F.col("normalized_order_id").isNull()
                    | (F.col("normalized_order_id") == ""),
                    F.lit("MISSING_BUSINESS_KEY")
                )
                .when(
                    F.col("normalized_user_id").isNull()
                    | (F.col("normalized_user_id") == "")
                    | F.col("normalized_seller_id").isNull()
                    | (F.col("normalized_seller_id") == "")
                    | F.col("normalized_order_status").isNull()
                    | (F.col("normalized_order_status") == "")
                    | F.col("normalized_currency").isNull()
                    | (F.col("normalized_currency") == "")
                    | F.col("normalized_created_at").isNull()
                    | F.col("normalized_updated_at").isNull()
                    | F.col("items").isNull()
                    | (F.size(F.col("items")) == 0),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    ~F.col("normalized_schema_version").isin(
                        ALLOWED_SCHEMA_VERSIONS
                    ),
                    F.lit("INVALID_SCHEMA_VERSION")
                )
                .when(
                    ~F.col("normalized_order_status").isin(
                        ALLOWED_ORDER_STATUSES
                    ),
                    F.lit("INVALID_STATUS")
                )
                .when(
                    ~F.col("normalized_currency").isin(
                        ALLOWED_CURRENCIES
                    ),
                    F.lit("INVALID_CURRENCY")
                )
                .when(
                    (
                        F.col("normalized_schema_version") == 2
                    )
                    & (
                        F.col("normalized_sales_channel").isNull()
                        | (
                            F.col("normalized_sales_channel")
                            == ""
                        )
                        | F.col(
                            "normalized_delivery_priority"
                        ).isNull()
                        | (
                            F.col(
                                "normalized_delivery_priority"
                            ) == ""
                        )
                    ),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    F.col("normalized_sales_channel").isNotNull()
                    & (
                        F.col("normalized_sales_channel") != ""
                    )
                    & ~F.col("normalized_sales_channel").isin(
                        ALLOWED_SALES_CHANNELS
                    ),
                    F.lit("INVALID_SALES_CHANNEL")
                )
                .when(
                    F.col(
                        "normalized_delivery_priority"
                    ).isNotNull()
                    & (
                        F.col(
                            "normalized_delivery_priority"
                        ) != ""
                    )
                    & ~F.col(
                        "normalized_delivery_priority"
                    ).isin(
                        ALLOWED_DELIVERY_PRIORITIES
                    ),
                    F.lit("INVALID_DELIVERY_PRIORITY")
                )
                .when(
                    F.col("normalized_updated_at")
                    < F.col("normalized_created_at"),
                    F.lit("INVALID_TIME_SEQUENCE")
                )
                .when(
                    F.col("reference_seller_id").isNull(),
                    F.lit("UNKNOWN_REFERENCE")
                )
                .when(
                    F.col("has_unknown_product"),
                    F.lit("UNKNOWN_REFERENCE")
                )
                .when(
                    F.col("has_invalid_item"),
                    F.lit("INVALID_ORDER_ITEM")
                )
                .when(
                    F.col("has_line_amount_mismatch"),
                    F.lit("AMOUNT_MISMATCH")
                )
                .when(
                    F.col("normalized_total_quantity")
                    != F.col("calculated_total_quantity"),
                    F.lit("AMOUNT_MISMATCH")
                )
            )
            .withColumn(
                "error_message",
                F.when(
                    F.col("error_code") == "RESCUED_DATA",
                    F.lit("Order contains rescued data")
                )
                .when(
                    F.col("error_code")
                    == "MISSING_BUSINESS_KEY",
                    F.lit("order_id is null or empty")
                )
                .when(
                    F.col("error_code")
                    == "MISSING_REQUIRED_FIELD",
                    F.lit(
                        "One or more required order fields "
                        "are missing"
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_SCHEMA_VERSION",
                    F.concat(
                        F.lit("Unsupported schema version: "),
                        F.col(
                            "normalized_schema_version"
                        ).cast("string")
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_STATUS",
                    F.concat(
                        F.lit("Unsupported order status: "),
                        F.col("normalized_order_status")
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_CURRENCY",
                    F.concat(
                        F.lit("Unsupported currency: "),
                        F.col("normalized_currency")
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_SALES_CHANNEL",
                    F.concat(
                        F.lit("Unsupported sales channel: "),
                        F.col("normalized_sales_channel")
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_DELIVERY_PRIORITY",
                    F.concat(
                        F.lit("Unsupported delivery priority: "),
                        F.col(
                            "normalized_delivery_priority"
                        )
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_TIME_SEQUENCE",
                    F.lit(
                        "updated_at is earlier than created_at"
                    )
                )
                .when(
                    (F.col("error_code") == "UNKNOWN_REFERENCE")
                    & F.col("reference_seller_id").isNull(),
                    F.concat(
                        F.lit("Unknown seller_id: "),
                        F.col("normalized_seller_id")
                    )
                )
                .when(
                    F.col("error_code") == "UNKNOWN_REFERENCE",
                    F.lit(
                        "At least one product_id is unknown"
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_ORDER_ITEM",
                    F.lit(
                        "Order contains an invalid item"
                    )
                )
                .when(
                    F.col("error_code")
                    == "AMOUNT_MISMATCH",
                    F.lit(
                        "Order quantity or line amount "
                        "does not match calculated value"
                    )
                )
            )
        )


        # ---------------------------------------------------------
        # 7. Разделение результата
        # ---------------------------------------------------------

        invalid_latest_orders_df = (
            validated_orders_df
            .filter(F.col("error_code").isNotNull())
        )

        valid_latest_orders_df = (
            validated_orders_df
            .filter(F.col("error_code").isNull())
        )


        # ---------------------------------------------------------
        # 8. Контрольные показатели
        # ---------------------------------------------------------

        bronze_count = bronze_orders_df.count()
        latest_count = latest_orders_df.count()
        superseded_count = superseded_orders_df.count()
        valid_latest_count = valid_latest_orders_df.count()
        invalid_latest_count = invalid_latest_orders_df.count()

        reconciled_count = (
            superseded_count
            + valid_latest_count
            + invalid_latest_count
        )

        assert bronze_count == reconciled_count, (
            f"Orders reconciliation failed: "
            f"Bronze={bronze_count}, "
            f"valid latest={valid_latest_count}, "
            f"invalid latest={invalid_latest_count}, "
            f"superseded={superseded_count}"
        )




        print("Orders normalization and DQ completed")
        print(f"Bronze rows:             {bronze_count}")
        print(f"Latest order versions:   {latest_count}")
        print(f"Valid latest orders:     {valid_latest_count}")
        print(f"Invalid latest orders:   {invalid_latest_count}")
        print(f"Superseded versions:     {superseded_count}")
        print("Reconciliation status:   PASSED")

        display(
            validated_orders_df
            .groupBy(
                F.coalesce(
                    F.col("error_code"),
                    F.lit("VALID")
                ).alias("validation_result")
            )
            .count()
            .orderBy(
                F.desc("count"),
                "validation_result"
            )
        )

        display(
            invalid_latest_orders_df
            .select(
                "normalized_order_id",
                "error_code",
                "error_message",
                "_source_file",
                "_batch_id"
            )
            .limit(50)
        )


        # Этап 6. Silver Layer
        # Ячейка 7B. Запись Silver orders и order_items

        from pyspark.sql import functions as F


        SILVER_ORDERS_TABLE = (
            "marketplace_lab.silver.orders"
        )

        SILVER_ORDER_ITEMS_TABLE = (
            "marketplace_lab.silver.order_items"
        )

        QUARANTINE_TABLE = (
            "marketplace_lab.quarantine.invalid_records"
        )


        # ---------------------------------------------------------
        # 1. Создание Silver orders
        # ---------------------------------------------------------

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_ORDERS_TABLE}
        (
            order_id              STRING        NOT NULL,
            user_id               STRING        NOT NULL,
            seller_id             STRING        NOT NULL,
            order_status          STRING        NOT NULL,
            currency              STRING        NOT NULL,

            created_at            TIMESTAMP     NOT NULL,
            updated_at            TIMESTAMP     NOT NULL,

            total_quantity        BIGINT        NOT NULL,
            order_amount          DECIMAL(18,2) NOT NULL,

            sales_channel         STRING,
            coupon_code           STRING,
            delivery_priority     STRING,
            schema_version        INT           NOT NULL,

            record_hash           STRING        NOT NULL,

            source_batch_id       STRING,
            source_load_date      DATE,
            source_file           STRING,
            bronze_ingest_ts      TIMESTAMP,

            silver_created_at     TIMESTAMP     NOT NULL,
            silver_updated_at     TIMESTAMP     NOT NULL
        )
        USING DELTA
        COMMENT 'Current cleaned and deduplicated Marketplace orders'
        TBLPROPERTIES
        (
            'quality' = 'silver',
            'delta.enableChangeDataFeed' = 'true'
        )
        """)


        # ---------------------------------------------------------
        # 2. Создание Silver order_items
        # ---------------------------------------------------------

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_ORDER_ITEMS_TABLE}
        (
            order_item_id         STRING        NOT NULL,
            order_id              STRING        NOT NULL,
            line_number           INT           NOT NULL,

            product_id            STRING        NOT NULL,
            quantity              BIGINT        NOT NULL,
            unit_price            DECIMAL(18,2) NOT NULL,
            line_amount           DECIMAL(18,2) NOT NULL,
            currency              STRING        NOT NULL,

            record_hash           STRING        NOT NULL,

            source_batch_id       STRING,
            source_load_date      DATE,
            source_file           STRING,
            bronze_ingest_ts      TIMESTAMP,

            silver_created_at     TIMESTAMP     NOT NULL,
            silver_updated_at     TIMESTAMP     NOT NULL
        )
        USING DELTA
        COMMENT 'Normalized line items of current Marketplace orders'
        TBLPROPERTIES
        (
            'quality' = 'silver',
            'delta.enableChangeDataFeed' = 'true'
        )
        """)


        # ---------------------------------------------------------
        # 3. Подготовка quarantine для невалидных заказов
        # ---------------------------------------------------------

        order_quarantine_df = (
            invalid_latest_orders_df
            .withColumn(
                "business_key",
                F.col("normalized_order_id")
            )
            .withColumn(
                "quarantine_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("orders"),
                        F.coalesce(
                            F.col("normalized_order_id"),
                            F.lit("<NULL>")
                        ),
                        F.col("error_code"),
                        F.coalesce(
                            F.col("_source_file"),
                            F.lit("<NO_FILE>")
                        ),
                        F.coalesce(
                            F.col("_batch_id"),
                            F.lit("<NO_BATCH>")
                        ),
                        F.col("raw_record")
                    ),
                    256
                )
            )
            .select(
                "quarantine_id",
                F.lit("orders").alias("dataset"),
                "business_key",
                "error_code",
                "error_message",
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_batch_id").alias(
                    "batch_id"
                ),
                F.col("_load_date").alias(
                    "load_date"
                ),
                F.col("_ingest_ts").alias(
                    "original_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "quarantined_at"
                ),
                F.lit(CURRENT_RUN_ID).alias(
                    "processing_run_id"
                ),
                "raw_record"
            )
        )

        order_quarantine_df.createOrReplaceTempView(
            "incoming_order_quarantine"
        )

        spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS target
        USING incoming_order_quarantine AS source
        ON target.quarantine_id = source.quarantine_id

        WHEN MATCHED THEN UPDATE SET
            target.error_message =
                source.error_message,
            target.quarantined_at =
                source.quarantined_at,
            target.processing_run_id =
                source.processing_run_id

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 4. Подготовка заголовков Silver orders
        # ---------------------------------------------------------

        silver_orders_source_df = (
            valid_latest_orders_df
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_order_id"),
                        F.col("normalized_user_id"),
                        F.col("normalized_seller_id"),
                        F.col("normalized_order_status"),
                        F.col("normalized_currency"),
                        F.col(
                            "normalized_created_at"
                        ).cast("string"),
                        F.col(
                            "normalized_updated_at"
                        ).cast("string"),
                        F.col(
                            "calculated_total_quantity"
                        ).cast("string"),
                        F.col(
                            "calculated_order_amount"
                        ).cast("string"),
                        F.coalesce(
                            F.col("normalized_sales_channel"),
                            F.lit("<NULL>")
                        ),
                        F.coalesce(
                            F.col("normalized_coupon_code"),
                            F.lit("<NULL>")
                        ),
                        F.coalesce(
                            F.col(
                                "normalized_delivery_priority"
                            ),
                            F.lit("<NULL>")
                        ),
                        F.col(
                            "normalized_schema_version"
                        ).cast("string")
                    ),
                    256
                )
            )
            .select(
                F.col("normalized_order_id").alias(
                    "order_id"
                ),
                F.col("normalized_user_id").alias(
                    "user_id"
                ),
                F.col("normalized_seller_id").alias(
                    "seller_id"
                ),
                F.col("normalized_order_status").alias(
                    "order_status"
                ),
                F.col("normalized_currency").alias(
                    "currency"
                ),
                F.col("normalized_created_at").alias(
                    "created_at"
                ),
                F.col("normalized_updated_at").alias(
                    "updated_at"
                ),
                F.col(
                    "calculated_total_quantity"
                ).alias("total_quantity"),
                F.col(
                    "calculated_order_amount"
                ).alias("order_amount"),
                F.col("normalized_sales_channel").alias(
                    "sales_channel"
                ),
                F.col("normalized_coupon_code").alias(
                    "coupon_code"
                ),
                F.col(
                    "normalized_delivery_priority"
                ).alias("delivery_priority"),
                F.col(
                    "normalized_schema_version"
                ).alias("schema_version"),
                "record_hash",
                F.col("_batch_id").alias(
                    "source_batch_id"
                ),
                F.col("_load_date").alias(
                    "source_load_date"
                ),
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_ingest_ts").alias(
                    "bronze_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "silver_created_at"
                ),
                F.current_timestamp().alias(
                    "silver_updated_at"
                )
            )
        )

        silver_orders_source_df.createOrReplaceTempView(
            "incoming_silver_orders"
        )


        # ---------------------------------------------------------
        # 5. MERGE Silver orders
        #
        # Source содержит полный актуальный снимок заказов.
        # Поэтому NOT MATCHED BY SOURCE удаляет запись, если новая
        # актуальная версия стала невалидной.
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {SILVER_ORDERS_TABLE} AS target
        USING incoming_silver_orders AS source
        ON target.order_id = source.order_id

        WHEN MATCHED
         AND source.record_hash <> target.record_hash
        THEN UPDATE SET
            target.user_id =
                source.user_id,
            target.seller_id =
                source.seller_id,
            target.order_status =
                source.order_status,
            target.currency =
                source.currency,
            target.created_at =
                source.created_at,
            target.updated_at =
                source.updated_at,
            target.total_quantity =
                source.total_quantity,
            target.order_amount =
                source.order_amount,
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
            target.source_batch_id =
                source.source_batch_id,
            target.source_load_date =
                source.source_load_date,
            target.source_file =
                source.source_file,
            target.bronze_ingest_ts =
                source.bronze_ingest_ts,
            target.silver_updated_at =
                current_timestamp()

        WHEN NOT MATCHED THEN INSERT *

        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)


        # ---------------------------------------------------------
        # 6. Разворачивание items
        # ---------------------------------------------------------

        exploded_order_items_df = (
            valid_latest_orders_df
            .select(
                "normalized_order_id",
                "normalized_currency",
                "_batch_id",
                "_load_date",
                "_source_file",
                "_ingest_ts",
                F.posexplode("items").alias(
                    "item_position",
                    "item"
                )
            )
            .withColumn(
                "line_number",
                F.col("item_position") + F.lit(1)
            )
            .withColumn(
                "normalized_product_id",
                F.upper(
                    F.trim(
                        F.col("item.product_id")
                    )
                )
            )
            .withColumn(
                "normalized_quantity",
                F.col("item.quantity").cast("long")
            )
            .withColumn(
                "normalized_unit_price",
                F.col("item.unit_price").cast(
                    "decimal(18,2)"
                )
            )
            .withColumn(
                "normalized_line_amount",
                F.col("item.line_amount").cast(
                    "decimal(18,2)"
                )
            )
            .withColumn(
                "order_item_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_order_id"),
                        F.col("line_number").cast("string")
                    ),
                    256
                )
            )
        )


        # ---------------------------------------------------------
        # 7. Подготовка Silver order_items
        # ---------------------------------------------------------

        silver_order_items_source_df = (
            exploded_order_items_df
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_order_id"),
                        F.col("line_number").cast("string"),
                        F.col("normalized_product_id"),
                        F.col(
                            "normalized_quantity"
                        ).cast("string"),
                        F.col(
                            "normalized_unit_price"
                        ).cast("string"),
                        F.col(
                            "normalized_line_amount"
                        ).cast("string"),
                        F.col("normalized_currency")
                    ),
                    256
                )
            )
            .select(
                "order_item_id",
                F.col("normalized_order_id").alias(
                    "order_id"
                ),
                F.col("line_number").cast("int").alias(
                    "line_number"
                ),
                F.col("normalized_product_id").alias(
                    "product_id"
                ),
                F.col("normalized_quantity").alias(
                    "quantity"
                ),
                F.col("normalized_unit_price").alias(
                    "unit_price"
                ),
                F.col("normalized_line_amount").alias(
                    "line_amount"
                ),
                F.col("normalized_currency").alias(
                    "currency"
                ),
                "record_hash",
                F.col("_batch_id").alias(
                    "source_batch_id"
                ),
                F.col("_load_date").alias(
                    "source_load_date"
                ),
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_ingest_ts").alias(
                    "bronze_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "silver_created_at"
                ),
                F.current_timestamp().alias(
                    "silver_updated_at"
                )
            )
        )

        silver_order_items_source_df.createOrReplaceTempView(
            "incoming_silver_order_items"
        )


        # ---------------------------------------------------------
        # 8. MERGE Silver order_items
        #
        # Полный снимок позволяет удалить строки старой версии
        # заказа, которых больше нет в актуальном массиве items.
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {SILVER_ORDER_ITEMS_TABLE} AS target
        USING incoming_silver_order_items AS source
        ON target.order_item_id = source.order_item_id

        WHEN MATCHED
         AND source.record_hash <> target.record_hash
        THEN UPDATE SET
            target.order_id =
                source.order_id,
            target.line_number =
                source.line_number,
            target.product_id =
                source.product_id,
            target.quantity =
                source.quantity,
            target.unit_price =
                source.unit_price,
            target.line_amount =
                source.line_amount,
            target.currency =
                source.currency,
            target.record_hash =
                source.record_hash,
            target.source_batch_id =
                source.source_batch_id,
            target.source_load_date =
                source.source_load_date,
            target.source_file =
                source.source_file,
            target.bronze_ingest_ts =
                source.bronze_ingest_ts,
            target.silver_updated_at =
                current_timestamp()

        WHEN NOT MATCHED THEN INSERT *

        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)


        # ---------------------------------------------------------
        # 9. Финальная проверка
        # ---------------------------------------------------------

        bronze_count = bronze_orders_df.count()
        valid_order_count = valid_latest_orders_df.count()
        invalid_order_count = invalid_latest_orders_df.count()
        superseded_count = superseded_orders_df.count()

        silver_order_count = spark.table(
            SILVER_ORDERS_TABLE
        ).count()

        expected_item_count = (
            silver_order_items_source_df.count()
        )

        silver_item_count = spark.table(
            SILVER_ORDER_ITEMS_TABLE
        ).count()

        distinct_silver_orders = (
            spark.table(SILVER_ORDERS_TABLE)
            .select("order_id")
            .distinct()
            .count()
        )

        distinct_silver_items = (
            spark.table(SILVER_ORDER_ITEMS_TABLE)
            .select("order_item_id")
            .distinct()
            .count()
        )

        header_quantity = (
            spark.table(SILVER_ORDERS_TABLE)
            .agg(
                F.sum("total_quantity").alias("quantity")
            )
            .first()["quantity"]
        )

        item_quantity = (
            spark.table(SILVER_ORDER_ITEMS_TABLE)
            .agg(
                F.sum("quantity").alias("quantity")
            )
            .first()["quantity"]
        )

        current_quarantine_count = (
            spark.table(QUARANTINE_TABLE)
            .select("quarantine_id")
            .join(
                order_quarantine_df
                .select("quarantine_id")
                .distinct(),
                "quarantine_id",
                "inner"
            )
            .count()
        )


        assert bronze_count == (
            valid_order_count
            + invalid_order_count
            + superseded_count
        ), "Orders reconciliation failed"

        assert silver_order_count == valid_order_count, (
            f"Expected {valid_order_count} Silver orders, "
            f"found {silver_order_count}"
        )

        assert distinct_silver_orders == silver_order_count, (
            "Duplicate order_id detected in Silver"
        )

        assert silver_item_count == expected_item_count, (
            f"Expected {expected_item_count} Silver items, "
            f"found {silver_item_count}"
        )

        assert distinct_silver_items == silver_item_count, (
            "Duplicate order_item_id detected in Silver"
        )

        assert header_quantity == item_quantity, (
            f"Header quantity {header_quantity} does not match "
            f"item quantity {item_quantity}"
        )

        assert current_quarantine_count == invalid_order_count, (
            "Order quarantine count does not match "
            "current invalid order count"
        )


        # ---------------------------------------------------------
        # 10. Результат
        # ---------------------------------------------------------

        print("Silver orders pipeline completed")
        print(f"Bronze order versions:   {bronze_count}")
        print(f"Superseded versions:     {superseded_count}")
        print(f"Valid current orders:    {valid_order_count}")
        print(f"Invalid current orders:  {invalid_order_count}")
        print(f"Silver orders:           {silver_order_count}")
        print(f"Silver order items:      {silver_item_count}")
        print(f"Header item quantity:    {header_quantity}")
        print(f"Detail item quantity:    {item_quantity}")
        print(f"Current quarantine rows: {current_quarantine_count}")
        print("Reconciliation status:   PASSED")

        display(
            spark.table(SILVER_ORDERS_TABLE)
            .groupBy(
                "order_status",
                "currency",
                "schema_version"
            )
            .agg(
                F.count("*").alias("order_count"),
                F.sum("order_amount").alias(
                    "order_amount"
                )
            )
            .orderBy(
                "order_status",
                "currency",
                "schema_version"
            )
        )

        display(
            spark.table(SILVER_ORDER_ITEMS_TABLE)
            .orderBy(
                "order_id",
                "line_number"
            )
            .limit(100)
        )



    elif DATASET == 'payments':

        # Этап 6. Silver Layer
        # Ячейка 9. Bronze payments -> Silver payments + Quarantine

        from datetime import datetime, timezone

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window


        CURRENT_RUN_ID = globals().get(
            "RUN_ID",
            datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        )

        BRONZE_PAYMENTS_TABLE = (
            "marketplace_lab.bronze.payments_raw"
        )

        SILVER_PAYMENTS_TABLE = (
            "marketplace_lab.silver.payments"
        )

        SILVER_ORDERS_TABLE = (
            "marketplace_lab.silver.orders"
        )

        QUARANTINE_TABLE = (
            "marketplace_lab.quarantine.invalid_records"
        )

        DQ_RULE_CATALOG_TABLE = (
            "marketplace_lab.ops.data_quality_rule_catalog"
        )


        ALLOWED_PAYMENT_STATUSES = [
            "FAILED",
            "PENDING",
            "REFUNDED",
            "SUCCEEDED"
        ]

        ALLOWED_PAYMENT_METHODS = [
            "APPLE_PAY",
            "BANK_TRANSFER",
            "CARD",
            "PAYPAL"
        ]

        ALLOWED_PAYMENT_CURRENCIES = [
            "EUR",
            "GBP",
            "USD"
        ]


        # ---------------------------------------------------------
        # 1. Добавляем правило способа оплаты
        # ---------------------------------------------------------

        payment_rules_df = (
            spark.createDataFrame([
                {
                    "error_code": "INVALID_PAYMENT_METHOD",
                    "rule_category": "DOMAIN",
                    "severity": "ERROR",
                    "description": "Unsupported payment method",
                    "quarantine_required": True
                }
            ])
            .withColumn("created_at", F.current_timestamp())
        )

        payment_rules_df.createOrReplaceTempView(
            "incoming_payment_dq_rules"
        )

        spark.sql(f"""
        MERGE INTO {DQ_RULE_CATALOG_TABLE} AS target
        USING incoming_payment_dq_rules AS source
        ON target.error_code = source.error_code

        WHEN MATCHED THEN UPDATE SET
            target.rule_category = source.rule_category,
            target.severity = source.severity,
            target.description = source.description,
            target.quarantine_required =
                source.quarantine_required,
            target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT
        (
            error_code,
            rule_category,
            severity,
            description,
            quarantine_required,
            created_at,
            updated_at
        )
        VALUES
        (
            source.error_code,
            source.rule_category,
            source.severity,
            source.description,
            source.quarantine_required,
            source.created_at,
            NULL
        )
        """)


        # ---------------------------------------------------------
        # 2. Создание Silver payments
        # ---------------------------------------------------------

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_PAYMENTS_TABLE}
        (
            payment_id            STRING        NOT NULL,
            order_id              STRING        NOT NULL,
            payment_status        STRING        NOT NULL,
            payment_method        STRING        NOT NULL,
            amount                DECIMAL(18,2) NOT NULL,
            currency              STRING        NOT NULL,
            payment_time          TIMESTAMP     NOT NULL,

            was_recovered         BOOLEAN       NOT NULL,
            record_hash           STRING        NOT NULL,

            source_batch_id       STRING,
            source_load_date      DATE,
            source_file           STRING,
            bronze_ingest_ts      TIMESTAMP,

            silver_created_at     TIMESTAMP     NOT NULL,
            silver_updated_at     TIMESTAMP     NOT NULL
        )
        USING DELTA
        COMMENT 'Cleaned and deduplicated Marketplace payments'
        TBLPROPERTIES
        (
            'quality' = 'silver',
            'delta.enableChangeDataFeed' = 'true'
        )
        """)


        # ---------------------------------------------------------
        # 3. Нормализация и восстановление payment_time
        # ---------------------------------------------------------

        bronze_payments_df = read_bronze_source(
            BRONZE_PAYMENTS_TABLE
        )

        bronze_payment_columns = bronze_payments_df.columns

        normalized_payments_df = (
            bronze_payments_df
            .withColumn(
                "raw_record",
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name in bronze_payment_columns
                        ]
                    )
                )
            )
            .withColumn(
                "rescued_payment_time",
                F.get_json_object(
                    F.col("_rescued_data"),
                    "$.payment_time"
                )
            )
            .withColumn(
                "normalized_payment_id",
                F.upper(F.trim(F.col("payment_id")))
            )
            .withColumn(
                "normalized_order_id",
                F.upper(F.trim(F.col("order_id")))
            )
            .withColumn(
                "normalized_payment_status",
                F.upper(F.trim(F.col("payment_status")))
            )
            .withColumn(
                "normalized_payment_method",
                F.upper(F.trim(F.col("payment_method")))
            )
            .withColumn(
                "normalized_amount",
                F.col("amount").cast("decimal(18,2)")
            )
            .withColumn(
                "normalized_currency",
                F.upper(F.trim(F.col("currency")))
            )
            .withColumn(
                "normalized_payment_time",
                F.coalesce(
                    F.col("payment_time").cast("timestamp"),
                    F.to_timestamp(
                        F.col("rescued_payment_time"),
                        "dd/MM/yyyy HH:mm:ss"
                    ),
                    F.to_timestamp(
                        F.col("rescued_payment_time"),
                        "dd/MM/yyyy H:mm:ss"
                    )
                )
            )
            .withColumn(
                "was_recovered",
                F.col("payment_time").isNull()
                & F.col(
                    "normalized_payment_time"
                ).isNotNull()
            )
        )


        # ---------------------------------------------------------
        # 4. Выбор актуального статуса каждого payment_id
        # ---------------------------------------------------------

        normalized_payments_df = (
            normalized_payments_df
            .withColumn(
                "_dedup_partition_key",
                F.coalesce(
                    F.col("normalized_payment_id"),
                    F.sha2(F.col("raw_record"), 256)
                )
            )
        )

        payment_version_window = (
            Window
            .partitionBy("_dedup_partition_key")
            .orderBy(
                F.col("normalized_payment_time")
                .desc_nulls_last(),
                F.col("_ingest_ts").desc(),
                F.col("_source_file").desc()
            )
        )

        ranked_payments_df = (
            normalized_payments_df
            .withColumn(
                "_version_rank",
                F.row_number().over(payment_version_window)
            )
        )

        superseded_payments_df = (
            ranked_payments_df
            .filter(F.col("_version_rank") > 1)
        )

        latest_payments_df = (
            ranked_payments_df
            .filter(F.col("_version_rank") == 1)
        )


        # ---------------------------------------------------------
        # 5. Referential integrity: payment -> Silver order
        # ---------------------------------------------------------

        silver_order_keys_df = (
            spark.table(SILVER_ORDERS_TABLE)
            .select(
                F.col("order_id").alias(
                    "reference_order_id"
                )
            )
            .distinct()
        )

        latest_payments_df = (
            latest_payments_df
            .join(
                silver_order_keys_df,
                latest_payments_df["normalized_order_id"]
                == silver_order_keys_df["reference_order_id"],
                "left"
            )
        )


        # ---------------------------------------------------------
        # 6. Data-quality проверки
        # ---------------------------------------------------------

        validated_payments_df = (
            latest_payments_df
            .withColumn(
                "error_code",
                F.when(
                    F.col("_rescued_data").isNotNull()
                    & F.col(
                        "normalized_payment_time"
                    ).isNull(),
                    F.lit("MALFORMED_RECORD")
                )
                .when(
                    F.col("normalized_payment_id").isNull()
                    | (F.col("normalized_payment_id") == ""),
                    F.lit("MISSING_BUSINESS_KEY")
                )
                .when(
                    F.col("normalized_order_id").isNull()
                    | (F.col("normalized_order_id") == "")
                    | F.col(
                        "normalized_payment_status"
                    ).isNull()
                    | (
                        F.col(
                            "normalized_payment_status"
                        ) == ""
                    )
                    | F.col(
                        "normalized_payment_method"
                    ).isNull()
                    | (
                        F.col(
                            "normalized_payment_method"
                        ) == ""
                    )
                    | F.col("normalized_amount").isNull()
                    | F.col("normalized_currency").isNull()
                    | (F.col("normalized_currency") == "")
                    | F.col(
                        "normalized_payment_time"
                    ).isNull(),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    ~F.col(
                        "normalized_payment_status"
                    ).isin(ALLOWED_PAYMENT_STATUSES),
                    F.lit("INVALID_STATUS")
                )
                .when(
                    ~F.col(
                        "normalized_payment_method"
                    ).isin(ALLOWED_PAYMENT_METHODS),
                    F.lit("INVALID_PAYMENT_METHOD")
                )
                .when(
                    ~F.col("normalized_currency").isin(
                        ALLOWED_PAYMENT_CURRENCIES
                    ),
                    F.lit("INVALID_CURRENCY")
                )
                .when(
                    F.col("normalized_amount") < F.lit(0),
                    F.lit("NEGATIVE_AMOUNT")
                )
                .when(
                    F.col("reference_order_id").isNull(),
                    F.lit("UNKNOWN_REFERENCE")
                )
            )
            .withColumn(
                "error_message",
                F.when(
                    F.col("error_code") == "MALFORMED_RECORD",
                    F.lit(
                        "CSV record cannot be fully recovered"
                    )
                )
                .when(
                    F.col("error_code")
                    == "MISSING_BUSINESS_KEY",
                    F.lit("payment_id is null or empty")
                )
                .when(
                    F.col("error_code")
                    == "MISSING_REQUIRED_FIELD",
                    F.lit(
                        "One or more required payment fields "
                        "are missing"
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_STATUS",
                    F.concat(
                        F.lit("Unsupported payment status: "),
                        F.col("normalized_payment_status"),
                        F.when(
                            F.col("reference_order_id").isNull(),
                            F.concat(
                                F.lit(" | Unknown order_id: "),
                                F.col("normalized_order_id")
                            )
                        ).otherwise(F.lit(""))
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_PAYMENT_METHOD",
                    F.concat(
                        F.lit("Unsupported payment method: "),
                        F.col("normalized_payment_method")
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_CURRENCY",
                    F.concat(
                        F.lit("Unsupported currency: "),
                        F.col("normalized_currency")
                    )
                )
                .when(
                    F.col("error_code") == "NEGATIVE_AMOUNT",
                    F.concat(
                        F.lit("Negative payment amount: "),
                        F.col(
                            "normalized_amount"
                        ).cast("string")
                    )
                )
                .when(
                    F.col("error_code")
                    == "UNKNOWN_REFERENCE",
                    F.concat(
                        F.lit("Unknown order_id: "),
                        F.col("normalized_order_id")
                    )
                )
            )
        )


        # ---------------------------------------------------------
        # 7. Валидные и quarantine-записи
        # ---------------------------------------------------------

        invalid_latest_payments_df = (
            validated_payments_df
            .filter(F.col("error_code").isNotNull())
        )

        valid_latest_payments_df = (
            validated_payments_df
            .filter(F.col("error_code").isNull())
        )


        payment_quarantine_df = (
            invalid_latest_payments_df
            .withColumn(
                "business_key",
                F.col("normalized_payment_id")
            )
            .withColumn(
                "quarantine_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("payments"),
                        F.coalesce(
                            F.col("normalized_payment_id"),
                            F.lit("<NULL>")
                        ),
                        F.col("error_code"),
                        F.coalesce(
                            F.col("_source_file"),
                            F.lit("<NO_FILE>")
                        ),
                        F.coalesce(
                            F.col("_batch_id"),
                            F.lit("<NO_BATCH>")
                        ),
                        F.col("raw_record")
                    ),
                    256
                )
            )
            .select(
                "quarantine_id",
                F.lit("payments").alias("dataset"),
                "business_key",
                "error_code",
                "error_message",
                F.col("_source_file").alias("source_file"),
                F.col("_batch_id").alias("batch_id"),
                F.col("_load_date").alias("load_date"),
                F.col("_ingest_ts").alias(
                    "original_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "quarantined_at"
                ),
                F.lit(CURRENT_RUN_ID).alias(
                    "processing_run_id"
                ),
                "raw_record"
            )
        )

        payment_quarantine_df.createOrReplaceTempView(
            "incoming_payment_quarantine"
        )

        spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS target
        USING incoming_payment_quarantine AS source
        ON target.quarantine_id = source.quarantine_id

        WHEN MATCHED THEN UPDATE SET
            target.error_message =
                source.error_message,
            target.quarantined_at =
                source.quarantined_at,
            target.processing_run_id =
                source.processing_run_id

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 8. Подготовка Silver payments
        # ---------------------------------------------------------

        silver_payments_source_df = (
            valid_latest_payments_df
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_payment_id"),
                        F.col("normalized_order_id"),
                        F.col("normalized_payment_status"),
                        F.col("normalized_payment_method"),
                        F.col(
                            "normalized_amount"
                        ).cast("string"),
                        F.col("normalized_currency"),
                        F.col(
                            "normalized_payment_time"
                        ).cast("string")
                    ),
                    256
                )
            )
            .select(
                F.col("normalized_payment_id").alias(
                    "payment_id"
                ),
                F.col("normalized_order_id").alias(
                    "order_id"
                ),
                F.col(
                    "normalized_payment_status"
                ).alias("payment_status"),
                F.col(
                    "normalized_payment_method"
                ).alias("payment_method"),
                F.col("normalized_amount").alias("amount"),
                F.col("normalized_currency").alias(
                    "currency"
                ),
                F.col(
                    "normalized_payment_time"
                ).alias("payment_time"),
                "was_recovered",
                "record_hash",
                F.col("_batch_id").alias(
                    "source_batch_id"
                ),
                F.col("_load_date").alias(
                    "source_load_date"
                ),
                F.col("_source_file").alias(
                    "source_file"
                ),
                F.col("_ingest_ts").alias(
                    "bronze_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "silver_created_at"
                ),
                F.current_timestamp().alias(
                    "silver_updated_at"
                )
            )
        )

        silver_payments_source_df.createOrReplaceTempView(
            "incoming_silver_payments"
        )


        # ---------------------------------------------------------
        # 9. Идемпотентный MERGE
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {SILVER_PAYMENTS_TABLE} AS target
        USING incoming_silver_payments AS source
        ON target.payment_id = source.payment_id

        WHEN MATCHED
         AND source.record_hash <> target.record_hash
        THEN UPDATE SET
            target.order_id =
                source.order_id,
            target.payment_status =
                source.payment_status,
            target.payment_method =
                source.payment_method,
            target.amount =
                source.amount,
            target.currency =
                source.currency,
            target.payment_time =
                source.payment_time,
            target.was_recovered =
                source.was_recovered,
            target.record_hash =
                source.record_hash,
            target.source_batch_id =
                source.source_batch_id,
            target.source_load_date =
                source.source_load_date,
            target.source_file =
                source.source_file,
            target.bronze_ingest_ts =
                source.bronze_ingest_ts,
            target.silver_updated_at =
                current_timestamp()

        WHEN NOT MATCHED THEN INSERT *

        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)


        # ---------------------------------------------------------
        # 10. Проверка результата
        # ---------------------------------------------------------

        bronze_count = bronze_payments_df.count()
        superseded_count = superseded_payments_df.count()
        valid_count = valid_latest_payments_df.count()
        invalid_count = invalid_latest_payments_df.count()

        silver_count = spark.table(
            SILVER_PAYMENTS_TABLE
        ).count()

        current_quarantine_count = (
            spark.table(QUARANTINE_TABLE)
            .select("quarantine_id")
            .join(
                payment_quarantine_df
                .select("quarantine_id")
                .distinct(),
                "quarantine_id",
                "inner"
            )
            .count()
        )

        assert bronze_count == (
            superseded_count + valid_count + invalid_count
        ), "Payments reconciliation failed"

        assert silver_count == valid_count, (
            f"Expected {valid_count} Silver payments, "
            f"found {silver_count}"
        )

        assert current_quarantine_count == invalid_count, (
            f"Expected {invalid_count} current quarantine rows, "
            f"found {current_quarantine_count}"
        )


        print("Silver payments pipeline completed")
        print(f"Bronze payment rows:     {bronze_count}")
        print(f"Superseded versions:     {superseded_count}")
        print(f"Valid Silver payments:   {valid_count}")
        print(f"Invalid payments:        {invalid_count}")
        print(f"Silver table rows:       {silver_count}")
        print(f"Current quarantine rows: {current_quarantine_count}")
        print("Reconciliation status:   PASSED")

        display(
            invalid_latest_payments_df
            .groupBy("error_code")
            .agg(
                F.count("*").alias("row_count")
            )
            .orderBy(F.desc("row_count"))
        )

        display(
            spark.table(SILVER_PAYMENTS_TABLE)
            .groupBy(
                "payment_status",
                "payment_method",
                "currency"
            )
            .agg(
                F.count("*").alias("payment_count"),
                F.sum("amount").alias("payment_amount")
            )
            .orderBy(
                "payment_status",
                "payment_method",
                "currency"
            )
        )



    elif DATASET == 'user_events':

        # Этап 6. Silver Layer
        # Ячейка 11. Bronze user_events -> Silver + Quarantine

        from datetime import datetime, timezone

        from pyspark.sql import functions as F
        from pyspark.sql.window import Window


        spark.conf.set(
            "spark.sql.session.timeZone",
            "UTC"
        )

        CURRENT_RUN_ID = globals().get(
            "RUN_ID",
            datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        )

        BRONZE_EVENTS_TABLE = (
            "marketplace_lab.bronze.user_events_raw"
        )

        SILVER_EVENTS_TABLE = (
            "marketplace_lab.silver.user_events"
        )

        SILVER_PRODUCTS_TABLE = (
            "marketplace_lab.silver.products"
        )

        SILVER_ORDERS_TABLE = (
            "marketplace_lab.silver.orders"
        )

        QUARANTINE_TABLE = (
            "marketplace_lab.quarantine.invalid_records"
        )

        DQ_RULE_CATALOG_TABLE = (
            "marketplace_lab.ops.data_quality_rule_catalog"
        )


        ALLOWED_EVENT_TYPES = [
            "user_registered",
            "product_viewed",
            "added_to_cart",
            "order_created",
            "payment_succeeded",
            "payment_failed",
            "order_cancelled"
        ]

        ALLOWED_EVENT_CURRENCIES = [
            "EUR",
            "GBP",
            "USD"
        ]

        PRODUCT_REQUIRED_EVENTS = [
            "product_viewed",
            "added_to_cart",
            "order_created"
        ]

        ORDER_REQUIRED_EVENTS = [
            "order_created",
            "payment_succeeded",
            "payment_failed",
            "order_cancelled"
        ]


        # ---------------------------------------------------------
        # 1. Специфичные правила user_events
        # ---------------------------------------------------------

        event_rules_df = (
            spark.createDataFrame([
                {
                    "error_code": "INVALID_EVENT_TYPE",
                    "rule_category": "DOMAIN",
                    "severity": "ERROR",
                    "description": "Unsupported user event type",
                    "quarantine_required": True
                },
                {
                    "error_code": "INVALID_EVENT_VALUE",
                    "rule_category": "BUSINESS",
                    "severity": "ERROR",
                    "description": (
                        "Event contains an invalid price "
                        "or quantity"
                    ),
                    "quarantine_required": True
                }
            ])
            .withColumn("created_at", F.current_timestamp())
        )

        event_rules_df.createOrReplaceTempView(
            "incoming_event_dq_rules"
        )

        spark.sql(f"""
        MERGE INTO {DQ_RULE_CATALOG_TABLE} AS target
        USING incoming_event_dq_rules AS source
        ON target.error_code = source.error_code

        WHEN MATCHED THEN UPDATE SET
            target.rule_category = source.rule_category,
            target.severity = source.severity,
            target.description = source.description,
            target.quarantine_required =
                source.quarantine_required,
            target.updated_at = current_timestamp()

        WHEN NOT MATCHED THEN INSERT
        (
            error_code,
            rule_category,
            severity,
            description,
            quarantine_required,
            created_at,
            updated_at
        )
        VALUES
        (
            source.error_code,
            source.rule_category,
            source.severity,
            source.description,
            source.quarantine_required,
            source.created_at,
            NULL
        )
        """)


        # ---------------------------------------------------------
        # 2. Создание Silver user_events
        # ---------------------------------------------------------

        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_EVENTS_TABLE}
        (
            event_id               STRING        NOT NULL,
            event_type             STRING        NOT NULL,
            event_time_utc         TIMESTAMP     NOT NULL,
            ingestion_time_utc     TIMESTAMP     NOT NULL,

            user_id                STRING        NOT NULL,
            session_id             STRING        NOT NULL,
            product_id             STRING,
            order_id               STRING,

            price                  DECIMAL(18,2),
            currency               STRING,
            quantity               BIGINT,

            country_code           STRING        NOT NULL,
            device_type            STRING        NOT NULL,

            arrival_delay_seconds  BIGINT        NOT NULL,
            is_late                BOOLEAN       NOT NULL,

            record_hash            STRING        NOT NULL,

            source_batch_id        STRING,
            source_load_date       DATE,
            source_file            STRING,
            bronze_ingest_ts       TIMESTAMP,

            silver_created_at      TIMESTAMP     NOT NULL,
            silver_updated_at      TIMESTAMP     NOT NULL
        )
        USING DELTA
        COMMENT 'Cleaned and deduplicated Marketplace user events'
        TBLPROPERTIES
        (
            'quality' = 'silver',
            'delta.enableChangeDataFeed' = 'true'
        )
        """)


        # ---------------------------------------------------------
        # 3. Нормализация Bronze
        # ---------------------------------------------------------

        bronze_events_df = read_bronze_source(
            BRONZE_EVENTS_TABLE
        )

        bronze_event_columns = bronze_events_df.columns

        normalized_events_df = (
            bronze_events_df
            .withColumn(
                "raw_record",
                F.to_json(
                    F.struct(
                        *[
                            F.col(column_name)
                            for column_name in bronze_event_columns
                        ]
                    )
                )
            )
            .withColumn(
                "normalized_event_id",
                F.upper(F.trim(F.col("event_id")))
            )
            .withColumn(
                "normalized_event_type",
                F.lower(F.trim(F.col("event_type")))
            )
            .withColumn(
                "normalized_user_id",
                F.upper(F.trim(F.col("user_id")))
            )
            .withColumn(
                "normalized_session_id",
                F.upper(F.trim(F.col("session_id")))
            )
            .withColumn(
                "normalized_product_id",
                F.upper(F.trim(F.col("product_id")))
            )
            .withColumn(
                "normalized_order_id",
                F.upper(F.trim(F.col("order_id")))
            )
            .withColumn(
                "normalized_price",
                F.col("price").cast("decimal(18,2)")
            )
            .withColumn(
                "normalized_currency",
                F.upper(F.trim(F.col("currency")))
            )
            .withColumn(
                "normalized_quantity",
                F.col("quantity").cast("long")
            )
            .withColumn(
                "normalized_country",
                F.upper(F.trim(F.col("country")))
            )
            .withColumn(
                "normalized_device",
                F.upper(F.trim(F.col("device")))
            )
            .withColumn(
                "event_time_utc",
                F.col("event_time").cast("timestamp")
            )
            .withColumn(
                "ingestion_time_utc",
                F.col("ingestion_time").cast("timestamp")
            )
            .withColumn(
                "arrival_delay_seconds",
                F.col("ingestion_time_utc").cast("long")
                - F.col("event_time_utc").cast("long")
            )
        )


        # Используем исходный is_late, если генератор сохранил его.
        # Иначе используем технический порог 24 часа.
        if "is_late" in bronze_events_df.columns:
            normalized_events_df = (
                normalized_events_df
                .withColumn(
                    "normalized_is_late",
                    F.coalesce(
                        F.col("is_late").cast("boolean"),
                        F.lit(False)
                    )
                )
            )
        else:
            normalized_events_df = (
                normalized_events_df
                .withColumn(
                    "normalized_is_late",
                    F.coalesce(
                        F.col("arrival_delay_seconds")
                        > F.lit(7 * 24 * 60 * 60),
                        F.lit(False)
                    )
                )
            )


        # ---------------------------------------------------------
        # 4. Дедупликация по максимальному ingestion_time
        #
        # Для NULL event_id используется hash исходной строки,
        # поэтому malformed-записи не схлопываются друг с другом.
        # ---------------------------------------------------------

        normalized_events_df = (
            normalized_events_df
            .withColumn(
                "_dedup_partition_key",
                F.coalesce(
                    F.col("normalized_event_id"),
                    F.sha2(F.col("raw_record"), 256)
                )
            )
        )

        event_version_window = (
            Window
            .partitionBy("_dedup_partition_key")
            .orderBy(
                F.col("ingestion_time_utc")
                .desc_nulls_last(),
                F.col("_ingest_ts").desc(),
                F.col("_source_file").desc()
            )
        )

        ranked_events_df = (
            normalized_events_df
            .withColumn(
                "_version_rank",
                F.row_number().over(event_version_window)
            )
        )

        superseded_events_df = (
            ranked_events_df
            .filter(F.col("_version_rank") > 1)
        )

        latest_events_df = (
            ranked_events_df
            .filter(F.col("_version_rank") == 1)
        )


        # ---------------------------------------------------------
        # 5. Referential integrity
        # ---------------------------------------------------------

        product_reference_df = (
            spark.table(SILVER_PRODUCTS_TABLE)
            .select(
                F.col("product_id").alias(
                    "reference_product_id"
                )
            )
            .distinct()
        )

        order_reference_df = (
            spark.table(SILVER_ORDERS_TABLE)
            .select(
                F.col("order_id").alias(
                    "reference_order_id"
                )
            )
            .distinct()
        )

        latest_events_df = (
            latest_events_df
            .join(
                product_reference_df,
                latest_events_df["normalized_product_id"]
                == product_reference_df["reference_product_id"],
                "left"
            )
            .join(
                order_reference_df,
                latest_events_df["normalized_order_id"]
                == order_reference_df["reference_order_id"],
                "left"
            )
        )


        # ---------------------------------------------------------
        # 6. Data-quality проверки
        # ---------------------------------------------------------

        validated_events_df = (
            latest_events_df
            .withColumn(
                "error_code",
                F.when(
                    F.col("_rescued_data").isNotNull(),
                    F.lit("MALFORMED_RECORD")
                )
                .when(
                    F.col("normalized_event_id").isNull()
                    | (F.col("normalized_event_id") == ""),
                    F.lit("MISSING_BUSINESS_KEY")
                )
                .when(
                    F.col("normalized_event_type").isNull()
                    | (F.col("normalized_event_type") == "")
                    | F.col("normalized_user_id").isNull()
                    | (F.col("normalized_user_id") == "")
                    | F.col("normalized_session_id").isNull()
                    | (F.col("normalized_session_id") == "")
                    | F.col("event_time_utc").isNull()
                    | F.col("ingestion_time_utc").isNull()
                    | F.col("normalized_country").isNull()
                    | (F.col("normalized_country") == "")
                    | F.col("normalized_device").isNull()
                    | (F.col("normalized_device") == ""),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    ~F.col("normalized_event_type").isin(
                        ALLOWED_EVENT_TYPES
                    ),
                    F.lit("INVALID_EVENT_TYPE")
                )
                .when(
                    F.col("normalized_event_type").isin(
                        PRODUCT_REQUIRED_EVENTS
                    )
                    & (
                        F.col("normalized_product_id").isNull()
                        | (
                            F.col("normalized_product_id")
                            == ""
                        )
                    ),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    F.col("normalized_event_type").isin(
                        ORDER_REQUIRED_EVENTS
                    )
                    & (
                        F.col("normalized_order_id").isNull()
                        | (
                            F.col("normalized_order_id")
                            == ""
                        )
                    ),
                    F.lit("MISSING_REQUIRED_FIELD")
                )
                .when(
                    F.col("normalized_product_id").isNotNull()
                    & (
                        F.col("normalized_product_id") != ""
                    )
                    & F.col("reference_product_id").isNull(),
                    F.lit("UNKNOWN_REFERENCE")
                )
                .when(
                    F.col("normalized_order_id").isNotNull()
                    & (
                        F.col("normalized_order_id") != ""
                    )
                    & F.col("reference_order_id").isNull(),
                    F.lit("UNKNOWN_REFERENCE")
                )
                .when(
                    F.col("normalized_currency").isNotNull()
                    & (
                        F.col("normalized_currency") != ""
                    )
                    & ~F.col("normalized_currency").isin(
                        ALLOWED_EVENT_CURRENCIES
                    ),
                    F.lit("INVALID_CURRENCY")
                )
                .when(
                    (F.col("normalized_price") < F.lit(0))
                    | (
                        F.col("normalized_quantity").isNotNull()
                        & (
                            F.col("normalized_quantity")
                            <= F.lit(0)
                        )
                    ),
                    F.lit("INVALID_EVENT_VALUE")
                )
                .when(
                    F.col("event_time_utc")
                    > F.col("ingestion_time_utc"),
                    F.lit("INVALID_TIME_SEQUENCE")
                )
            )
            .withColumn(
                "error_message",
                F.when(
                    F.col("error_code") == "MALFORMED_RECORD",
                    F.lit(
                        "JSON event cannot be parsed reliably"
                    )
                )
                .when(
                    F.col("error_code")
                    == "MISSING_BUSINESS_KEY",
                    F.lit("event_id is null or empty")
                )
                .when(
                    F.col("error_code")
                    == "MISSING_REQUIRED_FIELD",
                    F.lit(
                        "One or more required event fields "
                        "are missing"
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_EVENT_TYPE",
                    F.concat(
                        F.lit("Unsupported event type: "),
                        F.col("normalized_event_type")
                    )
                )
                .when(
                    (F.col("error_code") == "UNKNOWN_REFERENCE")
                    & F.col("reference_product_id").isNull()
                    & F.col("normalized_product_id").isNotNull(),
                    F.concat(
                        F.lit("Unknown product_id: "),
                        F.col("normalized_product_id")
                    )
                )
                .when(
                    F.col("error_code") == "UNKNOWN_REFERENCE",
                    F.concat(
                        F.lit("Unknown order_id: "),
                        F.col("normalized_order_id")
                    )
                )
                .when(
                    F.col("error_code") == "INVALID_CURRENCY",
                    F.concat(
                        F.lit("Unsupported currency: "),
                        F.col("normalized_currency")
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_EVENT_VALUE",
                    F.lit(
                        "Event price is negative or "
                        "quantity is not positive"
                    )
                )
                .when(
                    F.col("error_code")
                    == "INVALID_TIME_SEQUENCE",
                    F.lit(
                        "event_time is later than ingestion_time"
                    )
                )
            )
        )


        # ---------------------------------------------------------
        # 7. Разделение результата
        # ---------------------------------------------------------

        invalid_latest_events_df = (
            validated_events_df
            .filter(F.col("error_code").isNotNull())
        )

        valid_latest_events_df = (
            validated_events_df
            .filter(F.col("error_code").isNull())
        )


        # ---------------------------------------------------------
        # 8. Quarantine
        # ---------------------------------------------------------

        event_quarantine_df = (
            invalid_latest_events_df
            .withColumn(
                "business_key",
                F.col("normalized_event_id")
            )
            .withColumn(
                "quarantine_id",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.lit("user_events"),
                        F.coalesce(
                            F.col("normalized_event_id"),
                            F.lit("<NULL>")
                        ),
                        F.col("error_code"),
                        F.coalesce(
                            F.col("_source_file"),
                            F.lit("<NO_FILE>")
                        ),
                        F.coalesce(
                            F.col("_batch_id"),
                            F.lit("<NO_BATCH>")
                        ),
                        F.col("raw_record")
                    ),
                    256
                )
            )
            .select(
                "quarantine_id",
                F.lit("user_events").alias("dataset"),
                "business_key",
                "error_code",
                "error_message",
                F.col("_source_file").alias("source_file"),
                F.col("_batch_id").alias("batch_id"),
                F.col("_load_date").alias("load_date"),
                F.col("_ingest_ts").alias(
                    "original_ingest_ts"
                ),
                F.current_timestamp().alias(
                    "quarantined_at"
                ),
                F.lit(CURRENT_RUN_ID).alias(
                    "processing_run_id"
                ),
                "raw_record"
            )
        )

        event_quarantine_df.createOrReplaceTempView(
            "incoming_event_quarantine"
        )

        spark.sql(f"""
        MERGE INTO {QUARANTINE_TABLE} AS target
        USING incoming_event_quarantine AS source
        ON target.quarantine_id = source.quarantine_id

        WHEN MATCHED THEN UPDATE SET
            target.error_message =
                source.error_message,
            target.quarantined_at =
                source.quarantined_at,
            target.processing_run_id =
                source.processing_run_id

        WHEN NOT MATCHED THEN INSERT *
        """)


        # ---------------------------------------------------------
        # 9. Подготовка Silver
        # ---------------------------------------------------------

        silver_events_source_df = (
            valid_latest_events_df
            .withColumn(
                "record_hash",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("normalized_event_id"),
                        F.col("normalized_event_type"),
                        F.col("event_time_utc").cast("string"),
                        F.col("ingestion_time_utc").cast("string"),
                        F.col("normalized_user_id"),
                        F.col("normalized_session_id"),
                        F.coalesce(
                            F.col("normalized_product_id"),
                            F.lit("<NULL>")
                        ),
                        F.coalesce(
                            F.col("normalized_order_id"),
                            F.lit("<NULL>")
                        ),
                        F.coalesce(
                            F.col("normalized_price").cast("string"),
                            F.lit("<NULL>")
                        ),
                        F.coalesce(
                            F.col("normalized_currency"),
                            F.lit("<NULL>")
                        ),
                        F.coalesce(
                            F.col("normalized_quantity").cast("string"),
                            F.lit("<NULL>")
                        ),
                        F.col("normalized_country"),
                        F.col("normalized_device"),
                        F.col(
                            "arrival_delay_seconds"
                        ).cast("string"),
                        F.col(
                            "normalized_is_late"
                        ).cast("string")
                    ),
                    256
                )
            )
            .select(
                F.col("normalized_event_id").alias("event_id"),
                F.col("normalized_event_type").alias(
                    "event_type"
                ),
                "event_time_utc",
                "ingestion_time_utc",
                F.col("normalized_user_id").alias("user_id"),
                F.col("normalized_session_id").alias(
                    "session_id"
                ),
                F.col("normalized_product_id").alias(
                    "product_id"
                ),
                F.col("normalized_order_id").alias("order_id"),
                F.col("normalized_price").alias("price"),
                F.col("normalized_currency").alias("currency"),
                F.col("normalized_quantity").alias("quantity"),
                F.col("normalized_country").alias(
                    "country_code"
                ),
                F.col("normalized_device").alias("device_type"),
                F.col("arrival_delay_seconds").cast("long").alias(
                    "arrival_delay_seconds"
                ),
                F.col("normalized_is_late").alias("is_late"),
                "record_hash",
                F.col("_batch_id").alias("source_batch_id"),
                F.col("_load_date").alias("source_load_date"),
                F.col("_source_file").alias("source_file"),
                F.col("_ingest_ts").alias("bronze_ingest_ts"),
                F.current_timestamp().alias(
                    "silver_created_at"
                ),
                F.current_timestamp().alias(
                    "silver_updated_at"
                )
            )
        )

        silver_events_source_df.createOrReplaceTempView(
            "incoming_silver_events"
        )


        # ---------------------------------------------------------
        # 10. Идемпотентный MERGE
        # ---------------------------------------------------------

        spark.sql(f"""
        MERGE INTO {SILVER_EVENTS_TABLE} AS target
        USING incoming_silver_events AS source
        ON target.event_id = source.event_id

        WHEN MATCHED
         AND source.record_hash <> target.record_hash
        THEN UPDATE SET
            target.event_type = source.event_type,
            target.event_time_utc = source.event_time_utc,
            target.ingestion_time_utc =
                source.ingestion_time_utc,
            target.user_id = source.user_id,
            target.session_id = source.session_id,
            target.product_id = source.product_id,
            target.order_id = source.order_id,
            target.price = source.price,
            target.currency = source.currency,
            target.quantity = source.quantity,
            target.country_code = source.country_code,
            target.device_type = source.device_type,
            target.arrival_delay_seconds =
                source.arrival_delay_seconds,
            target.is_late = source.is_late,
            target.record_hash = source.record_hash,
            target.source_batch_id = source.source_batch_id,
            target.source_load_date = source.source_load_date,
            target.source_file = source.source_file,
            target.bronze_ingest_ts =
                source.bronze_ingest_ts,
            target.silver_updated_at =
                current_timestamp()

        WHEN NOT MATCHED THEN INSERT *

        WHEN NOT MATCHED BY SOURCE THEN DELETE
        """)


        # ---------------------------------------------------------
        # 11. Reconciliation
        # ---------------------------------------------------------

        bronze_count = bronze_events_df.count()
        superseded_count = superseded_events_df.count()
        valid_count = valid_latest_events_df.count()
        invalid_count = invalid_latest_events_df.count()

        silver_count = spark.table(
            SILVER_EVENTS_TABLE
        ).count()

        distinct_silver_event_count = (
            spark.table(SILVER_EVENTS_TABLE)
            .select("event_id")
            .distinct()
            .count()
        )

        late_event_count = (
            spark.table(SILVER_EVENTS_TABLE)
            .filter(F.col("is_late"))
            .count()
        )

        current_quarantine_count = (
            spark.table(QUARANTINE_TABLE)
            .select("quarantine_id")
            .join(
                event_quarantine_df
                .select("quarantine_id")
                .distinct(),
                "quarantine_id",
                "inner"
            )
            .count()
        )

        assert bronze_count == (
            superseded_count + valid_count + invalid_count
        ), "User-events reconciliation failed"

        assert silver_count == valid_count, (
            f"Expected {valid_count} Silver events, "
            f"found {silver_count}"
        )

        assert distinct_silver_event_count == silver_count, (
            "Duplicate event_id detected in Silver"
        )

        assert current_quarantine_count == invalid_count, (
            f"Expected {invalid_count} current quarantine rows, "
            f"found {current_quarantine_count}"
        )


        # ---------------------------------------------------------
        # 12. Результат
        # ---------------------------------------------------------

        print("Silver user_events pipeline completed")
        print(f"Bronze event rows:       {bronze_count}")
        print(f"Superseded duplicates:   {superseded_count}")
        print(f"Valid Silver events:     {valid_count}")
        print(f"Invalid latest events:   {invalid_count}")
        print(f"Silver table rows:       {silver_count}")
        print(f"Late Silver events:      {late_event_count}")
        print(f"Current quarantine rows: {current_quarantine_count}")
        print("Reconciliation status:   PASSED")

        display(
            invalid_latest_events_df
            .groupBy("error_code")
            .count()
            .orderBy(F.desc("count"))
        )

        display(
            spark.table(SILVER_EVENTS_TABLE)
            .groupBy(
                "event_type",
                "is_late"
            )
            .agg(
                F.count("*").alias("event_count")
            )
            .orderBy(
                "event_type",
                "is_late"
            )
        )




    rows_after = spark.table(TARGET_TABLES[DATASET]).count()
    quarantine_after = (
        spark.table(QUARANTINE_TABLE)
        .filter(F.col("dataset") == DATASET)
        .count()
    )
    rows_added = max(rows_after - rows_before, 0)
    quarantine_added = max(quarantine_after - quarantine_before, 0)
    completed = datetime.now(timezone.utc).replace(tzinfo=None)

    metrics = {
        "target_table": TARGET_TABLES[DATASET],
        "rows_before": str(rows_before),
        "rows_after": str(rows_after),
        "quarantine_before": str(quarantine_before),
        "quarantine_after": str(quarantine_after),
    }
    if DATASET == "orders":
        metrics["order_items_rows"] = str(
            spark.table("marketplace_lab.silver.order_items").count()
        )

    write_task_audit(
        "SUCCEEDED",
        completed_at=completed,
        input_rows=input_rows,
        output_rows=rows_added,
        rejected_rows=quarantine_added,
        metrics=metrics,
    )

except Exception as error:
    completed = datetime.now(timezone.utc).replace(tzinfo=None)
    message = f"{type(error).__name__}: {str(error)}"
    write_task_audit(
        "FAILED",
        completed_at=completed,
        input_rows=input_rows,
        error_code=type(error).__name__,
        error_message=message[:4000],
        metrics={"target_table": TARGET_TABLES[DATASET]},
    )
    raise


# COMMAND ----------

result_df = spark.sql(f"""
SELECT
    run_id,
    task_key,
    task_run_id,
    dataset,
    task_status,
    input_row_count,
    output_row_count,
    rejected_row_count,
    metrics,
    started_at,
    completed_at
FROM {TASK_AUDIT_TABLE}
WHERE run_id = '{RUN_ID.replace("'", "''")}'
  AND task_key = '{TASK_KEY}'
  AND repair_count = {REPAIR_COUNT}
""")

assert result_df.count() == 1
assert result_df.first()["task_status"] == "SUCCEEDED"

print(f"Silver task completed: {TASK_KEY}")
display(result_df)
