# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Silver framework setup

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "marketplace_lab"
QUARANTINE_SCHEMA = "quarantine"
OPS_SCHEMA = "ops"
TASK_AUDIT_TABLE = f"{CATALOG}.{OPS_SCHEMA}.pipeline_task_runs"


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

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{QUARANTINE_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPS_SCHEMA}")

# Этап 6. Silver Layer
# Ячейка 2. Quarantine-контракт и каталог ошибок

from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. Названия служебных таблиц
# ---------------------------------------------------------

QUARANTINE_TABLE = (
    f"{CATALOG}.{QUARANTINE_SCHEMA}.invalid_records"
)

DQ_RULE_CATALOG_TABLE = (
    f"{CATALOG}.{OPS_SCHEMA}.data_quality_rule_catalog"
)


# ---------------------------------------------------------
# 2. Создание единой quarantine-таблицы
# ---------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE}
(
    quarantine_id       STRING    NOT NULL
        COMMENT 'Детерминированный идентификатор ошибки',

    dataset             STRING    NOT NULL
        COMMENT 'Название исходного набора данных',

    business_key        STRING
        COMMENT 'Бизнес-ключ исходной записи',

    error_code          STRING    NOT NULL
        COMMENT 'Машиночитаемый код ошибки',

    error_message       STRING    NOT NULL
        COMMENT 'Описание причины помещения в quarantine',

    source_file         STRING
        COMMENT 'Путь к исходному файлу',

    batch_id            STRING
        COMMENT 'Идентификатор входного batch',

    load_date           DATE
        COMMENT 'Дата загрузки исходного batch',

    original_ingest_ts  TIMESTAMP
        COMMENT 'Время поступления записи в Bronze',

    quarantined_at      TIMESTAMP NOT NULL
        COMMENT 'Время помещения записи в quarantine',

    processing_run_id   STRING    NOT NULL
        COMMENT 'Идентификатор запуска Silver',

    raw_record          STRING
        COMMENT 'Исходная запись в JSON-представлении'
)
USING DELTA
COMMENT 'Invalid Marketplace records rejected during Silver processing'
TBLPROPERTIES
(
    'quality' = 'quarantine',
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ---------------------------------------------------------
# 3. Каталог стандартных правил качества
# ---------------------------------------------------------

DQ_RULES = [
    {
        "error_code": "MALFORMED_RECORD",
        "rule_category": "PARSING",
        "severity": "ERROR",
        "description": (
            "Исходная JSON- или CSV-запись не может быть "
            "корректно разобрана"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "RESCUED_DATA",
        "rule_category": "SCHEMA",
        "severity": "ERROR",
        "description": (
            "Запись содержит неизвестные поля или значения, "
            "не совместимые с Bronze-схемой"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "MISSING_BUSINESS_KEY",
        "rule_category": "COMPLETENESS",
        "severity": "ERROR",
        "description": (
            "Обязательный бизнес-ключ отсутствует или пуст"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "MISSING_REQUIRED_FIELD",
        "rule_category": "COMPLETENESS",
        "severity": "ERROR",
        "description": (
            "Одно или несколько обязательных полей отсутствуют"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "INVALID_TIMESTAMP",
        "rule_category": "TYPE",
        "severity": "ERROR",
        "description": (
            "Дата или время отсутствуют либо не могут быть "
            "преобразованы в timestamp"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "INVALID_DECIMAL",
        "rule_category": "TYPE",
        "severity": "ERROR",
        "description": (
            "Числовое значение не может быть преобразовано "
            "в decimal"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "INVALID_STATUS",
        "rule_category": "DOMAIN",
        "severity": "ERROR",
        "description": (
            "Статус отсутствует в разрешённом перечне значений"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "INVALID_CURRENCY",
        "rule_category": "DOMAIN",
        "severity": "ERROR",
        "description": (
            "Код валюты отсутствует в поддерживаемом перечне"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "NEGATIVE_AMOUNT",
        "rule_category": "BUSINESS",
        "severity": "ERROR",
        "description": (
            "Цена или денежная сумма имеет отрицательное значение"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "UNKNOWN_REFERENCE",
        "rule_category": "REFERENTIAL",
        "severity": "ERROR",
        "description": (
            "Связанная запись отсутствует в справочнике "
            "или родительском наборе данных"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "AMOUNT_MISMATCH",
        "rule_category": "RECONCILIATION",
        "severity": "ERROR",
        "description": (
            "Расчётная сумма не совпадает с заявленной суммой"
        ),
        "quarantine_required": True
    },
    {
        "error_code": "DUPLICATE_SUPERSEDED",
        "rule_category": "UNIQUENESS",
        "severity": "INFO",
        "description": (
            "Запись была заменена более актуальной версией "
            "того же бизнес-ключа"
        ),
        "quarantine_required": False
    }
]


dq_rules_df = (
    spark.createDataFrame(DQ_RULES)
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


# ---------------------------------------------------------
# 4. Создание таблицы каталога правил
# ---------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {DQ_RULE_CATALOG_TABLE}
(
    error_code             STRING    NOT NULL,
    rule_category          STRING    NOT NULL,
    severity               STRING    NOT NULL,
    description            STRING    NOT NULL,
    quarantine_required    BOOLEAN   NOT NULL,
    created_at             TIMESTAMP NOT NULL,
    updated_at             TIMESTAMP
)
USING DELTA
COMMENT 'Catalog of data-quality rules used by Silver pipelines'
TBLPROPERTIES
(
    'quality' = 'ops'
)
""")


# ---------------------------------------------------------
# 5. Идемпотентное обновление каталога правил
# ---------------------------------------------------------

dq_rules_df.createOrReplaceTempView(
    "incoming_dq_rules"
)

spark.sql(f"""
MERGE INTO {DQ_RULE_CATALOG_TABLE} AS target
USING incoming_dq_rules AS source
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
# 6. Проверка структуры quarantine
# ---------------------------------------------------------

required_quarantine_columns = {
    "quarantine_id",
    "dataset",
    "business_key",
    "error_code",
    "error_message",
    "source_file",
    "batch_id",
    "load_date",
    "original_ingest_ts",
    "quarantined_at",
    "processing_run_id",
    "raw_record"
}

actual_quarantine_columns = set(
    spark.table(QUARANTINE_TABLE).columns
)

missing_quarantine_columns = sorted(
    required_quarantine_columns
    - actual_quarantine_columns
)

rule_count = spark.table(
    DQ_RULE_CATALOG_TABLE
).count()

quarantine_row_count = spark.table(
    QUARANTINE_TABLE
).count()

assert len(missing_quarantine_columns) == 0, (
    "Missing quarantine columns: "
    f"{missing_quarantine_columns}"
)

assert rule_count >= len(DQ_RULES), (
    f"Expected at least {len(DQ_RULES)} DQ rules, "
    f"found {rule_count}"
)


# ---------------------------------------------------------
# 7. Результат
# ---------------------------------------------------------

print("Quarantine framework is ready")
print(f"Quarantine table:        {QUARANTINE_TABLE}")
print(f"Current quarantine rows: {quarantine_row_count}")
print(f"DQ rule catalog:         {DQ_RULE_CATALOG_TABLE}")
print(f"Registered DQ rules:     {rule_count}")
print(f"Missing columns:         {len(missing_quarantine_columns)}")

display(
    spark.table(DQ_RULE_CATALOG_TABLE)
    .orderBy(
        "rule_category",
        "error_code"
    )
)

display(
    spark.sql(
        f"DESCRIBE TABLE {QUARANTINE_TABLE}"
    )
)

# COMMAND ----------

# Enable row-level concurrency for shared Silver operational tables.
for table_name in [QUARANTINE_TABLE, DQ_RULE_CATALOG_TABLE]:
    detail = spark.sql(f"DESCRIBE DETAIL {table_name}").first().asDict(True)
    assert not detail.get("partitionColumns", []), (
        f"{table_name} must be unpartitioned for row-level concurrency"
    )
    spark.sql(f"""
    ALTER TABLE {table_name}
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
    """)

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

setup_audit_df = spark.createDataFrame([(
    RUN_ID, "silver_setup", TASK_RUN_ID, REPAIR_COUNT, None, "SUCCEEDED",
    TASK_STARTED_AT, completed, None, None, None, None, None, None,
    {
        "quarantine_table": QUARANTINE_TABLE,
        "dq_rule_catalog": DQ_RULE_CATALOG_TABLE,
        "registered_rules": str(rule_count),
    },
    TASK_STARTED_AT, completed,
)], audit_schema)
setup_audit_df.createOrReplaceTempView("silver_setup_audit_source")

spark.sql(f"""
MERGE INTO {TASK_AUDIT_TABLE} AS target
USING silver_setup_audit_source AS source
  ON target.run_id = source.run_id
 AND target.task_key = source.task_key
 AND target.repair_count = source.repair_count
WHEN MATCHED THEN UPDATE SET
    target.task_run_id = source.task_run_id,
    target.task_status = source.task_status,
    target.started_at = source.started_at,
    target.completed_at = source.completed_at,
    target.metrics = source.metrics,
    target.updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

print("Silver framework setup completed")
display(setup_audit_df)
