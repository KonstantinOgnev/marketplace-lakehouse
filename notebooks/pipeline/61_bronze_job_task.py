# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Parameterized Bronze Auto Loader task
# MAGIC
# MAGIC One notebook is reused by five parallel Lakeflow Job tasks.
# MAGIC Each task must pass its own `dataset` parameter.

# COMMAND ----------

from datetime import datetime, timezone
import re

from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG = "marketplace_lab"
BRONZE_SCHEMA = "bronze"
OPS_SCHEMA = "ops"

LANDING_ROOT = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/landing"
STATE_ROOT = f"/Volumes/{CATALOG}/{OPS_SCHEMA}/checkpoints/_autoloader"
CHECKPOINT_ROOT = f"{STATE_ROOT}/checkpoints"
SCHEMA_ROOT = f"{STATE_ROOT}/schemas"

REGISTRY_TABLE = f"{CATALOG}.{OPS_SCHEMA}.batch_registry"
TASK_AUDIT_TABLE = f"{CATALOG}.{OPS_SCHEMA}.pipeline_task_runs"


def ensure_widget(name: str, default_value: str = "") -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value)


WIDGET_DEFAULTS = {
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
}

for widget_name, widget_default in WIDGET_DEFAULTS.items():
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
EFFECTIVE_SOURCE_SET_RAW = widget_value("effective_source_set").lower()
TASK_RUN_ID = widget_value("task_run_id") or "MANUAL"
REPAIR_COUNT = int(widget_value("repair_count") or "0")

ALLOWED_DATASETS = {
    "sellers",
    "products",
    "orders",
    "payments",
    "user_events",
}

assert RUN_ID, "run_id is required"
assert DATASET in ALLOWED_DATASETS, (
    f"Invalid dataset '{DATASET}'. Allowed values: {sorted(ALLOWED_DATASETS)}"
)
assert MODE in {"incremental", "backfill", "replay", "full-refresh"}, (
    f"Unsupported mode '{MODE}'"
)

if EFFECTIVE_SOURCE_SET_RAW in {"", "all"}:
    EFFECTIVE_SOURCE_SET = ALLOWED_DATASETS.copy()
else:
    EFFECTIVE_SOURCE_SET = {
        value.strip()
        for value in EFFECTIVE_SOURCE_SET_RAW.split(",")
        if value.strip()
    }

TASK_KEY = f"bronze_{DATASET}"
TASK_STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Source schemas and independent Auto Loader state

# COMMAND ----------

order_item_schema = T.StructType([
    T.StructField("product_id", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price", T.DecimalType(12, 2), True),
    T.StructField("line_amount", T.DecimalType(14, 2), True),
])

SOURCE_CONFIG = {
    "user_events": {
        "format": "json",
        "schema": T.StructType([
            T.StructField("event_id", T.StringType(), True),
            T.StructField("event_type", T.StringType(), True),
            T.StructField("event_time", T.TimestampType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("product_id", T.StringType(), True),
            T.StructField("order_id", T.StringType(), True),
            T.StructField("session_id", T.StringType(), True),
            T.StructField("price", T.DecimalType(12, 2), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("quantity", T.IntegerType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("device", T.StringType(), True),
            T.StructField("ingestion_time", T.TimestampType(), True),
            T.StructField("_corrupt_record", T.StringType(), True),
        ]),
        "reader_options": {
            "mode": "PERMISSIVE",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "columnNameOfCorruptRecord": "_corrupt_record",
        },
    },
    "orders": {
        "format": "json",
        "schema": T.StructType([
            T.StructField("order_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("seller_id", T.StringType(), True),
            T.StructField("items", T.ArrayType(order_item_schema, True), True),
            T.StructField("total_quantity", T.LongType(), True),
            T.StructField("order_amount", T.DecimalType(18, 2), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("order_status", T.StringType(), True),
            T.StructField("created_at", T.TimestampType(), True),
            T.StructField("updated_at", T.TimestampType(), True),
            T.StructField("sales_channel", T.StringType(), True),
            T.StructField("coupon_code", T.StringType(), True),
            T.StructField("delivery_priority", T.StringType(), True),
            T.StructField("schema_version", T.IntegerType(), True),
            T.StructField("_corrupt_record", T.StringType(), True),
        ]),
        "reader_options": {
            "mode": "PERMISSIVE",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "columnNameOfCorruptRecord": "_corrupt_record",
        },
    },
    "payments": {
        "format": "csv",
        "schema": T.StructType([
            T.StructField("payment_id", T.StringType(), True),
            T.StructField("order_id", T.StringType(), True),
            T.StructField("payment_status", T.StringType(), True),
            T.StructField("payment_method", T.StringType(), True),
            T.StructField("amount", T.DecimalType(18, 2), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("payment_time", T.TimestampType(), True),
            T.StructField("_corrupt_record", T.StringType(), True),
        ]),
        "reader_options": {
            "header": "true",
            "delimiter": ",",
            "mode": "PERMISSIVE",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
            "columnNameOfCorruptRecord": "_corrupt_record",
        },
    },
    "products": {
        "format": "parquet",
        "schema": T.StructType([
            T.StructField("product_id", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("seller_id", T.StringType(), True),
            T.StructField("product_name", T.StringType(), True),
            T.StructField("price", T.DecimalType(12, 2), True),
            T.StructField("is_active", T.BooleanType(), True),
        ]),
        "reader_options": {},
    },
    "sellers": {
        "format": "csv",
        "schema": T.StructType([
            T.StructField("seller_id", T.StringType(), True),
            T.StructField("country", T.StringType(), True),
            T.StructField("segment", T.StringType(), True),
            T.StructField("registration_date", T.DateType(), True),
            T.StructField("status", T.StringType(), True),
        ]),
        "reader_options": {
            "header": "true",
            "delimiter": ",",
            "mode": "PERMISSIVE",
            "dateFormat": "yyyy-MM-dd",
            "pathGlobFilter": "*.csv",
        },
    },
}

for source_name, config in SOURCE_CONFIG.items():
    config["source_path"] = f"{LANDING_ROOT}/{source_name}"
    config["target_table"] = f"{CATALOG}.{BRONZE_SCHEMA}.{source_name}_raw"
    config["checkpoint_path"] = f"{CHECKPOINT_ROOT}/{source_name}"
    config["schema_path"] = f"{SCHEMA_ROOT}/{source_name}"


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


config = SOURCE_CONFIG[DATASET]
assert path_exists(config["source_path"]), (
    f"Landing source path does not exist: {config['source_path']}"
)

dbutils.fs.mkdirs(CHECKPOINT_ROOT)
dbutils.fs.mkdirs(SCHEMA_ROOT)


# COMMAND ----------
# MAGIC %md
# MAGIC ## Task audit helpers

# COMMAND ----------

def write_task_audit(
    status: str,
    completed_at=None,
    input_rows=None,
    output_rows=None,
    rejected_rows=None,
    source_files=None,
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
        RUN_ID,
        TASK_KEY,
        TASK_RUN_ID,
        REPAIR_COUNT,
        DATASET,
        status,
        TASK_STARTED_AT,
        completed_at,
        input_rows,
        output_rows,
        rejected_rows,
        source_files,
        error_code,
        error_message,
        metrics,
        TASK_STARTED_AT,
        now_value,
    )], audit_schema)

    audit_df.createOrReplaceTempView("bronze_task_audit_source")

    spark.sql(f"""
    MERGE INTO {TASK_AUDIT_TABLE} AS target
    USING bronze_task_audit_source AS source
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
        target.source_file_count = source.source_file_count,
        target.error_code = source.error_code,
        target.error_message = source.error_message,
        target.metrics = source.metrics,
        target.updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT *
    """)


if DATASET not in EFFECTIVE_SOURCE_SET:
    completed = datetime.now(timezone.utc).replace(tzinfo=None)
    write_task_audit(
        status="SKIPPED",
        completed_at=completed,
        metrics={
            "reason": "dataset_not_in_effective_source_set",
            "effective_source_set": ",".join(sorted(EFFECTIVE_SOURCE_SET)),
        },
    )
    print(f"Dataset {DATASET} skipped: not present in effective_source_set")
    dbutils.notebook.exit(f"SKIPPED:{DATASET}")


write_task_audit(
    status="RUNNING",
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
# MAGIC ## Auto Loader AvailableNow execution

# COMMAND ----------

def escaped_sql(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def set_registry_processing() -> None:
    spark.sql(f"""
    UPDATE {REGISTRY_TABLE}
    SET
        batch_status = 'DISCOVERED',
        discovered_at = COALESCE(discovered_at, current_timestamp()),
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = '{DATASET}'
      AND batch_status IN ('LANDED', 'FAILED', 'DISCOVERED')
    """)

    spark.sql(f"""
    UPDATE {REGISTRY_TABLE}
    SET
        batch_status = 'PROCESSING',
        processing_started_at = COALESCE(
            processing_started_at,
            current_timestamp()
        ),
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = '{DATASET}'
      AND batch_status IN ('DISCOVERED', 'PROCESSING')
    """)


def set_registry_success() -> None:
    spark.sql(f"""
    UPDATE {REGISTRY_TABLE}
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = COALESCE(processed_at, current_timestamp()),
        bronze_table = '{config['target_table']}',
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = '{DATASET}'
      AND batch_status IN (
          'LANDED', 'DISCOVERED', 'PROCESSING', 'BRONZE_WRITTEN'
      )
    """)


def set_registry_failure(message: str) -> None:
    spark.sql(f"""
    UPDATE {REGISTRY_TABLE}
    SET
        batch_status = 'FAILED',
        error_message = '{escaped_sql(message)[:4000]}',
        updated_at = current_timestamp()
    WHERE dataset = '{DATASET}'
      AND batch_status IN ('LANDED', 'DISCOVERED', 'PROCESSING')
    """)


def build_bronze_stream():
    reader = (
        spark.readStream
        .format("cloudFiles")
        .schema(config["schema"])
        .option("cloudFiles.format", config["format"])
        .option("cloudFiles.schemaLocation", config["schema_path"])
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("rescuedDataColumn", "_auto_rescued_data")
    )

    for option_name, option_value in config["reader_options"].items():
        reader = reader.option(option_name, option_value)

    source_df = reader.load(config["source_path"])

    corrupt_value = (
        F.col("_corrupt_record")
        if "_corrupt_record" in source_df.columns
        else F.lit(None).cast("string")
    )

    return (
        source_df
        .withColumn(
            "_rescued_data",
            F.coalesce(F.col("_auto_rescued_data"), corrupt_value),
        )
        .drop("_auto_rescued_data", "_corrupt_record")
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_batch_id",
            F.regexp_extract(F.col("_metadata.file_path"), r"batch_id=([^/]+)", 1),
        )
        .withColumn(
            "_load_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"load_date=([^/]+)",
                    1,
                ),
                "yyyy-MM-dd",
            ),
        )
    )


target_existed_before = spark.catalog.tableExists(config["target_table"])
rows_before = spark.table(config["target_table"]).count() if target_existed_before else 0

try:
    set_registry_processing()
    bronze_stream_df = build_bronze_stream()

    safe_run_id = re.sub(r"[^A-Za-z0-9_]", "_", RUN_ID)[-80:]
    query = (
        bronze_stream_df.writeStream
        .queryName(f"bronze_{DATASET}_{safe_run_id}")
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["checkpoint_path"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config["target_table"])
    )
    query.awaitTermination()
    set_registry_success()

    target_df = spark.table(config["target_table"])
    rows_after = target_df.count()
    rows_written = max(rows_after - rows_before, 0)

    current_run_df = target_df.filter(
        F.col("_ingest_ts") >= F.lit(TASK_STARTED_AT)
    )
    current_run_rows = current_run_df.count()
    current_run_rescued = current_run_df.filter(
        F.col("_rescued_data").isNotNull()
    ).count()
    current_run_files = current_run_df.select("_source_file").distinct().count()
    missing_metadata = current_run_df.filter(
        F.col("_source_file").isNull()
        | (F.col("_source_file") == "")
        | F.col("_batch_id").isNull()
        | (F.col("_batch_id") == "")
        | F.col("_load_date").isNull()
        | F.col("_ingest_ts").isNull()
    ).count()

    assert missing_metadata == 0, (
        f"{DATASET}: {missing_metadata} newly written rows have missing lineage"
    )
    assert current_run_rows == rows_written, (
        f"{DATASET}: row delta {rows_written} differs from current-run rows "
        f"{current_run_rows}"
    )

    task_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    write_task_audit(
        status="SUCCEEDED",
        completed_at=task_completed_at,
        input_rows=current_run_rows,
        output_rows=rows_written,
        rejected_rows=current_run_rescued,
        source_files=current_run_files,
        metrics={
            "target_table": config["target_table"],
            "checkpoint_path": config["checkpoint_path"],
            "schema_path": config["schema_path"],
            "rows_before": str(rows_before),
            "rows_after": str(rows_after),
            "missing_metadata_rows": str(missing_metadata),
        },
    )

except Exception as error:
    error_message = f"{type(error).__name__}: {str(error)}"
    set_registry_failure(error_message)
    task_completed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    write_task_audit(
        status="FAILED",
        completed_at=task_completed_at,
        error_code=type(error).__name__,
        error_message=error_message[:4000],
        metrics={
            "target_table": config["target_table"],
            "checkpoint_path": config["checkpoint_path"],
        },
    )
    raise


# COMMAND ----------
# MAGIC %md
# MAGIC ## Result

# COMMAND ----------

result_df = spark.sql(f"""
SELECT
    run_id,
    task_key,
    task_run_id,
    repair_count,
    dataset,
    task_status,
    input_row_count,
    output_row_count,
    rejected_row_count,
    source_file_count,
    started_at,
    completed_at,
    metrics
FROM {TASK_AUDIT_TABLE}
WHERE run_id = '{escaped_sql(RUN_ID)}'
  AND task_key = '{TASK_KEY}'
  AND repair_count = {REPAIR_COUNT}
""")

assert result_df.count() == 1
assert result_df.first()["task_status"] == "SUCCEEDED"

print(f"Bronze task completed: {TASK_KEY}")
print(f"Run ID:               {RUN_ID}")
print(f"Dataset:              {DATASET}")
print(f"Target table:         {config['target_table']}")
print(f"Rows before:          {rows_before}")
print(f"Rows after:           {rows_after}")
print(f"Rows written:         {rows_written}")
print(f"Rescued rows:         {current_run_rescued}")
print(f"Source files:         {current_run_files}")

display(result_df)
