# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — physical redelivery test
# MAGIC
# MAGIC This test copies one already processed `orders` file under a new name.
# MAGIC Expected result after the full Job run:
# MAGIC
# MAGIC - Bronze grows by the number of rows in the copied file;
# MAGIC - Silver orders does not grow;
# MAGIC - Gold fact_orders does not grow.
# MAGIC
# MAGIC Run actions in order: `prepare` → full Lakeflow Job → `validate` → `cleanup`.

# COMMAND ----------

from datetime import datetime, timezone
import re

from pyspark.sql import functions as F, types as T

CATALOG = "marketplace_lab"
TEST_TABLE = f"{CATALOG}.ops.redelivery_test_runs"
BRONZE_TABLE = f"{CATALOG}.bronze.orders_raw"
SILVER_TABLE = f"{CATALOG}.silver.orders"
GOLD_TABLE = f"{CATALOG}.gold.fact_orders"
ORDERS_ROOT = f"/Volumes/{CATALOG}/bronze/landing/orders"

dbutils.widgets.dropdown("action", "prepare", ["prepare", "validate", "cleanup"], "Action")
dbutils.widgets.text("test_id", "orders_redelivery_01", "Test ID")
dbutils.widgets.text("load_date", "", "Load date (blank = latest)")
dbutils.widgets.text("batch_id", "", "Batch ID (blank = latest)")

action = dbutils.widgets.get("action").strip().lower()
test_id = dbutils.widgets.get("test_id").strip()
requested_load_date = dbutils.widgets.get("load_date").strip()
requested_batch_id = dbutils.widgets.get("batch_id").strip()

assert action in {"prepare", "validate", "cleanup"}
assert re.fullmatch(r"[A-Za-z0-9_-]+", test_id), (
    "test_id may contain only letters, digits, underscores and hyphens"
)

print(f"Action: {action}")
print(f"Test ID: {test_id}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
      test_id STRING NOT NULL,
      prepared_at TIMESTAMP NOT NULL,
      load_date STRING NOT NULL,
      batch_id STRING NOT NULL,
      source_file STRING NOT NULL,
      duplicate_file STRING NOT NULL,
      source_file_rows BIGINT NOT NULL,
      baseline_bronze_rows BIGINT NOT NULL,
      baseline_silver_rows BIGINT NOT NULL,
      baseline_gold_rows BIGINT NOT NULL,
      validation_bronze_rows BIGINT,
      validation_silver_rows BIGINT,
      validation_gold_rows BIGINT,
      bronze_row_delta BIGINT,
      silver_row_delta BIGINT,
      gold_row_delta BIGINT,
      validation_status STRING,
      validated_at TIMESTAMP,
      cleaned_up_at TIMESTAMP
    ) USING DELTA
    """
)


def table_count(table_name: str) -> int:
    return spark.table(table_name).count()


def latest_named_directory(root: str, prefix: str, requested_value: str = ""):
    directories = [
        item for item in dbutils.fs.ls(root)
        if item.isDir() and item.name.rstrip("/").startswith(prefix)
    ]
    if requested_value:
        expected_name = f"{prefix}{requested_value}"
        matches = [item for item in directories if item.name.rstrip("/") == expected_name]
        assert matches, f"Directory {expected_name} was not found under {root}"
        return matches[0]
    assert directories, f"No {prefix} directories found under {root}"
    return sorted(directories, key=lambda x: x.name)[-1]

# COMMAND ----------

if action == "prepare":
    previous = spark.table(TEST_TABLE).filter(F.col("test_id") == test_id)
    assert previous.limit(1).count() == 0, (
        f"test_id={test_id} already exists. Use a new test_id or finish its cleanup."
    )

    load_date_dir = latest_named_directory(ORDERS_ROOT, "load_date=", requested_load_date)
    batch_dir = latest_named_directory(
        load_date_dir.path,
        "batch_id=",
        requested_batch_id,
    )

    source_candidates = [
        item for item in dbutils.fs.ls(batch_dir.path)
        if not item.isDir()
        and item.size > 0
        and not item.name.startswith(("_", ".", "redelivery_test_"))
        and (item.name.endswith(".json") or item.name.startswith("part-"))
    ]
    assert source_candidates, f"No orders data files found in {batch_dir.path}"
    source = sorted(source_candidates, key=lambda x: x.name)[0]

    duplicate_name = f"redelivery_test_{test_id}_{source.name}"
    duplicate_path = batch_dir.path.rstrip("/") + "/" + duplicate_name
    assert not any(item.path == duplicate_path for item in dbutils.fs.ls(batch_dir.path)), (
        f"Duplicate test file already exists: {duplicate_path}"
    )

    source_file_rows = spark.read.format("json").load(source.path).count()
    assert source_file_rows > 0, "Selected source file contains no readable rows"

    baseline_bronze = table_count(BRONZE_TABLE)
    baseline_silver = table_count(SILVER_TABLE)
    baseline_gold = table_count(GOLD_TABLE)

    copied = dbutils.fs.cp(source.path, duplicate_path)
    assert copied, f"Copy failed: {source.path} -> {duplicate_path}"

    load_date = load_date_dir.name.rstrip("/").split("=", 1)[1]
    batch_id = batch_dir.name.rstrip("/").split("=", 1)[1]
    prepared_at = datetime.now(timezone.utc).replace(tzinfo=None)

    row_schema = T.StructType([
        T.StructField("test_id", T.StringType(), False),
        T.StructField("prepared_at", T.TimestampType(), False),
        T.StructField("load_date", T.StringType(), False),
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("source_file", T.StringType(), False),
        T.StructField("duplicate_file", T.StringType(), False),
        T.StructField("source_file_rows", T.LongType(), False),
        T.StructField("baseline_bronze_rows", T.LongType(), False),
        T.StructField("baseline_silver_rows", T.LongType(), False),
        T.StructField("baseline_gold_rows", T.LongType(), False),
    ])
    row_df = spark.createDataFrame(
        [(
            test_id,
            prepared_at,
            load_date,
            batch_id,
            source.path,
            duplicate_path,
            source_file_rows,
            baseline_bronze,
            baseline_silver,
            baseline_gold,
        )],
        row_schema,
    )
    row_df.write.mode("append").saveAsTable(TEST_TABLE)

    print("REDELIVERY TEST FILE PREPARED")
    print(f"Load date: {load_date}")
    print(f"Batch ID: {batch_id}")
    print(f"Source rows: {source_file_rows}")
    print(f"Duplicate file: {duplicate_path}")
    print("Now run the full Lakeflow Job once with incremental mode.")
    display(row_df)

# COMMAND ----------

if action == "validate":
    test_row = spark.table(TEST_TABLE).filter(F.col("test_id") == test_id)
    assert test_row.count() == 1, f"Exactly one prepared row expected for test_id={test_id}"
    baseline = test_row.first()

    current_bronze = table_count(BRONZE_TABLE)
    current_silver = table_count(SILVER_TABLE)
    current_gold = table_count(GOLD_TABLE)

    bronze_delta = current_bronze - baseline.baseline_bronze_rows
    silver_delta = current_silver - baseline.baseline_silver_rows
    gold_delta = current_gold - baseline.baseline_gold_rows

    duplicate_path_exists = any(
        item.path == baseline.duplicate_file
        for item in dbutils.fs.ls(baseline.duplicate_file.rsplit("/", 1)[0])
    )

    if not duplicate_path_exists:
        validation_status = "FAILED_TEST_FILE_MISSING"
    elif bronze_delta != baseline.source_file_rows:
        validation_status = "FAILED_UNEXPECTED_BRONZE_DELTA"
    elif silver_delta != 0:
        validation_status = "FAILED_SILVER_NOT_IDEMPOTENT"
    elif gold_delta != 0:
        validation_status = "FAILED_GOLD_NOT_IDEMPOTENT"
    else:
        validation_status = "PASSED"

    update_df = spark.createDataFrame(
        [(
            test_id,
            current_bronze,
            current_silver,
            current_gold,
            bronze_delta,
            silver_delta,
            gold_delta,
            validation_status,
            datetime.now(timezone.utc).replace(tzinfo=None),
        )],
        """test_id string, validation_bronze_rows long, validation_silver_rows long,
           validation_gold_rows long, bronze_row_delta long, silver_row_delta long,
           gold_row_delta long, validation_status string, validated_at timestamp""",
    )
    update_df.createOrReplaceTempView("redelivery_validation_update")
    spark.sql(
        f"""
        MERGE INTO {TEST_TABLE} t
        USING redelivery_validation_update s
          ON t.test_id = s.test_id
        WHEN MATCHED THEN UPDATE SET
          t.validation_bronze_rows = s.validation_bronze_rows,
          t.validation_silver_rows = s.validation_silver_rows,
          t.validation_gold_rows = s.validation_gold_rows,
          t.bronze_row_delta = s.bronze_row_delta,
          t.silver_row_delta = s.silver_row_delta,
          t.gold_row_delta = s.gold_row_delta,
          t.validation_status = s.validation_status,
          t.validated_at = s.validated_at
        """
    )

    result_df = spark.table(TEST_TABLE).filter(F.col("test_id") == test_id)
    display(result_df)
    print(f"Bronze row delta: {bronze_delta} (expected {baseline.source_file_rows})")
    print(f"Silver row delta: {silver_delta} (expected 0)")
    print(f"Gold row delta: {gold_delta} (expected 0)")
    print(f"Validation status: {validation_status}")
    assert validation_status == "PASSED", "Physical redelivery validation failed"
    print("PHYSICAL REDELIVERY TEST PASSED")

# COMMAND ----------

if action == "cleanup":
    test_row = spark.table(TEST_TABLE).filter(F.col("test_id") == test_id)
    assert test_row.count() == 1, f"Exactly one test row expected for test_id={test_id}"
    test = test_row.first()
    assert test.validation_status == "PASSED", (
        "Cleanup is allowed only after a PASSED validation"
    )

    parent_path = test.duplicate_file.rsplit("/", 1)[0]
    duplicate_exists = any(item.path == test.duplicate_file for item in dbutils.fs.ls(parent_path))
    if duplicate_exists:
        removed = dbutils.fs.rm(test.duplicate_file, False)
        assert removed, f"Could not remove {test.duplicate_file}"

    cleanup_df = spark.createDataFrame(
        [(test_id, datetime.now(timezone.utc).replace(tzinfo=None))],
        "test_id string, cleaned_up_at timestamp",
    )
    cleanup_df.createOrReplaceTempView("redelivery_cleanup_update")
    spark.sql(
        f"""
        MERGE INTO {TEST_TABLE} t
        USING redelivery_cleanup_update s
          ON t.test_id = s.test_id
        WHEN MATCHED THEN UPDATE SET t.cleaned_up_at = s.cleaned_up_at
        """
    )

    print("CLEANUP COMPLETED")
    print(f"Removed test file: {test.duplicate_file}")
    display(spark.table(TEST_TABLE).filter(F.col("test_id") == test_id))
