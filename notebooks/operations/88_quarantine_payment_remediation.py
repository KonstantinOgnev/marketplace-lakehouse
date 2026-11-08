# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — quarantine payment remediation
# MAGIC
# MAGIC Corrects one `payments / INVALID_STATUS` record by redelivering a valid
# MAGIC CSV version through Landing. The original quarantine record remains
# MAGIC immutable; resolution is tracked in an Ops audit table.
# MAGIC
# MAGIC Run actions in order: `prepare` → full incremental Job → `validate`.

# COMMAND ----------

from datetime import datetime, timezone
import csv
import hashlib
import io
import json
import re

from pyspark.sql import functions as F

CATALOG = "marketplace_lab"
QUARANTINE_TABLE = f"{CATALOG}.quarantine.invalid_records"
SILVER_ORDERS = f"{CATALOG}.silver.orders"
SILVER_PAYMENTS = f"{CATALOG}.silver.payments"
GOLD_PAYMENTS = f"{CATALOG}.gold.fact_payments"
REMEDIATION_TABLE = f"{CATALOG}.ops.quarantine_remediation_log"
PAYMENTS_LANDING = f"/Volumes/{CATALOG}/bronze/landing/payments"

DEFAULT_QUARANTINE_ID = "19497096d991009fd1118ee4029a5535e7d4b30550d4650a95f671d2215917c5"

dbutils.widgets.dropdown("action", "prepare", ["prepare", "validate"], "Action")
dbutils.widgets.text("quarantine_id", DEFAULT_QUARANTINE_ID, "Quarantine ID")
dbutils.widgets.text("remediation_id", "payment_status_fix_01", "Remediation ID")

action = dbutils.widgets.get("action").strip().lower()
quarantine_id = dbutils.widgets.get("quarantine_id").strip()
remediation_id = dbutils.widgets.get("remediation_id").strip()

assert action in {"prepare", "validate"}
assert re.fullmatch(r"[A-Za-z0-9_-]+", remediation_id), (
    "remediation_id may contain only letters, digits, underscores and hyphens"
)

print(f"Action: {action}")
print(f"Remediation ID: {remediation_id}")
print(f"Quarantine ID: {quarantine_id}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {REMEDIATION_TABLE} (
      remediation_id STRING NOT NULL,
      quarantine_id STRING NOT NULL,
      dataset STRING NOT NULL,
      business_key STRING NOT NULL,
      original_error_code STRING NOT NULL,
      original_error_message STRING NOT NULL,
      original_raw_record STRING,
      corrected_order_id STRING NOT NULL,
      corrected_payment_status STRING NOT NULL,
      corrected_source_file STRING NOT NULL,
      corrected_batch_id STRING NOT NULL,
      remediation_status STRING NOT NULL,
      prepared_at TIMESTAMP NOT NULL,
      validated_at TIMESTAMP,
      silver_row_count BIGINT,
      gold_row_count BIGINT,
      resolution_message STRING
    ) USING DELTA
    """
)

# COMMAND ----------

if action == "prepare":
    existing_audit = spark.table(REMEDIATION_TABLE).filter(
        F.col("remediation_id") == remediation_id
    )
    assert existing_audit.limit(1).count() == 0, (
        f"remediation_id={remediation_id} already exists; use action=validate or a new ID"
    )

    quarantine_df = (
        spark.table(QUARANTINE_TABLE)
        .filter(F.col("quarantine_id") == quarantine_id)
    )
    assert quarantine_df.count() == 1, "Exactly one quarantine record is required"
    quarantine_row = quarantine_df.first()
    assert quarantine_row.dataset == "payments"
    assert quarantine_row.error_code == "INVALID_STATUS"

    raw = json.loads(quarantine_row.raw_record)
    assert raw.get("payment_id"), "Original payment_id is missing"
    assert raw.get("payment_method"), "Original payment_method is missing"
    assert raw.get("amount") is not None, "Original amount is missing"
    assert raw.get("currency"), "Original currency is missing"

    # Preserve the payment currency. Gold requires payment.currency to match
    # order.currency. Prefer an order in that currency without a payment; if
    # every such order already has one, use the first matching order because
    # multiple payment attempts for one order are valid.
    matching_orders_df = (
        spark.table(SILVER_ORDERS)
        .filter(F.upper(F.trim(F.col("currency"))) == str(raw["currency"]).upper())
        .select("order_id")
        .distinct()
    )
    unused_order_df = (
        matching_orders_df
        .join(
            spark.table(SILVER_PAYMENTS).select("order_id").distinct(),
            "order_id",
            "left_anti",
        )
        .orderBy("order_id")
    )
    order_candidate = unused_order_df.first()
    if order_candidate is None:
        order_candidate = (
            matching_orders_df.orderBy("order_id").first()
        )
    assert order_candidate is not None, (
        f"No valid Silver order is available for currency={raw['currency']}"
    )
    corrected_order_id = order_candidate.order_id

    prepared_at = datetime.now(timezone.utc).replace(tzinfo=None)
    load_date = prepared_at.date().isoformat()
    corrected_batch_id = f"remediation_{remediation_id}"
    batch_path = (
        f"{PAYMENTS_LANDING}/load_date={load_date}/"
        f"batch_id={corrected_batch_id}"
    )
    final_path = f"{batch_path}/payment_correction.csv"
    temp_path = f"{batch_path}/.payment_correction.csv.tmp"

    payment_time = prepared_at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{prepared_at.microsecond // 1000:03d}Z"
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([
        "payment_id", "order_id", "payment_status", "payment_method",
        "amount", "currency", "payment_time",
    ])
    writer.writerow([
        raw["payment_id"],
        corrected_order_id,
        "FAILED",
        raw["payment_method"],
        raw["amount"],
        raw["currency"],
        payment_time,
    ])

    dbutils.fs.mkdirs(batch_path)
    assert not any(item.path == final_path for item in dbutils.fs.ls(batch_path)), (
        f"Correction file already exists: {final_path}"
    )
    dbutils.fs.put(temp_path, output.getvalue(), overwrite=False)
    moved = dbutils.fs.mv(temp_path, final_path)
    assert moved, f"Atomic publish failed: {temp_path} -> {final_path}"

    audit_df = spark.createDataFrame(
        [(
            remediation_id,
            quarantine_id,
            "payments",
            raw["payment_id"],
            quarantine_row.error_code,
            quarantine_row.error_message,
            quarantine_row.raw_record,
            corrected_order_id,
            "FAILED",
            final_path,
            corrected_batch_id,
            "PREPARED",
            prepared_at,
            None,
            None,
            None,
            "Corrected INVALID_STATUS and UNKNOWN_REFERENCE via Landing redelivery",
        )],
        """remediation_id string, quarantine_id string, dataset string,
           business_key string, original_error_code string,
           original_error_message string, original_raw_record string,
           corrected_order_id string, corrected_payment_status string,
           corrected_source_file string, corrected_batch_id string,
           remediation_status string, prepared_at timestamp, validated_at timestamp,
           silver_row_count long, gold_row_count long, resolution_message string""",
    )
    # A newer remediation for the same quarantine row supersedes any earlier
    # prepared attempt that did not complete validation.
    escaped_quarantine_id = quarantine_id.replace("'", "''")
    spark.sql(
        f"""
        UPDATE {REMEDIATION_TABLE}
        SET remediation_status = 'SUPERSEDED',
            resolution_message = 'Superseded by a newer remediation attempt'
        WHERE quarantine_id = '{escaped_quarantine_id}'
          AND remediation_status = 'PREPARED'
        """
    )
    audit_df.write.mode("append").saveAsTable(REMEDIATION_TABLE)

    print("QUARANTINE REMEDIATION PREPARED")
    print(f"Business key: {raw['payment_id']}")
    print("Corrected status: FAILED")
    print(f"Corrected order_id: {corrected_order_id}")
    print(f"Correction file: {final_path}")
    print(f"Use load_date={load_date} for the incremental Job run.")
    display(audit_df)

# COMMAND ----------

if action == "validate":
    audit_df = spark.table(REMEDIATION_TABLE).filter(
        F.col("remediation_id") == remediation_id
    )
    assert audit_df.count() == 1, "Prepared remediation audit row was not found"
    audit = audit_df.first()

    silver_match_df = (
        spark.table(SILVER_PAYMENTS)
        .filter(
            (F.col("payment_id") == audit.business_key)
            & (F.col("order_id") == audit.corrected_order_id)
            & (F.col("payment_status") == audit.corrected_payment_status)
            & (F.col("source_batch_id") == audit.corrected_batch_id)
        )
    )
    gold_match_df = (
        spark.table(GOLD_PAYMENTS)
        .filter(F.col("payment_id") == audit.business_key)
    )
    silver_count = silver_match_df.count()
    gold_count = gold_match_df.count()

    status = "RESOLVED" if silver_count == 1 and gold_count == 1 else "VALIDATION_FAILED"
    validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    update_df = spark.createDataFrame(
        [(
            remediation_id,
            status,
            validated_at,
            silver_count,
            gold_count,
            (
                "Corrected payment is present in Silver and Gold; original quarantine row retained"
                if status == "RESOLVED"
                else "Corrected payment was not found exactly once in both Silver and Gold"
            ),
        )],
        """remediation_id string, remediation_status string, validated_at timestamp,
           silver_row_count long, gold_row_count long, resolution_message string""",
    )
    update_df.createOrReplaceTempView("quarantine_remediation_update")
    spark.sql(
        f"""
        MERGE INTO {REMEDIATION_TABLE} t
        USING quarantine_remediation_update s
          ON t.remediation_id = s.remediation_id
        WHEN MATCHED THEN UPDATE SET
          t.remediation_status = s.remediation_status,
          t.validated_at = s.validated_at,
          t.silver_row_count = s.silver_row_count,
          t.gold_row_count = s.gold_row_count,
          t.resolution_message = s.resolution_message
        """
    )

    display(spark.table(REMEDIATION_TABLE).filter(F.col("remediation_id") == remediation_id))
    print(f"Silver corrected rows: {silver_count} (expected 1)")
    print(f"Gold corrected rows: {gold_count} (expected 1)")
    print(f"Remediation status: {status}")
    assert status == "RESOLVED", "Quarantine remediation validation failed"
    print("QUARANTINE REMEDIATION TEST PASSED")
