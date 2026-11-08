# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Production runbook and final readiness report
# MAGIC
# MAGIC Publishes operational recovery procedures, records the documented COST_01
# MAGIC exception, and creates the final Stage 14 readiness view. The exception
# MAGIC remains visible and does not get converted into a false `PASSED` result.

# COMMAND ----------

import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T


def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


for widget_name, default_value in {
    "catalog_name": "marketplace_lab",
    "environment": "dev",
    "cost01_disposition": "ACCEPTED_FOR_LAB",
    "exception_review_days": "90",
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
COST01_DISPOSITION = (dbutils.widgets.get("cost01_disposition") or "ACCEPTED_FOR_LAB").strip().upper()
REVIEW_DAYS = int(dbutils.widgets.get("exception_review_days") or "90")

assert ENVIRONMENT in {"dev", "test", "prod"}
assert COST01_DISPOSITION in {"ACCEPTED_FOR_LAB", "FEATURE_UNAVAILABLE", "PENDING_ENABLEMENT"}
assert 1 <= REVIEW_DAYS <= 365

OPS = f"{CATALOG}.ops"
RUNS = f"{OPS}.production_hardening_runs"
CHECKS = f"{OPS}.production_hardening_checks"
RUNBOOK = f"{OPS}.production_runbook_procedures"
EXCEPTIONS = f"{OPS}.production_hardening_exceptions"
FINAL_VIEW = f"{OPS}.v_production_readiness_final"

for table_name in [RUNS, CHECKS, RUNBOOK]:
    assert spark.catalog.tableExists(table_name), f"Missing {table_name}; run notebook 140 first"

NOW = datetime.now(timezone.utc).replace(tzinfo=None)
REVIEW_AT = NOW + timedelta(days=REVIEW_DAYS)
CURRENT_USER = spark.sql("SELECT current_user() AS user").first().user

latest_audit = spark.sql(f"""
SELECT audit_run_id
FROM {RUNS}
WHERE environment = '{ENVIRONMENT}'
QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
""").first()
assert latest_audit is not None
AUDIT_RUN_ID = latest_audit.audit_run_id

# COMMAND ----------
# 1. Publish the operational runbook

procedures = [
    (
        10, "BRONZE_FAILURE", "One or more Bronze Auto Loader tasks failed",
        [
            "Open the failed Lakeflow run and identify the source-specific Bronze task",
            "Read ops.pipeline_task_runs and ops.pipeline_runs for the run_id",
            "Check source_batch_manifest for expected files and batch status",
            "Inspect the source-specific checkpoint and schema location; do not delete either",
            "Inspect _rescued_data and quarantine metrics before changing the schema",
        ],
        [
            "Correct the source path, schema contract, permission, or malformed input problem",
            "Use Repair run to restart the failed task and its dependent tasks",
            "Preserve the original checkpoint so already committed files remain idempotent",
            "Use replay mode only when an explicit controlled reprocessing decision was made",
        ],
        [
            "Confirm the Bronze table row count and distinct source-file count",
            "Confirm the batch registry progressed to BRONZE_WRITTEN or the expected final status",
            "Confirm no duplicate business keys were introduced downstream",
        ],
        "Escalate when checkpoint corruption, repeated schema failure, or data loss is suspected",
        "Data Engineer",
    ),
    (
        20, "BATCH_RESTART", "A batch must be restarted after a task failure",
        [
            "Locate the original pipeline run_id, batch_id and load_date",
            "Determine the last successfully completed task from ops.pipeline_task_runs",
            "Verify that source files are unchanged and still registered in source_batch_manifest",
        ],
        [
            "Prefer Lakeflow Repair run over starting an unrelated new run",
            "If a new run is required, reuse batch_id and use replay mode with an explicit new run_id",
            "Never remove checkpoints merely to force rediscovery",
        ],
        [
            "Compare source rows, Bronze rows, Silver rows and quarantine rows",
            "Run the idempotency checks and confirm facts and aggregates were not doubled",
        ],
        "Escalate when the source batch changed after partial processing",
        "Data Engineer",
    ),
    (
        30, "BACKFILL", "Historical dates require controlled reprocessing",
        [
            "Confirm requested start_date and end_date with the data owner",
            "Estimate source volume, DBU cost and affected Gold partitions",
            "Check that no overlapping incremental run is active",
        ],
        [
            "Run the Job with mode=backfill and explicit start_date/end_date",
            "Keep max_concurrent_runs=1 for state-mutating processing",
            "Use MERGE semantics in Silver and Gold and rebuild only affected aggregates",
        ],
        [
            "Reconcile daily row counts for every date in the requested range",
            "Confirm fact business keys remain unique",
            "Compare Gold totals before and after and obtain owner approval",
        ],
        "Escalate when the backfill changes closed financial reporting periods",
        "Data Engineer and Data Owner",
    ),
    (
        40, "QUARANTINE_REMEDIATION", "Invalid records require correction and replay",
        [
            "Group quarantine rows by dataset, error_code, batch_id and source_file",
            "Determine whether the issue is source data, schema drift, parsing, or business validation",
            "Measure quarantine ratio against the configured threshold",
        ],
        [
            "Fix the contract or source data without editing the immutable Bronze record",
            "Write corrected input as a new controlled batch or use replay mode",
            "Retain the original quarantine record and link it to the remediation run_id",
        ],
        [
            "Confirm corrected rows reached Silver",
            "Confirm no duplicate facts were created",
            "Update DQ evidence and close the incident",
        ],
        "Escalate when quarantine exceeds the contract threshold or affects critical fields",
        "Data Engineer and Source Owner",
    ),
    (
        50, "CHECKPOINT_RECOVERY", "Auto Loader checkpoint appears stale or damaged",
        [
            "Confirm the checkpoint path belongs to exactly one source stream",
            "Review recent offsets, commits and the last successful Bronze task",
            "Compare files in landing with _source_file values already written to Bronze",
            "Create a recoverable backup of checkpoint metadata before any intervention",
        ],
        [
            "Repair permissions or transient storage access first",
            "Restart with the same checkpoint when metadata is healthy",
            "Create a new checkpoint only under an approved replay plan with duplicate protection",
        ],
        [
            "Reconcile discovered files, processed files and Bronze row counts",
            "Confirm no historical files were silently skipped or duplicated",
        ],
        "Escalate before deleting, moving, or replacing any checkpoint directory",
        "Senior Data Engineer",
    ),
    (
        60, "SCHEMA_DRIFT", "An unexpected field or incompatible type appears",
        [
            "Inspect _rescued_data and the Auto Loader schema history",
            "Compare the new field with the source data contract and downstream dependencies",
            "Classify the change as additive, compatible cast, breaking, or corrupted input",
        ],
        [
            "Allow approved additive fields through the configured evolution mode",
            "Route incompatible values to quarantine rather than silently coercing them",
            "Version Silver transformations and update tests before publishing downstream",
        ],
        [
            "Run contract, type, nullability and row-count checks",
            "Confirm Gold schemas and BI queries remain compatible",
        ],
        "Escalate all breaking changes and unapproved sensitive fields",
        "Data Engineer and Data Owner",
    ),
    (
        70, "DATA_SKEW", "A few Spark tasks run substantially longer than the median",
        [
            "Compare maximum and median task duration in Spark UI or Query Profile",
            "Inspect partition sizes, shuffle read/write and spill",
            "Identify heavy-hitter keys such as HOT-SELLER",
        ],
        [
            "Apply early filtering and projection before joins",
            "Test AQE skew handling and pre-aggregation",
            "Split heavy hitters or apply salting only after skew is measured",
        ],
        [
            "Compare duration, shuffle, spill and max/median task ratio with the baseline",
            "Confirm optimized output is data-equivalent to the baseline",
        ],
        "Escalate when spill, executor loss, or SLA breach remains after measured remediation",
        "Data Engineer",
    ),
    (
        80, "GOLD_QUALITY", "Gold validation fails or a business metric is unexpected",
        [
            "Open ops.data_quality_results for the affected run_id",
            "Trace the metric from Gold facts to Silver business keys and Bronze source files",
            "Check uniqueness, referential integrity, negative amounts, statuses and quarantine ratio",
        ],
        [
            "Block publication for critical contract violations",
            "Correct Silver logic or affected source batches and rerun only impacted dates",
            "Rebuild aggregates from idempotent facts rather than incrementing incorrect totals",
        ],
        [
            "Re-run Gold DQ and reconciliation checks",
            "Compare GMV, order count, payment success and cancellation rate with prior periods",
            "Record approval before republishing",
        ],
        "Escalate material financial discrepancies or unexplained volume drops",
        "Data Engineer and Analytics Owner",
    ),
    (
        90, "DUPLICATE_DELIVERY", "The same source files or batch are delivered again",
        [
            "Compare file names, sizes, batch_id and manifest fingerprints",
            "Check whether Auto Loader already committed the files in its checkpoint",
            "Inspect business-key duplicates in Silver and Gold",
        ],
        [
            "Keep immutable landing evidence and do not rename duplicates to evade checkpointing",
            "Use batch registry and MERGE business keys to make replay idempotent",
            "Quarantine conflicting same-name files with different content",
        ],
        [
            "Confirm Bronze behavior matches the replay decision",
            "Confirm Silver and Gold row counts and aggregates are unchanged for identical delivery",
        ],
        "Escalate when duplicate delivery contains conflicting content",
        "Data Engineer and Source Owner",
    ),
    (
        100, "PARTIAL_JOB_RECOVERY", "The Job stopped after only part of the DAG completed",
        [
            "Identify successful, failed and skipped tasks in the Lakeflow run graph",
            "Read task-level audit rows and verify output commits for successful tasks",
            "Determine whether failure_handler completed",
        ],
        [
            "Use Repair run from the earliest failed task",
            "Do not rerun successful Bronze tasks with new checkpoints",
            "Rely on MERGE and business keys for repeatable Silver and Gold tasks",
        ],
        [
            "Confirm the final pipeline status and publication marker",
            "Run complete DQ and compare all task counts with the source manifest",
        ],
        "Escalate when task status disagrees with committed Delta output",
        "Senior Data Engineer",
    ),
]

runbook_schema = T.StructType([
    T.StructField("procedure_order", T.IntegerType(), False),
    T.StructField("incident_type", T.StringType(), False),
    T.StructField("symptom", T.StringType(), False),
    T.StructField("diagnosis_steps", T.ArrayType(T.StringType()), False),
    T.StructField("recovery_steps", T.ArrayType(T.StringType()), False),
    T.StructField("validation_steps", T.ArrayType(T.StringType()), False),
    T.StructField("escalation_condition", T.StringType(), False),
    T.StructField("owner_role", T.StringType(), False),
])

runbook_df = spark.createDataFrame(procedures, runbook_schema).select(
    F.sha2("incident_type", 256).alias("procedure_id"),
    "procedure_order", "incident_type", "symptom", "diagnosis_steps",
    "recovery_steps", "validation_steps", "escalation_condition", "owner_role",
    F.lit("ACTIVE").alias("procedure_status"),
    F.lit(NOW).cast("timestamp").alias("created_at"),
    F.lit(NOW).cast("timestamp").alias("updated_at"),
)
runbook_df.createOrReplaceTempView("incoming_stage14_runbook")

spark.sql(f"""
MERGE INTO {RUNBOOK} t
USING incoming_stage14_runbook s ON t.procedure_id=s.procedure_id
WHEN MATCHED THEN UPDATE SET
  t.procedure_order=s.procedure_order, t.symptom=s.symptom,
  t.diagnosis_steps=s.diagnosis_steps, t.recovery_steps=s.recovery_steps,
  t.validation_steps=s.validation_steps, t.escalation_condition=s.escalation_condition,
  t.owner_role=s.owner_role, t.procedure_status=s.procedure_status,
  t.updated_at=s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# 2. Document the COST_01 exception without hiding it

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {EXCEPTIONS}
(
    exception_id       STRING    NOT NULL,
    audit_run_id       STRING    NOT NULL,
    environment        STRING    NOT NULL,
    control_id         STRING    NOT NULL,
    disposition        STRING    NOT NULL,
    rationale          STRING    NOT NULL,
    compensating_controls ARRAY<STRING> NOT NULL,
    owner_identity     STRING    NOT NULL,
    accepted_at        TIMESTAMP NOT NULL,
    review_at          TIMESTAMP NOT NULL,
    exception_status   STRING    NOT NULL,
    created_at         TIMESTAMP NOT NULL,
    updated_at         TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Time-bounded production-hardening exceptions and compensating controls'
TBLPROPERTIES ('quality'='ops')
""")

exception_id = F.sha2(F.lit(f"{AUDIT_RUN_ID}|COST_01"), 256)
exception_df = spark.range(1).select(
    exception_id.alias("exception_id"),
    F.lit(AUDIT_RUN_ID).alias("audit_run_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    F.lit("COST_01").alias("control_id"),
    F.lit(COST01_DISPOSITION).alias("disposition"),
    F.lit(
        "Serverless usage policy is not attached or is unavailable in this workspace; "
        "the limitation is accepted for the laboratory environment only"
    ).alias("rationale"),
    F.array(
        F.lit("All pipeline tasks use serverless compute"),
        F.lit("max_concurrent_runs is limited to 1"),
        F.lit("Billing and performance metrics are retained in ops"),
        F.lit("Scheduled processing uses bounded AvailableNow execution"),
    ).alias("compensating_controls"),
    F.lit(CURRENT_USER).alias("owner_identity"),
    F.lit(NOW).cast("timestamp").alias("accepted_at"),
    F.lit(REVIEW_AT).cast("timestamp").alias("review_at"),
    F.lit("OPEN").alias("exception_status"),
    F.lit(NOW).cast("timestamp").alias("created_at"),
    F.lit(NOW).cast("timestamp").alias("updated_at"),
)
exception_df.createOrReplaceTempView("incoming_stage14_exception")

spark.sql(f"""
MERGE INTO {EXCEPTIONS} t
USING incoming_stage14_exception s ON t.exception_id=s.exception_id
WHEN MATCHED THEN UPDATE SET
  t.disposition=s.disposition, t.rationale=s.rationale,
  t.compensating_controls=s.compensating_controls,
  t.owner_identity=s.owner_identity, t.accepted_at=s.accepted_at,
  t.review_at=s.review_at, t.exception_status=s.exception_status,
  t.updated_at=s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"""
UPDATE {CHECKS}
SET actual_value=CONCAT(
      COALESCE(actual_value,''),
      '; documented_exception=', '{COST01_DISPOSITION}',
      '; review_at=', CAST(TIMESTAMP('{REVIEW_AT}') AS STRING)
    ),
    evidence='{EXCEPTIONS}',
    remediation='Review serverless Usage policy availability before the exception review date',
    updated_at=current_timestamp()
WHERE audit_run_id='{AUDIT_RUN_ID}' AND check_id='COST_01'
""")

# COMMAND ----------
# 3. Final readiness view and validation

spark.sql(f"""
CREATE OR REPLACE VIEW {FINAL_VIEW} AS
WITH latest AS (
  SELECT * FROM {RUNS}
  WHERE environment='{ENVIRONMENT}'
  QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC)=1
), exception_summary AS (
  SELECT audit_run_id,
    COUNT_IF(exception_status='OPEN') AS open_documented_exceptions,
    MIN(review_at) AS next_exception_review_at
  FROM {EXCEPTIONS}
  GROUP BY audit_run_id
)
SELECT
  l.*,
  COALESCE(e.open_documented_exceptions,0) AS open_documented_exceptions,
  e.next_exception_review_at,
  CASE
    WHEN l.failed_checks>0 THEN 'NOT_READY'
    WHEN l.run_status='READY' AND COALESCE(e.open_documented_exceptions,0)=0 THEN 'READY'
    WHEN l.run_status='READY' THEN 'READY_WITH_DOCUMENTED_EXCEPTION'
    ELSE l.run_status
  END AS final_readiness_status
FROM latest l
LEFT JOIN exception_summary e USING (audit_run_id)
""")

active_procedures = spark.table(RUNBOOK).filter(F.col("procedure_status") == "ACTIVE").count()
assert active_procedures >= len(procedures), "Runbook publication is incomplete"

display(spark.table(RUNBOOK).orderBy("procedure_order"))
display(spark.table(EXCEPTIONS).filter(F.col("audit_run_id") == AUDIT_RUN_ID))
display(spark.table(FINAL_VIEW))

final_row = spark.table(FINAL_VIEW).first()
print("Stage 14 production runbook and final report completed")
print(f"Audit run id: {AUDIT_RUN_ID}")
print(f"Active runbook procedures: {active_procedures}")
print(f"Passed checks: {final_row.passed_checks}/{final_row.total_checks}")
print(f"Failed checks: {final_row.failed_checks}")
print(f"Manual checks: {final_row.manual_checks}")
print(f"Readiness: {final_row.readiness_pct}%")
print(f"Documented exceptions: {final_row.open_documented_exceptions}")
print(f"Final readiness status: {final_row.final_readiness_status}")
print(f"Exception review at: {final_row.next_exception_review_at}")
