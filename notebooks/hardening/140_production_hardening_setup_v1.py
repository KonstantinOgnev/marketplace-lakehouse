# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Production hardening setup and baseline audit
# MAGIC
# MAGIC Creates durable readiness, configuration, test-run, and runbook storage.
# MAGIC Executes only checks that can be proven from the current notebook session;
# MAGIC UI/API-dependent controls are explicitly marked `MANUAL_REQUIRED`.

# COMMAND ----------

import uuid
from datetime import datetime, timezone
from decimal import Decimal

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
    "prod_catalog_name": "marketplace_prod",
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
PROD_CATALOG = (dbutils.widgets.get("prod_catalog_name") or "marketplace_prod").strip()

assert ENVIRONMENT in {"dev", "test", "prod"}
assert CATALOG and PROD_CATALOG

OPS = f"{CATALOG}.ops"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"
QUARANTINE = f"{CATALOG}.quarantine"

HARDENING_RUNS = f"{OPS}.production_hardening_runs"
HARDENING_CHECKS = f"{OPS}.production_hardening_checks"
CONFIG_REGISTRY = f"{OPS}.production_config_registry"
TEST_RUNS = f"{OPS}.production_hardening_test_runs"
RUNBOOK = f"{OPS}.production_runbook_procedures"
SUMMARY_VIEW = f"{OPS}.v_production_hardening_summary"
LATEST_VIEW = f"{OPS}.v_production_hardening_latest"

assert spark.catalog.databaseExists(OPS), f"Missing schema: {OPS}"

AUDIT_RUN_ID = f"hardening_{ENVIRONMENT}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
STARTED_AT = datetime.now(timezone.utc).replace(tzinfo=None)
CURRENT_IDENTITY = spark.sql("SELECT current_user() AS identity").first().identity

# COMMAND ----------
# 1. Durable Stage 14 objects

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HARDENING_RUNS}
(
    audit_run_id              STRING      NOT NULL,
    environment               STRING      NOT NULL,
    catalog_name              STRING      NOT NULL,
    execution_identity        STRING      NOT NULL,
    run_status                STRING      NOT NULL,
    total_checks              BIGINT,
    passed_checks             BIGINT,
    warning_checks            BIGINT,
    failed_checks             BIGINT,
    manual_checks             BIGINT,
    readiness_pct             DECIMAL(8,2),
    started_at                TIMESTAMP   NOT NULL,
    completed_at              TIMESTAMP,
    notes                     STRING,
    created_at                TIMESTAMP   NOT NULL,
    updated_at                TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Stage 14 production-hardening audit runs'
TBLPROPERTIES ('quality' = 'ops', 'delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {HARDENING_CHECKS}
(
    check_result_id           STRING      NOT NULL,
    audit_run_id              STRING      NOT NULL,
    environment               STRING      NOT NULL,
    check_id                  STRING      NOT NULL,
    control_domain            STRING      NOT NULL,
    check_name                STRING      NOT NULL,
    severity                  STRING      NOT NULL,
    automation_level          STRING      NOT NULL,
    check_status              STRING      NOT NULL,
    expected_value            STRING,
    actual_value              STRING,
    evidence                  STRING,
    remediation               STRING,
    blocking_for_prod         BOOLEAN     NOT NULL,
    checked_at                TIMESTAMP   NOT NULL,
    created_at                TIMESTAMP   NOT NULL,
    updated_at                TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Individual Stage 14 readiness-control outcomes'
TBLPROPERTIES ('quality' = 'ops', 'delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CONFIG_REGISTRY}
(
    config_id                 STRING      NOT NULL,
    environment               STRING      NOT NULL,
    config_key                STRING      NOT NULL,
    config_value              STRING,
    is_sensitive              BOOLEAN     NOT NULL,
    configuration_source      STRING      NOT NULL,
    validation_status         STRING      NOT NULL,
    description               STRING,
    created_at                TIMESTAMP   NOT NULL,
    updated_at                TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Non-secret environment configuration registry; secret values are forbidden'
TBLPROPERTIES ('quality' = 'ops')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TEST_RUNS}
(
    test_run_id               STRING      NOT NULL,
    audit_run_id              STRING      NOT NULL,
    test_scenario             STRING      NOT NULL,
    source_run_id             STRING,
    test_status               STRING      NOT NULL,
    expected_result           STRING,
    actual_result             STRING,
    evidence_json             STRING,
    started_at                TIMESTAMP,
    completed_at              TIMESTAMP,
    created_at                TIMESTAMP   NOT NULL,
    updated_at                TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Stage 14 idempotency, backfill, redelivery, drift and recovery tests'
TBLPROPERTIES ('quality' = 'ops')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RUNBOOK}
(
    procedure_id              STRING      NOT NULL,
    procedure_order           INT         NOT NULL,
    incident_type             STRING      NOT NULL,
    symptom                   STRING      NOT NULL,
    diagnosis_steps           ARRAY<STRING> NOT NULL,
    recovery_steps            ARRAY<STRING> NOT NULL,
    validation_steps          ARRAY<STRING> NOT NULL,
    escalation_condition      STRING      NOT NULL,
    owner_role                STRING      NOT NULL,
    procedure_status          STRING      NOT NULL,
    created_at                TIMESTAMP   NOT NULL,
    updated_at                TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'Operational runbook procedures for the Marketplace pipeline'
TBLPROPERTIES ('quality' = 'ops')
""")

# COMMAND ----------
# 2. Environment configuration without secrets

config_rows = [
    ("dev", "catalog_name", CATALOG, False, "JOB_PARAMETER", "ACTIVE", "Development Unity Catalog"),
    ("dev", "landing_root", f"/Volumes/{CATALOG}/bronze/landing", False, "DERIVED", "ACTIVE", "Development landing root"),
    ("dev", "checkpoint_root", f"/Volumes/{CATALOG}/ops/checkpoints/_autoloader/checkpoints", False, "DERIVED", "ACTIVE", "Independent Auto Loader checkpoints"),
    ("dev", "schema_root", f"/Volumes/{CATALOG}/ops/checkpoints/_autoloader/schemas", False, "DERIVED", "ACTIVE", "Independent Auto Loader schema locations"),
    ("dev", "max_concurrent_runs", "1", False, "JOB_SETTING", "RECOMMENDED", "Prevent overlapping production-state mutations"),
    ("dev", "default_mode", "incremental", False, "JOB_PARAMETER", "ACTIVE", "Default daily processing mode"),
    ("prod", "catalog_name", PROD_CATALOG, False, "DEPLOYMENT_PARAMETER", "PROPOSED", "Dedicated production Unity Catalog"),
    ("prod", "landing_root", f"/Volumes/{PROD_CATALOG}/bronze/landing", False, "DEPLOYMENT_PARAMETER", "PROPOSED", "Production landing root"),
    ("prod", "checkpoint_root", f"/Volumes/{PROD_CATALOG}/ops/checkpoints/_autoloader/checkpoints", False, "DEPLOYMENT_PARAMETER", "PROPOSED", "Production checkpoints isolated from dev"),
    ("prod", "schema_root", f"/Volumes/{PROD_CATALOG}/ops/checkpoints/_autoloader/schemas", False, "DEPLOYMENT_PARAMETER", "PROPOSED", "Production schemas isolated from dev"),
    ("prod", "max_concurrent_runs", "1", False, "JOB_SETTING", "RECOMMENDED", "Prevent overlapping production-state mutations"),
    ("prod", "default_mode", "incremental", False, "JOB_PARAMETER", "PROPOSED", "Default production mode"),
    ("prod", "run_as_identity", None, False, "LAKEFLOW_JOB_SETTING", "MANUAL_REQUIRED", "Dedicated runtime service principal application ID or display name"),
    ("prod", "budget_policy", None, False, "SERVERLESS_BUDGET_POLICY", "MANUAL_REQUIRED", "Serverless budget policy or compute policy"),
]

config_schema = """environment string, config_key string, config_value string,
                   is_sensitive boolean, configuration_source string,
                   validation_status string, description string"""
config_df = spark.createDataFrame(config_rows, config_schema).select(
    F.sha2(F.concat_ws("|", "environment", "config_key"), 256).alias("config_id"),
    "environment", "config_key", "config_value", "is_sensitive",
    "configuration_source", "validation_status", "description",
    F.lit(STARTED_AT).cast("timestamp").alias("created_at"),
    F.lit(STARTED_AT).cast("timestamp").alias("updated_at"),
)
config_df.createOrReplaceTempView("incoming_stage14_config")

spark.sql(f"""
MERGE INTO {CONFIG_REGISTRY} t
USING incoming_stage14_config s
  ON t.config_id = s.config_id
WHEN MATCHED THEN UPDATE SET
  t.config_value = s.config_value,
  t.is_sensitive = s.is_sensitive,
  t.configuration_source = s.configuration_source,
  t.validation_status = s.validation_status,
  t.description = s.description,
  t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# 3. Automated and manual-gate checks

check_rows = []


def add_check(
    check_id, domain, name, severity, automation, status,
    expected, actual, evidence, remediation, blocking,
):
    check_rows.append((
        check_id, domain, name, severity, automation, status,
        expected, actual, evidence, remediation, blocking,
    ))


def object_check(check_id, domain, object_name, severity="CRITICAL", blocking=True):
    exists = spark.catalog.tableExists(object_name)
    add_check(
        check_id, domain, f"Required object exists: {object_name}", severity,
        "AUTOMATED", "PASSED" if exists else "FAILED", "exists=true",
        f"exists={str(exists).lower()}", "spark.catalog.tableExists",
        f"Create or restore {object_name}", blocking,
    )


def schema_check(check_id, schema_name):
    exists = spark.catalog.databaseExists(schema_name)
    add_check(
        check_id, "DATA_GOVERNANCE", f"Required schema exists: {schema_name}",
        "CRITICAL", "AUTOMATED", "PASSED" if exists else "FAILED",
        "exists=true", f"exists={str(exists).lower()}",
        "spark.catalog.databaseExists", f"Create schema {schema_name}", True,
    )


for index, schema_name in enumerate([BRONZE, SILVER, GOLD, OPS, QUARANTINE], start=1):
    schema_check(f"SCHEMA_{index:02d}", schema_name)

required_tables = [
    f"{BRONZE}.orders_raw", f"{BRONZE}.payments_raw", f"{BRONZE}.products_raw",
    f"{BRONZE}.sellers_raw", f"{BRONZE}.user_events_raw",
    f"{GOLD}.fact_orders", f"{GOLD}.fact_payments", f"{GOLD}.fact_events",
    f"{OPS}.pipeline_runs", f"{OPS}.pipeline_task_runs",
    f"{OPS}.batch_registry", f"{OPS}.data_quality_results", f"{OPS}.table_metrics",
]

for index, table_name in enumerate(required_tables, start=1):
    object_check(f"OBJECT_{index:02d}", "PIPELINE_OBJECTS", table_name)

identity_is_human = "@" in CURRENT_IDENTITY
add_check(
    "IDENTITY_01", "IDENTITY", "Current execution identity captured",
    "INFO", "AUTOMATED", "PASSED", "non-empty identity", CURRENT_IDENTITY,
    "SELECT current_user()", "None", False,
)
add_check(
    "IDENTITY_02", "IDENTITY", "Production Lakeflow Job runs as a dedicated service principal",
    "CRITICAL", "MANUAL", "MANUAL_REQUIRED", "service principal configured in Run as",
    f"interactive identity={CURRENT_IDENTITY}; appears_human={identity_is_human}",
    "Interactive notebook identity cannot prove Lakeflow Job Run as configuration",
    "Open Lakeflow Job > Permissions/Run as; capture service principal and run evidence", True,
)

dataset_names = ["orders", "payments", "products", "sellers", "user_events"]
checkpoint_paths = [
    f"/Volumes/{CATALOG}/ops/checkpoints/_autoloader/checkpoints/{dataset}"
    for dataset in dataset_names
]
schema_paths = [
    f"/Volumes/{CATALOG}/ops/checkpoints/_autoloader/schemas/{dataset}"
    for dataset in dataset_names
]

unique_state_paths = len(set(checkpoint_paths + schema_paths)) == 10
add_check(
    "STATE_01", "STREAMING_STATE", "Checkpoint and schema paths are unique per source",
    "CRITICAL", "AUTOMATED", "PASSED" if unique_state_paths else "FAILED",
    "10 distinct paths", f"{len(set(checkpoint_paths + schema_paths))} distinct paths",
    "Derived from the production Bronze source configuration",
    "Assign a separate checkpointLocation and schemaLocation to every source", True,
)

def path_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


existing_checkpoints = sum(path_exists(path) for path in checkpoint_paths)
existing_schemas = sum(path_exists(path) for path in schema_paths)
state_status = "PASSED" if existing_checkpoints == 5 and existing_schemas == 5 else "WARNING"
add_check(
    "STATE_02", "STREAMING_STATE", "All source checkpoint and schema paths currently exist",
    "HIGH", "AUTOMATED", state_status, "5 checkpoints and 5 schema paths",
    f"checkpoints={existing_checkpoints}; schemas={existing_schemas}",
    "dbutils.fs.ls on explicit Volume paths",
    "Run each Bronze stream once or investigate the missing source state path", False,
)

recent_success_count = 0
if spark.catalog.tableExists(f"{OPS}.pipeline_runs"):
    run_df = spark.table(f"{OPS}.pipeline_runs")
    status_column = next(
        (name for name in ["run_status", "status", "pipeline_status"] if name in run_df.columns),
        None,
    )
    if status_column:
        recent_success_count = run_df.filter(
            F.upper(F.col(status_column)).isin("SUCCEEDED", "SUCCESS", "COMPLETED")
        ).count()

add_check(
    "RELIABILITY_01", "RELIABILITY", "At least one successful pipeline run is registered",
    "HIGH", "AUTOMATED", "PASSED" if recent_success_count > 0 else "WARNING",
    "> 0 successful runs", str(recent_success_count),
    f"{OPS}.pipeline_runs", "Execute and publish a complete Lakeflow Job run", False,
)

for check_id, name, remediation in [
    ("SECURITY_01", "Notebook source contains no plaintext secrets", "Run the Stage 14 source scanner and remove or rotate every finding"),
    ("SECURITY_02", "Secret scopes follow application roles and least privilege", "Review scope ACLs; grant READ only to the runtime service principal"),
    ("PERMISSION_01", "Runtime service principal has minimum Unity Catalog grants", "Capture SHOW GRANTS and remove ownership or MANAGE privileges not required at runtime"),
    ("JOB_01", "Retries, timeouts and failure handler are configured", "Export the Lakeflow Job definition and validate every task setting"),
    ("COST_01", "Compute or serverless budget policy is attached", "Attach an approved policy with worker, DBU and tagging limits"),
    ("COST_02", "Unused interactive compute terminates automatically", "Enable auto termination for classic interactive compute; prefer ephemeral Jobs compute"),
    ("CONFIG_01", "Dev and prod catalogs, paths and checkpoints are isolated", "Deploy the proposed prod configuration and validate that no dev path is referenced"),
]:
    add_check(
        check_id,
        "SECURITY" if check_id.startswith("SECURITY") or check_id.startswith("PERMISSION") else "JOB_CONFIGURATION",
        name, "CRITICAL" if check_id in {"SECURITY_01", "PERMISSION_01", "JOB_01"} else "HIGH",
        "MANUAL", "MANUAL_REQUIRED", "validated evidence", None,
        "Requires Workspace UI/API, exported notebook source, or administrator evidence",
        remediation, check_id in {"SECURITY_01", "PERMISSION_01", "JOB_01", "CONFIG_01"},
    )

check_schema = T.StructType([
    T.StructField("check_id", T.StringType(), False),
    T.StructField("control_domain", T.StringType(), False),
    T.StructField("check_name", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),
    T.StructField("automation_level", T.StringType(), False),
    T.StructField("check_status", T.StringType(), False),
    T.StructField("expected_value", T.StringType(), True),
    T.StructField("actual_value", T.StringType(), True),
    T.StructField("evidence", T.StringType(), True),
    T.StructField("remediation", T.StringType(), True),
    T.StructField("blocking_for_prod", T.BooleanType(), False),
])

checks_df = spark.createDataFrame(check_rows, check_schema).select(
    F.sha2(F.concat_ws("|", F.lit(AUDIT_RUN_ID), "check_id"), 256).alias("check_result_id"),
    F.lit(AUDIT_RUN_ID).alias("audit_run_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    "check_id", "control_domain", "check_name", "severity", "automation_level",
    "check_status", "expected_value", "actual_value", "evidence", "remediation",
    "blocking_for_prod",
    F.lit(STARTED_AT).cast("timestamp").alias("checked_at"),
    F.lit(STARTED_AT).cast("timestamp").alias("created_at"),
    F.lit(STARTED_AT).cast("timestamp").alias("updated_at"),
)
checks_df.write.format("delta").mode("append").saveAsTable(HARDENING_CHECKS)

# COMMAND ----------
# 4. Score and register the audit run

counts = (
    checks_df.groupBy("check_status").count()
    .collect()
)
status_counts = {row.check_status: int(row["count"]) for row in counts}
total_checks = len(check_rows)
passed_checks = status_counts.get("PASSED", 0)
warning_checks = status_counts.get("WARNING", 0)
failed_checks = status_counts.get("FAILED", 0)
manual_checks = status_counts.get("MANUAL_REQUIRED", 0)
readiness_pct = round(100.0 * passed_checks / total_checks, 2) if total_checks else 0.0

blocking_open = checks_df.filter(
    F.col("blocking_for_prod")
    & F.col("check_status").isin("FAILED", "MANUAL_REQUIRED")
).count()

audit_status = "BLOCKED" if failed_checks > 0 else "REVIEW_REQUIRED" if blocking_open > 0 else "READY"
COMPLETED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

run_schema = """audit_run_id string, environment string, catalog_name string,
                execution_identity string, run_status string, total_checks long,
                passed_checks long, warning_checks long, failed_checks long,
                manual_checks long, readiness_pct decimal(8,2), started_at timestamp,
                completed_at timestamp, notes string, created_at timestamp,
                updated_at timestamp"""

run_df = spark.createDataFrame([(
    AUDIT_RUN_ID, ENVIRONMENT, CATALOG, CURRENT_IDENTITY, audit_status,
    total_checks, passed_checks, warning_checks, failed_checks, manual_checks,
    Decimal(str(readiness_pct)), STARTED_AT, COMPLETED_AT,
    f"blocking_open={blocking_open}; automated baseline only",
    STARTED_AT, COMPLETED_AT,
)], run_schema)
run_df.write.format("delta").mode("append").saveAsTable(HARDENING_RUNS)

# COMMAND ----------
# 5. Operational views and validation

spark.sql(f"""
CREATE OR REPLACE VIEW {SUMMARY_VIEW} AS
SELECT
  r.audit_run_id,
  r.environment,
  r.catalog_name,
  r.execution_identity,
  r.run_status,
  r.total_checks,
  r.passed_checks,
  r.warning_checks,
  r.failed_checks,
  r.manual_checks,
  r.readiness_pct,
  COUNT_IF(c.blocking_for_prod AND c.check_status <> 'PASSED') AS open_blocking_controls,
  r.started_at,
  r.completed_at,
  r.notes
FROM {HARDENING_RUNS} r
JOIN {HARDENING_CHECKS} c USING (audit_run_id)
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {LATEST_VIEW} AS
WITH latest_run AS
(
  SELECT audit_run_id
  FROM {HARDENING_RUNS}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY environment ORDER BY started_at DESC) = 1
)
SELECT c.*
FROM {HARDENING_CHECKS} c
JOIN latest_run USING (audit_run_id)
""")

expected_objects = [
    HARDENING_RUNS, HARDENING_CHECKS, CONFIG_REGISTRY,
    TEST_RUNS, RUNBOOK, SUMMARY_VIEW, LATEST_VIEW,
]
failed_objects = [name for name in expected_objects if not spark.catalog.tableExists(name)]
assert not failed_objects, f"Failed Stage 14 objects: {failed_objects}"

display(
    checks_df.select(
        "control_domain", "check_id", "check_name", "severity",
        "automation_level", "check_status", "actual_value",
        "blocking_for_prod", "remediation",
    ).orderBy("control_domain", "check_id")
)
display(spark.table(SUMMARY_VIEW).filter(F.col("audit_run_id") == AUDIT_RUN_ID))
display(spark.table(CONFIG_REGISTRY).orderBy("environment", "config_key"))

print("Stage 14 production-hardening storage and baseline audit completed")
print(f"Audit run id: {AUDIT_RUN_ID}")
print(f"Execution identity: {CURRENT_IDENTITY}")
print(f"Audit status: {audit_status}")
print(f"Total checks: {total_checks}")
print(f"Passed checks: {passed_checks}")
print(f"Warnings: {warning_checks}")
print(f"Failed checks: {failed_checks}")
print(f"Manual checks: {manual_checks}")
print(f"Open blocking controls: {blocking_open}")
print(f"Readiness: {readiness_pct}%")
print(f"Failed objects: {len(failed_objects)}")
print("Next action: scan notebook sources for plaintext secrets and unsafe configuration")
