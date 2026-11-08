# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Unity Catalog grants audit
# MAGIC
# MAGIC Resolves the Lakeflow Job service principal from `Run as`, captures its
# MAGIC Unity Catalog grants, checks required pipeline access and flags excessive
# MAGIC administrative privileges. No credentials or secret values are stored.

# COMMAND ----------

import hashlib
import json
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
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
    "job_id": "928771202769607",
    "required_schemas": "bronze,silver,gold,ops,quarantine",
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
JOB_ID = int((dbutils.widgets.get("job_id") or "0").strip())
REQUIRED_SCHEMAS = [
    value.strip()
    for value in dbutils.widgets.get("required_schemas").split(",")
    if value.strip()
]

assert ENVIRONMENT in {"dev", "test", "prod"}
assert CATALOG and JOB_ID > 0 and REQUIRED_SCHEMAS

OPS = f"{CATALOG}.ops"
RUNS = f"{OPS}.production_hardening_runs"
CHECKS = f"{OPS}.production_hardening_checks"
GRANT_AUDIT = f"{OPS}.production_permission_audit_results"
GRANT_SNAPSHOT = f"{OPS}.production_permission_grant_snapshot"

assert spark.catalog.tableExists(RUNS), f"Run notebook 140 first; missing {RUNS}"
assert spark.catalog.tableExists(CHECKS), f"Run notebook 140 first; missing {CHECKS}"

AUDIT_ID = f"grant_audit_{ENVIRONMENT}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
CHECKED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------
# 1. Resolve the runtime principal from the audited Job

workspace = WorkspaceClient()
job_definition = workspace.jobs.get(job_id=JOB_ID).as_dict()
settings = job_definition.get("settings") or {}
run_as = job_definition.get("run_as") or settings.get("run_as") or {}
SERVICE_PRINCIPAL = run_as.get("service_principal_name")

assert SERVICE_PRINCIPAL, (
    f"Job {JOB_ID} is not configured with run_as.service_principal_name; "
    "complete notebook 142 remediation first"
)


def quote_identifier(value):
    return "`" + value.replace("`", "``") + "`"


principal_sql = quote_identifier(SERVICE_PRINCIPAL)

# COMMAND ----------
# 2. Capture the grants visible to the current auditor

grant_query_errors = []
raw_grant_rows = []

# SHOW GRANTS is scoped to a securable object in Databricks SQL. Query the
# catalog and every required schema; schema results include privileges that
# affect that object, including inherited catalog grants.
grant_targets = [
    ("CATALOG", quote_identifier(CATALOG), CATALOG),
] + [
    (
        "SCHEMA",
        f"{quote_identifier(CATALOG)}.{quote_identifier(schema_name)}",
        f"{CATALOG}.{schema_name}",
    )
    for schema_name in REQUIRED_SCHEMAS
]

for object_type, object_sql, object_name in grant_targets:
    try:
        target_df = spark.sql(
            f"SHOW GRANTS {principal_sql} ON {object_type} {object_sql}"
        )
        raw_grant_rows.extend(row.asDict(recursive=True) for row in target_df.collect())
    except Exception as exc:
        grant_query_errors.append(
            f"{object_type} {object_name}: {type(exc).__name__}: {str(exc)[:500]}"
        )


def normalized_key_map(row):
    return {
        key.lower().replace("_", "").replace(" ", ""): value
        for key, value in row.items()
    }


def first_value(row, candidates):
    normalized = normalized_key_map(row)
    for candidate in candidates:
        key = candidate.lower().replace("_", "").replace(" ", "")
        if key in normalized:
            return normalized[key]
    return None


def normalize_action(value):
    return str(value or "").strip().upper().replace("_", " ")


def normalize_type(value):
    return str(value or "").strip().upper().replace("_", " ")


grants = []
seen_grants = set()
for raw in raw_grant_rows:
    principal = first_value(raw, ["principal"])
    action = normalize_action(first_value(raw, ["actionType", "action_type", "privilege"]));
    object_type = normalize_type(first_value(raw, ["objectType", "object_type", "securable_type"]));
    object_key = str(first_value(raw, ["objectKey", "object_key", "securable_name", "object_name"]) or "")
    inherited_from = str(first_value(raw, ["inheritedFrom", "inherited_from"]) or "")
    grant_key = (action, object_type, object_key, inherited_from)
    if grant_key in seen_grants:
        continue
    seen_grants.add(grant_key)
    grants.append({
        "principal": str(principal or SERVICE_PRINCIPAL),
        "action": action,
        "object_type": object_type,
        "object_key": object_key,
        "inherited_from": inherited_from,
    })

# COMMAND ----------
# 3. Persist a normalized, non-secret permission snapshot

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GRANT_SNAPSHOT}
(
    grant_snapshot_id STRING    NOT NULL,
    grant_audit_id    STRING    NOT NULL,
    environment       STRING    NOT NULL,
    job_id            BIGINT    NOT NULL,
    principal         STRING    NOT NULL,
    action_type       STRING    NOT NULL,
    object_type       STRING,
    object_key        STRING,
    inherited_from    STRING,
    captured_at       TIMESTAMP NOT NULL,
    created_at        TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Normalized Unity Catalog grants captured for Stage 14 least-privilege audits'
TBLPROPERTIES ('quality' = 'ops')
""")

snapshot_schema = T.StructType([
    T.StructField("grant_snapshot_id", T.StringType(), False),
    T.StructField("grant_audit_id", T.StringType(), False),
    T.StructField("environment", T.StringType(), False),
    T.StructField("job_id", T.LongType(), False),
    T.StructField("principal", T.StringType(), False),
    T.StructField("action_type", T.StringType(), False),
    T.StructField("object_type", T.StringType(), True),
    T.StructField("object_key", T.StringType(), True),
    T.StructField("inherited_from", T.StringType(), True),
    T.StructField("captured_at", T.TimestampType(), False),
    T.StructField("created_at", T.TimestampType(), False),
])

snapshot_rows = []
for grant in grants:
    fingerprint = hashlib.sha256(
        f"{AUDIT_ID}|{grant['action']}|{grant['object_type']}|{grant['object_key']}".encode("utf-8")
    ).hexdigest()
    snapshot_rows.append((
        fingerprint, AUDIT_ID, ENVIRONMENT, JOB_ID, SERVICE_PRINCIPAL,
        grant["action"], grant["object_type"], grant["object_key"],
        grant["inherited_from"], CHECKED_AT, CHECKED_AT,
    ))

snapshot_df = spark.createDataFrame(snapshot_rows, snapshot_schema)
snapshot_df.write.format("delta").mode("append").saveAsTable(GRANT_SNAPSHOT)

# COMMAND ----------
# 4. Evaluate effective access and least privilege


def object_matches(grant, object_type, object_key):
    actual_type = grant["object_type"]
    actual_key = grant["object_key"].lower()
    expected_key = object_key.lower()
    return actual_type == object_type and (
        actual_key == expected_key
        or actual_key.endswith("." + expected_key)
    )


def has_action(action, object_type, object_key):
    wanted = normalize_action(action)
    for grant in grants:
        if grant["action"] not in {wanted, "ALL PRIVILEGES"}:
            continue
        if object_matches(grant, object_type, object_key):
            return True

        # Catalog grants are inherited by child schemas and objects.
        if object_type != "CATALOG" and object_matches(grant, "CATALOG", CATALOG):
            return True
    return False


control_rows = []


def add_control(control_id, name, passed, actual, remediation, severity="CRITICAL"):
    control_rows.append((control_id, name, severity, passed, actual, remediation))


grant_capture_ok = len(grant_query_errors) == 0
add_control(
    "GRANT_CAPTURE", "Unity Catalog grants can be inspected", grant_capture_ok,
    f"grant_rows={len(grants)}" if grant_capture_ok else f"errors={' | '.join(grant_query_errors)}",
    "Grant the auditor permission to inspect privileges and rerun this notebook",
)

use_catalog_ok = has_action("USE CATALOG", "CATALOG", CATALOG)
add_control(
    "USE_CATALOG", f"Runtime principal can use catalog {CATALOG}", use_catalog_ok,
    f"use_catalog={use_catalog_ok}",
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO the runtime service principal",
)

schema_access = {}
for schema_name in REQUIRED_SCHEMAS:
    full_schema = f"{CATALOG}.{schema_name}"
    schema_access[schema_name] = has_action("USE SCHEMA", "SCHEMA", full_schema)
    add_control(
        f"USE_SCHEMA_{schema_name.upper()}",
        f"Runtime principal can use schema {full_schema}",
        schema_access[schema_name],
        f"use_schema={schema_access[schema_name]}",
        f"GRANT USE SCHEMA ON SCHEMA {full_schema} TO the runtime service principal",
    )

# The ETL principal creates or replaces managed Delta objects in these schemas.
write_schema_access = {}
for schema_name in REQUIRED_SCHEMAS:
    full_schema = f"{CATALOG}.{schema_name}"
    checks = {
        "CREATE TABLE": has_action("CREATE TABLE", "SCHEMA", full_schema),
        "SELECT": has_action("SELECT", "SCHEMA", full_schema),
        "MODIFY": has_action("MODIFY", "SCHEMA", full_schema),
    }
    write_schema_access[schema_name] = all(checks.values())
    missing = [action for action, exists in checks.items() if not exists]
    add_control(
        f"WRITE_SCHEMA_{schema_name.upper()}",
        f"Runtime principal has ETL rights in {full_schema}",
        write_schema_access[schema_name],
        f"missing={','.join(missing) or 'none'}",
        f"Grant only missing CREATE TABLE, SELECT and MODIFY privileges on {full_schema}",
    )

forbidden_actions = {
    "MANAGE", "CREATE CATALOG", "CREATE EXTERNAL LOCATION",
    "CREATE CONNECTION", "SET SHARE PERMISSION",
}
excessive_grants = [
    grant for grant in grants
    if grant["action"] in forbidden_actions
    or (grant["action"] == "ALL PRIVILEGES" and object_matches(grant, "CATALOG", CATALOG))
]
least_privilege_ok = grant_capture_ok and len(excessive_grants) == 0
excessive_summary = sorted({
    f"{grant['action']} ON {grant['object_type']} {grant['object_key']}"
    for grant in excessive_grants
})
add_control(
    "NO_EXCESSIVE_GRANTS", "Runtime principal has no administrative catalog privileges",
    least_privilege_ok,
    f"excessive={'; '.join(excessive_summary) or 'none'}",
    "Revoke MANAGE, catalog-level ALL PRIVILEGES and account-level object-creation privileges",
)

# COMMAND ----------
# 5. Persist audit controls

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GRANT_AUDIT}
(
    result_id       STRING    NOT NULL,
    grant_audit_id  STRING    NOT NULL,
    environment     STRING    NOT NULL,
    job_id          BIGINT    NOT NULL,
    principal       STRING    NOT NULL,
    control_id      STRING    NOT NULL,
    control_name    STRING    NOT NULL,
    severity        STRING    NOT NULL,
    control_status  STRING    NOT NULL,
    actual_value    STRING,
    remediation     STRING,
    checked_at      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Stage 14 least-privilege audit controls for the Lakeflow runtime principal'
TBLPROPERTIES ('quality' = 'ops')
""")

control_schema = """control_id string, control_name string, severity string,
                    passed boolean, actual_value string, remediation string"""
controls_df = spark.createDataFrame(control_rows, control_schema).select(
    F.sha2(F.concat_ws("|", F.lit(AUDIT_ID), "control_id"), 256).alias("result_id"),
    F.lit(AUDIT_ID).alias("grant_audit_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    F.lit(JOB_ID).cast("long").alias("job_id"),
    F.lit(SERVICE_PRINCIPAL).alias("principal"),
    "control_id", "control_name", "severity",
    F.when(F.col("passed"), "PASSED").otherwise("FAILED").alias("control_status"),
    "actual_value", "remediation",
    F.lit(CHECKED_AT).cast("timestamp").alias("checked_at"),
    F.lit(CHECKED_AT).cast("timestamp").alias("created_at"),
)
controls_df.write.format("delta").mode("append").saveAsTable(GRANT_AUDIT)

# COMMAND ----------
# 6. Update PERMISSION_01 and the readiness score

latest_audit = spark.sql(f"""
SELECT audit_run_id
FROM {RUNS}
WHERE environment = '{ENVIRONMENT}'
QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
""").first()
assert latest_audit is not None, f"No Stage 14 audit exists for environment={ENVIRONMENT}"
HARDENING_AUDIT_RUN_ID = latest_audit.audit_run_id

failed_controls = [row[0] for row in control_rows if not row[3]]
permission_ok = not failed_controls

if not grant_capture_ok:
    permission_status = "MANUAL_REQUIRED"
elif permission_ok:
    permission_status = "PASSED"
else:
    permission_status = "FAILED"

actual_value = (
    f"grant_audit_id={AUDIT_ID}; grants={len(grants)}; "
    f"passed={len(control_rows) - len(failed_controls)}/{len(control_rows)}; "
    f"failed={','.join(failed_controls) or 'none'}"
).replace("'", "''")

remediation = (
    "Review failed grant controls and apply only the listed missing or revocation actions"
    if failed_controls else
    "Re-run after every runtime identity or Unity Catalog grant change"
).replace("'", "''")

spark.sql(f"""
UPDATE {CHECKS}
SET check_status = '{permission_status}',
    automation_level = 'AUTOMATED',
    actual_value = '{actual_value}',
    evidence = '{GRANT_AUDIT}; grant_audit_id={AUDIT_ID}',
    remediation = '{remediation}',
    checked_at = current_timestamp(),
    updated_at = current_timestamp()
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
  AND check_id = 'PERMISSION_01'
""")

spark.sql(f"""
MERGE INTO {RUNS} r
USING (
  SELECT
    audit_run_id,
    COUNT(*) AS total_checks,
    COUNT_IF(check_status = 'PASSED') AS passed_checks,
    COUNT_IF(check_status = 'WARNING') AS warning_checks,
    COUNT_IF(check_status = 'FAILED') AS failed_checks,
    COUNT_IF(check_status = 'MANUAL_REQUIRED') AS manual_checks,
    CAST(ROUND(100.0 * COUNT_IF(check_status = 'PASSED') / COUNT(*), 2) AS DECIMAL(8,2)) AS readiness_pct,
    COUNT_IF(blocking_for_prod AND check_status <> 'PASSED') AS blocking_open
  FROM {CHECKS}
  WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
  GROUP BY audit_run_id
) s
ON r.audit_run_id = s.audit_run_id
WHEN MATCHED THEN UPDATE SET
  r.total_checks = s.total_checks,
  r.passed_checks = s.passed_checks,
  r.warning_checks = s.warning_checks,
  r.failed_checks = s.failed_checks,
  r.manual_checks = s.manual_checks,
  r.readiness_pct = s.readiness_pct,
  r.run_status = CASE
    WHEN s.failed_checks > 0 THEN 'BLOCKED'
    WHEN s.blocking_open > 0 THEN 'REVIEW_REQUIRED'
    ELSE 'READY'
  END,
  r.notes = CONCAT('blocking_open=', s.blocking_open, '; Grant audit=', '{AUDIT_ID}'),
  r.updated_at = current_timestamp()
""")

# COMMAND ----------
# 7. Operator output

display(
    controls_df.select(
        "control_id", "control_name", "severity", "control_status",
        "actual_value", "remediation",
    ).orderBy("control_id")
)

display(
    snapshot_df.select("action_type", "object_type", "object_key", "inherited_from")
    .orderBy("object_type", "object_key", "action_type")
)

display(spark.sql(f"""
SELECT check_id, check_status, actual_value, remediation
FROM {CHECKS}
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
  AND check_id = 'PERMISSION_01'
"""))

display(spark.sql(f"""
SELECT *
FROM {OPS}.v_production_hardening_summary
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
"""))

print("Stage 14 Unity Catalog grants audit completed")
print(f"Grant audit id: {AUDIT_ID}")
print(f"Job id: {JOB_ID}")
print(f"Runtime principal: {SERVICE_PRINCIPAL}")
print(f"Captured grants: {len(grants)}")
print(f"Passed controls: {len(control_rows) - len(failed_controls)}")
print(f"Failed controls: {len(failed_controls)}")
print(f"Failed control ids: {failed_controls}")
print(f"PERMISSION_01 status: {permission_status}")
print(
    "Next action: apply only the listed grant changes and rerun this notebook"
    if failed_controls else
    "Next action: validate dev/prod isolation and cost controls"
)
