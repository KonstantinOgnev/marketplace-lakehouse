# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Secret scope ACL audit
# MAGIC
# MAGIC Discovers secret scopes referenced by pipeline source or Job settings and
# MAGIC validates their ACLs. Secret values are never requested, read, printed, or
# MAGIC persisted. If the pipeline references no scopes, the control is recorded
# MAGIC as passed/not-used.

# COMMAND ----------

import os
import re
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql import types as T


def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


current_user = spark.sql("SELECT current_user() AS user").first().user
default_root = f"/Workspace/Users/{current_user}"

for widget_name, default_value in {
    "catalog_name": "marketplace_lab",
    "environment": "dev",
    "job_id": "928771202769607",
    "workspace_root": default_root,
    "max_files": "10000",
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
JOB_ID = int((dbutils.widgets.get("job_id") or "0").strip())
WORKSPACE_ROOT = (dbutils.widgets.get("workspace_root") or default_root).strip().rstrip("/")
MAX_FILES = int(dbutils.widgets.get("max_files") or "10000")

assert ENVIRONMENT in {"dev", "test", "prod"}
assert JOB_ID > 0
assert WORKSPACE_ROOT.startswith("/Workspace/") and os.path.isdir(WORKSPACE_ROOT)
assert 1 <= MAX_FILES <= 100000

OPS = f"{CATALOG}.ops"
RUNS = f"{OPS}.production_hardening_runs"
CHECKS = f"{OPS}.production_hardening_checks"
RESULTS = f"{OPS}.production_secret_scope_audit_results"
REFERENCES = f"{OPS}.production_secret_scope_references"

assert spark.catalog.tableExists(RUNS), f"Run notebook 140 first; missing {RUNS}"
assert spark.catalog.tableExists(CHECKS), f"Run notebook 140 first; missing {CHECKS}"

AUDIT_ID = f"secret_acl_audit_{ENVIRONMENT}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
CHECKED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------
# 1. Resolve the Job runtime identity

workspace = WorkspaceClient()
job = workspace.jobs.get(job_id=JOB_ID).as_dict()
settings = job.get("settings") or {}
run_as = job.get("run_as") or settings.get("run_as") or {}
SERVICE_PRINCIPAL = run_as.get("service_principal_name")
assert SERVICE_PRINCIPAL, "The audited Job must run as a service principal"

# COMMAND ----------
# 2. Discover literal secret-scope references without reading secret values

TEXT_EXTENSIONS = {".py", ".sql", ".scala", ".r", ".sh", ".json", ".yaml", ".yml", ".conf", ".txt"}
SELF_NAME = "145_secret_scope_acl_audit_v1.py"

source_patterns = [
    (
        "DBUTILS_SECRET",
        re.compile(
            r"dbutils\.secrets\.(?:get|getBytes)\s*\(\s*"
            r"(?:scope\s*=\s*)?[\"']([^\"']+)[\"']",
            re.IGNORECASE,
        ),
    ),
    (
        "SQL_SECRET_FUNCTION",
        re.compile(r"\bsecret\s*\(\s*[\"']([^\"']+)[\"']", re.IGNORECASE),
    ),
    (
        "DYNAMIC_SECRET_REFERENCE",
        re.compile(r"\{\{secrets/([^/}\s]+)/[^}\s]+\}\}", re.IGNORECASE),
    ),
]

source_paths = []
for directory, child_dirs, filenames in os.walk(WORKSPACE_ROOT):
    child_dirs[:] = [name for name in child_dirs if name not in {".git", ".idea", "__pycache__", ".venv"}]
    for filename in filenames:
        if filename == SELF_NAME:
            continue
        if os.path.splitext(filename)[1].lower() in TEXT_EXTENSIONS:
            source_paths.append(os.path.join(directory, filename))
            if len(source_paths) >= MAX_FILES:
                break
    if len(source_paths) >= MAX_FILES:
        break

references = []
unreadable_files = []
for source_path in source_paths:
    try:
        with open(source_path, "r", encoding="utf-8", errors="replace") as handle:
            for line_number, line in enumerate(handle, start=1):
                for reference_type, pattern in source_patterns:
                    for match in pattern.finditer(line):
                        references.append((
                            match.group(1), "WORKSPACE_SOURCE", source_path,
                            line_number, reference_type,
                        ))
    except Exception as exc:
        unreadable_files.append(f"{source_path}:{type(exc).__name__}")

# Job settings can contain {{secrets/scope/key}} references. Search the in-memory
# definition, but persist only the scope and the configuration path.
def walk_job_settings(value, path="settings"):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from walk_job_settings(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from walk_job_settings(child, f"{path}[{index}]")
    elif isinstance(value, str):
        for match in source_patterns[2][1].finditer(value):
            yield match.group(1), "JOB_SETTING", path, 0, "DYNAMIC_SECRET_REFERENCE"


references.extend(walk_job_settings(settings))
references = sorted(set(references))
referenced_scopes = sorted({row[0] for row in references})
scan_complete = not unreadable_files and len(source_paths) < MAX_FILES

# COMMAND ----------
# 3. Inventory scopes and ACLs through the Secrets API

scope_inventory_error = None
try:
    available_scopes = {
        item.name for item in workspace.secrets.list_scopes()
        if item.name
    }
except Exception as exc:
    available_scopes = set()
    scope_inventory_error = f"{type(exc).__name__}: {str(exc)[:1000]}"

missing_scopes = sorted(set(referenced_scopes) - available_scopes)
acl_rows = []
acl_errors = []

for scope in referenced_scopes:
    if scope in missing_scopes:
        continue
    try:
        for acl in workspace.secrets.list_acls(scope=scope):
            acl_dict = acl.as_dict()
            acl_rows.append((
                scope,
                str(acl_dict.get("principal") or ""),
                str(acl_dict.get("permission") or "").upper(),
            ))
    except Exception as exc:
        acl_errors.append(f"{scope}:{type(exc).__name__}:{str(exc)[:500]}")

broad_principals = {"users", "all users", "account users", "admins"}
broad_acls = sorted({
    f"{scope}:{principal}:{permission}"
    for scope, principal, permission in acl_rows
    if principal.strip().lower() in broad_principals
    and permission in {"READ", "WRITE", "MANAGE"}
})

runtime_acl_by_scope = {
    scope: sorted({
        permission
        for acl_scope, principal, permission in acl_rows
        if acl_scope == scope and principal == SERVICE_PRINCIPAL
    })
    for scope in referenced_scopes
}
missing_runtime_read = sorted([
    scope for scope in referenced_scopes
    if "READ" not in runtime_acl_by_scope.get(scope, [])
])
elevated_runtime_scopes = sorted([
    scope for scope, permissions in runtime_acl_by_scope.items()
    if any(permission in {"WRITE", "MANAGE"} for permission in permissions)
])

# COMMAND ----------
# 4. Persist references and control evidence

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {REFERENCES}
(
    reference_id    STRING    NOT NULL,
    audit_id        STRING    NOT NULL,
    environment     STRING    NOT NULL,
    scope_name      STRING    NOT NULL,
    reference_source STRING   NOT NULL,
    source_path     STRING    NOT NULL,
    line_number     BIGINT    NOT NULL,
    reference_type  STRING    NOT NULL,
    detected_at     TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Secret-scope references only; secret keys and values are not persisted'
TBLPROPERTIES ('quality' = 'ops')
""")

reference_schema = """scope_name string, reference_source string, source_path string,
                      line_number long, reference_type string"""
references_df = spark.createDataFrame(references, reference_schema).select(
    F.sha2(F.concat_ws("|", F.lit(AUDIT_ID), "scope_name", "reference_source", "source_path", "line_number"), 256).alias("reference_id"),
    F.lit(AUDIT_ID).alias("audit_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    "scope_name", "reference_source", "source_path", "line_number", "reference_type",
    F.lit(CHECKED_AT).cast("timestamp").alias("detected_at"),
    F.lit(CHECKED_AT).cast("timestamp").alias("created_at"),
)
references_df.write.format("delta").mode("append").saveAsTable(REFERENCES)

control_rows = []


def add_control(control_id, name, status, actual, remediation):
    control_rows.append((control_id, name, status, actual, remediation))


add_control(
    "REFERENCE_SCAN", "Pipeline secret-scope references were scanned",
    "PASSED" if scan_complete else "MANUAL_REQUIRED",
    f"files={len(source_paths)}; references={len(references)}; unreadable={len(unreadable_files)}; max_reached={len(source_paths) >= MAX_FILES}",
    "Resolve unreadable files or increase max_files, then rerun",
)

inventory_ok = scope_inventory_error is None
add_control(
    "SCOPE_INVENTORY", "Workspace secret scopes can be inventoried",
    "PASSED" if inventory_ok else "MANUAL_REQUIRED",
    f"available_scopes={len(available_scopes)}" if inventory_ok else f"error={scope_inventory_error}",
    "Grant the auditor permission to list secret scopes or capture administrator evidence",
)

if not referenced_scopes:
    add_control(
        "PIPELINE_SCOPE_ACL", "Referenced scopes follow application least privilege",
        "PASSED", "referenced_scopes=none; control_result=NOT_USED",
        "Re-run after adding any dbutils.secrets or dynamic secret reference",
    )
else:
    acl_complete = not acl_errors and inventory_ok
    acl_ok = (
        acl_complete
        and not missing_scopes
        and not missing_runtime_read
        and not elevated_runtime_scopes
        and not broad_acls
    )
    acl_status = "PASSED" if acl_ok else "MANUAL_REQUIRED" if not acl_complete else "FAILED"
    add_control(
        "PIPELINE_SCOPE_ACL", "Referenced scopes follow application least privilege",
        acl_status,
        (
            f"referenced={','.join(referenced_scopes)}; "
            f"missing_scopes={','.join(missing_scopes) or 'none'}; "
            f"missing_runtime_read={','.join(missing_runtime_read) or 'none'}; "
            f"runtime_elevated={','.join(elevated_runtime_scopes) or 'none'}; "
            f"broad_acls={';'.join(broad_acls) or 'none'}; "
            f"acl_errors={'|'.join(acl_errors) or 'none'}"
        ),
        "Grant the runtime service principal READ only; remove broad and elevated ACLs",
    )

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULTS}
(
    result_id       STRING    NOT NULL,
    audit_id        STRING    NOT NULL,
    environment     STRING    NOT NULL,
    job_id          BIGINT    NOT NULL,
    runtime_principal STRING  NOT NULL,
    control_id      STRING    NOT NULL,
    control_name    STRING    NOT NULL,
    control_status  STRING    NOT NULL,
    actual_value    STRING,
    remediation     STRING,
    checked_at      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Stage 14 secret-scope reference and ACL audit results'
TBLPROPERTIES ('quality' = 'ops')
""")

control_schema = """control_id string, control_name string, control_status string,
                    actual_value string, remediation string"""
controls_df = spark.createDataFrame(control_rows, control_schema).select(
    F.sha2(F.concat_ws("|", F.lit(AUDIT_ID), "control_id"), 256).alias("result_id"),
    F.lit(AUDIT_ID).alias("audit_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    F.lit(JOB_ID).cast("long").alias("job_id"),
    F.lit(SERVICE_PRINCIPAL).alias("runtime_principal"),
    "control_id", "control_name", "control_status", "actual_value", "remediation",
    F.lit(CHECKED_AT).cast("timestamp").alias("checked_at"),
    F.lit(CHECKED_AT).cast("timestamp").alias("created_at"),
)
controls_df.write.format("delta").mode("append").saveAsTable(RESULTS)

# COMMAND ----------
# 5. Update SECURITY_02 and readiness

latest_audit = spark.sql(f"""
SELECT audit_run_id FROM {RUNS}
WHERE environment = '{ENVIRONMENT}'
QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
""").first()
assert latest_audit is not None
HARDENING_AUDIT_RUN_ID = latest_audit.audit_run_id

statuses = [row[2] for row in control_rows]
if "FAILED" in statuses:
    security_status = "FAILED"
elif "MANUAL_REQUIRED" in statuses:
    security_status = "MANUAL_REQUIRED"
else:
    security_status = "PASSED"

security_actual = (
    f"audit_id={AUDIT_ID}; referenced_scopes={','.join(referenced_scopes) or 'none'}; "
    f"controls_passed={statuses.count('PASSED')}/{len(statuses)}"
).replace("'", "''")

spark.sql(f"""
UPDATE {CHECKS}
SET check_status = '{security_status}', automation_level = 'AUTOMATED',
    actual_value = '{security_actual}',
    evidence = '{RESULTS}; audit_id={AUDIT_ID}',
    remediation = 'Review the detailed scope ACL controls and apply least privilege',
    checked_at = current_timestamp(), updated_at = current_timestamp()
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}' AND check_id = 'SECURITY_02'
""")

spark.sql(f"""
MERGE INTO {RUNS} r
USING (
  SELECT audit_run_id, COUNT(*) total_checks,
    COUNT_IF(check_status='PASSED') passed_checks,
    COUNT_IF(check_status='WARNING') warning_checks,
    COUNT_IF(check_status='FAILED') failed_checks,
    COUNT_IF(check_status='MANUAL_REQUIRED') manual_checks,
    CAST(ROUND(100.0*COUNT_IF(check_status='PASSED')/COUNT(*),2) AS DECIMAL(8,2)) readiness_pct,
    COUNT_IF(blocking_for_prod AND check_status<>'PASSED') blocking_open
  FROM {CHECKS} WHERE audit_run_id='{HARDENING_AUDIT_RUN_ID}' GROUP BY audit_run_id
) s ON r.audit_run_id=s.audit_run_id
WHEN MATCHED THEN UPDATE SET
  r.total_checks=s.total_checks, r.passed_checks=s.passed_checks,
  r.warning_checks=s.warning_checks, r.failed_checks=s.failed_checks,
  r.manual_checks=s.manual_checks, r.readiness_pct=s.readiness_pct,
  r.run_status=CASE WHEN s.failed_checks>0 THEN 'BLOCKED'
                    WHEN s.blocking_open>0 THEN 'REVIEW_REQUIRED'
                    ELSE 'READY' END,
  r.notes=CONCAT('blocking_open=',s.blocking_open,'; Secret ACL audit=','{AUDIT_ID}'),
  r.updated_at=current_timestamp()
""")

# COMMAND ----------
# 6. Operator output

display(controls_df.select(
    "control_id", "control_name", "control_status", "actual_value", "remediation"
).orderBy("control_id"))

display(references_df.select(
    "scope_name", "reference_source", "source_path", "line_number", "reference_type"
).orderBy("scope_name", "source_path", "line_number"))

display(spark.sql(f"""
SELECT check_id, check_status, actual_value, remediation FROM {CHECKS}
WHERE audit_run_id='{HARDENING_AUDIT_RUN_ID}' AND check_id='SECURITY_02'
"""))

display(spark.sql(f"""
SELECT * FROM {OPS}.v_production_hardening_summary
WHERE audit_run_id='{HARDENING_AUDIT_RUN_ID}'
"""))

print("Stage 14 secret scope ACL audit completed")
print(f"Audit id: {AUDIT_ID}")
print(f"Runtime principal: {SERVICE_PRINCIPAL}")
print(f"Files scanned: {len(source_paths)}")
print(f"Referenced scopes: {referenced_scopes}")
print(f"Available scopes: {len(available_scopes)}")
print(f"ACL rows inspected: {len(acl_rows)}")
print(f"SECURITY_02 status: {security_status}")
print("Next action: document COST_01 disposition and generate the Stage 14 runbook")
