# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Lakeflow Job configuration audit
# MAGIC
# MAGIC Reads the Lakeflow Job definition through the Databricks SDK and validates
# MAGIC its production-hardening controls. No credentials or tokens are persisted.

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
    "required_parameters": "environment,load_date,mode,start_date,end_date,run_id,source_set,data_scale",
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
JOB_ID = int((dbutils.widgets.get("job_id") or "0").strip())
REQUIRED_PARAMETERS = {
    item.strip()
    for item in dbutils.widgets.get("required_parameters").split(",")
    if item.strip()
}

assert ENVIRONMENT in {"dev", "test", "prod"}
assert JOB_ID > 0, "Set job_id to the Lakeflow Job identifier"

OPS = f"{CATALOG}.ops"
RUNS = f"{OPS}.production_hardening_runs"
CHECKS = f"{OPS}.production_hardening_checks"
JOB_AUDIT = f"{OPS}.production_job_audit_results"

assert spark.catalog.tableExists(RUNS), f"Run notebook 140 first; missing {RUNS}"
assert spark.catalog.tableExists(CHECKS), f"Run notebook 140 first; missing {CHECKS}"

AUDIT_ID = f"job_audit_{ENVIRONMENT}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
CHECKED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------
# 1. Read the Job definition

workspace = WorkspaceClient()
job_definition = workspace.jobs.get(job_id=JOB_ID).as_dict()
settings = job_definition.get("settings") or {}
tasks = settings.get("tasks") or []

assert tasks, f"Job {JOB_ID} has no tasks"

job_name = settings.get("name") or job_definition.get("name") or str(JOB_ID)
max_concurrent_runs = settings.get("max_concurrent_runs")
run_as = job_definition.get("run_as") or settings.get("run_as") or {}
service_principal_name = run_as.get("service_principal_name")
run_as_user_name = run_as.get("user_name")

job_parameters = settings.get("parameters") or []
job_parameter_names = {
    row.get("name")
    for row in job_parameters
    if isinstance(row, dict) and row.get("name")
}

# Some pipeline parameters are intentionally resolved at task level. In
# particular, register_run publishes the business run_id and downstream tasks
# consume it through their notebook base_parameters. A Job-level parameter with
# the same name would override those task values, so both scopes are audited.
task_parameter_names = set()
for task in tasks:
    notebook_task = task.get("notebook_task") or {}
    task_parameter_names.update((notebook_task.get("base_parameters") or {}).keys())

    wheel_task = task.get("python_wheel_task") or {}
    task_parameter_names.update((wheel_task.get("named_parameters") or {}).keys())

parameter_names = job_parameter_names | task_parameter_names

# COMMAND ----------
# 2. Derive control-level evidence

task_keys = {task.get("task_key") for task in tasks if task.get("task_key")}


def dependency_keys(task):
    return {
        dependency.get("task_key")
        for dependency in (task.get("depends_on") or [])
        if dependency.get("task_key")
    }


def has_compute(task):
    explicit_compute_keys = {
        "existing_cluster_id", "job_cluster_key", "new_cluster",
        "environment_key", "sql_task", "pipeline_task",
    }
    if any(task.get(key) is not None for key in explicit_compute_keys):
        return True

    # A saved notebook task without a classic/job cluster reference is a valid
    # serverless notebook task. Jobs API does not expose a separate cluster
    # object for that execution mode, so absence of cluster fields is evidence
    # of inherited serverless compute rather than missing compute.
    return task.get("notebook_task") is not None


def task_timeout(task):
    return int(task.get("timeout_seconds") or 0)


def task_retries(task):
    return int(task.get("max_retries") or 0)


invalid_dependencies = []
for task in tasks:
    missing = sorted(dependency_keys(task) - task_keys)
    if missing:
        invalid_dependencies.append(f"{task.get('task_key')}->{','.join(missing)}")

tasks_without_timeout = sorted(
    task.get("task_key") for task in tasks if task_timeout(task) <= 0
)
tasks_without_retries = sorted(
    task.get("task_key") for task in tasks
    if task_retries(task) <= 0 and (task.get("run_if") or "ALL_SUCCESS") not in {"ALL_FAILED", "AT_LEAST_ONE_FAILED"}
)
tasks_without_compute = sorted(
    task.get("task_key") for task in tasks if not has_compute(task)
)
failure_handlers = sorted(
    task.get("task_key") for task in tasks
    if (task.get("run_if") or "ALL_SUCCESS") in {"ALL_FAILED", "AT_LEAST_ONE_FAILED"}
)
missing_parameters = sorted(REQUIRED_PARAMETERS - parameter_names)

identity_ok = bool(service_principal_name) and not run_as_user_name
retries_ok = not tasks_without_retries
timeouts_ok = not tasks_without_timeout
dependencies_ok = not invalid_dependencies
failure_handler_ok = bool(failure_handlers)
concurrency_ok = max_concurrent_runs == 1
parameters_ok = not missing_parameters
compute_ok = not tasks_without_compute

# Configuration fingerprint detects drift without storing the entire definition.
safe_fingerprint_payload = {
    "job_id": JOB_ID,
    "job_name": job_name,
    "max_concurrent_runs": max_concurrent_runs,
    "run_as_type": "service_principal" if service_principal_name else "user_or_unset",
    "job_parameter_names": sorted(job_parameter_names),
    "task_parameter_names": sorted(task_parameter_names),
    "tasks": [
        {
            "task_key": task.get("task_key"),
            "depends_on": sorted(dependency_keys(task)),
            "max_retries": task_retries(task),
            "timeout_seconds": task_timeout(task),
            "run_if": task.get("run_if") or "ALL_SUCCESS",
            "has_compute": has_compute(task),
        }
        for task in sorted(tasks, key=lambda item: item.get("task_key") or "")
    ],
}
definition_fingerprint = hashlib.sha256(
    json.dumps(safe_fingerprint_payload, sort_keys=True).encode("utf-8")
).hexdigest()

# COMMAND ----------
# 3. Persist granular audit results

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {JOB_AUDIT}
(
    result_id              STRING    NOT NULL,
    job_audit_id           STRING    NOT NULL,
    environment            STRING    NOT NULL,
    job_id                 BIGINT    NOT NULL,
    job_name               STRING,
    control_id             STRING    NOT NULL,
    control_name           STRING    NOT NULL,
    control_status         STRING    NOT NULL,
    actual_value           STRING,
    remediation            STRING,
    definition_fingerprint STRING    NOT NULL,
    checked_at             TIMESTAMP NOT NULL,
    created_at             TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Stage 14 Lakeflow Job configuration audit evidence'
TBLPROPERTIES ('quality' = 'ops')
""")


def status(value):
    return "PASSED" if value else "FAILED"


audit_rows = [
    ("IDENTITY_02", "Dedicated service principal configured", identity_ok,
     f"run_as={'service_principal' if service_principal_name else 'user_or_unset'}",
     "Set Job > Run as to the dedicated runtime service principal"),
    ("JOB_RETRIES", "Retries configured for processing tasks", retries_ok,
     f"tasks_without_retries={','.join(tasks_without_retries) or 'none'}",
     "Set max_retries to at least 1 for every processing task"),
    ("JOB_TIMEOUTS", "Timeout configured for every task", timeouts_ok,
     f"tasks_without_timeout={','.join(tasks_without_timeout) or 'none'}",
     "Set a finite timeout_seconds for every task"),
    ("JOB_DEPENDENCIES", "All task dependencies resolve", dependencies_ok,
     f"invalid_dependencies={';'.join(invalid_dependencies) or 'none'}",
     "Correct every depends_on task_key"),
    ("JOB_FAILURE_HANDLER", "Failure handler exists", failure_handler_ok,
     f"failure_handlers={','.join(failure_handlers) or 'none'}",
     "Add a task with run_if=AT_LEAST_ONE_FAILED or ALL_FAILED"),
    ("JOB_CONCURRENCY", "Concurrent state-mutating runs are prevented", concurrency_ok,
     f"max_concurrent_runs={max_concurrent_runs}",
     "Set max_concurrent_runs=1"),
    ("JOB_PARAMETERS", "Required parameters are declared", parameters_ok,
     f"missing_parameters={','.join(missing_parameters) or 'none'}; "
     f"job_scope={','.join(sorted(job_parameter_names)) or 'none'}; "
     f"task_scope={','.join(sorted(task_parameter_names)) or 'none'}",
     "Declare each Stage 8 parameter at Job or task scope; do not override task-value run_id"),
    ("JOB_COMPUTE", "Every task has an explicit execution environment", compute_ok,
     f"tasks_without_compute={','.join(tasks_without_compute) or 'none'}",
     "Assign serverless environment, job cluster, SQL warehouse, or pipeline"),
]

result_schema = """control_id string, control_name string, passed boolean,
                   actual_value string, remediation string"""
results_df = spark.createDataFrame(audit_rows, result_schema).select(
    F.sha2(F.concat_ws("|", F.lit(AUDIT_ID), "control_id"), 256).alias("result_id"),
    F.lit(AUDIT_ID).alias("job_audit_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    F.lit(JOB_ID).cast("long").alias("job_id"),
    F.lit(job_name).alias("job_name"),
    "control_id", "control_name",
    F.when(F.col("passed"), F.lit("PASSED")).otherwise(F.lit("FAILED")).alias("control_status"),
    "actual_value", "remediation",
    F.lit(definition_fingerprint).alias("definition_fingerprint"),
    F.lit(CHECKED_AT).cast("timestamp").alias("checked_at"),
    F.lit(CHECKED_AT).cast("timestamp").alias("created_at"),
)
results_df.write.format("delta").mode("append").saveAsTable(JOB_AUDIT)

# COMMAND ----------
# 4. Update the latest Stage 14 controls and score

latest_audit = spark.sql(f"""
SELECT audit_run_id
FROM {RUNS}
WHERE environment = '{ENVIRONMENT}'
QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
""").first()
assert latest_audit is not None, f"No Stage 14 audit exists for environment={ENVIRONMENT}"
HARDENING_AUDIT_RUN_ID = latest_audit.audit_run_id

job_control_ok = all([
    retries_ok, timeouts_ok, dependencies_ok, failure_handler_ok,
    concurrency_ok, parameters_ok, compute_ok,
])

control_updates = [
    (
        "IDENTITY_02", status(identity_ok),
        f"job_id={JOB_ID}; run_as={'service_principal' if service_principal_name else 'user_or_unset'}",
        "Set Job Run as to the dedicated runtime service principal" if not identity_ok else "Keep the service principal assignment under change control",
    ),
    (
        "JOB_01", status(job_control_ok),
        f"job_id={JOB_ID}; passed={sum([retries_ok, timeouts_ok, dependencies_ok, failure_handler_ok, concurrency_ok, parameters_ok, compute_ok])}/7; job_audit_id={AUDIT_ID}",
        "Open the granular job audit results and correct every FAILED control" if not job_control_ok else "Re-run this audit after every Job definition change",
    ),
]

for check_id, check_status, actual_value, remediation in control_updates:
    safe_actual = actual_value.replace("'", "''")
    safe_remediation = remediation.replace("'", "''")
    spark.sql(f"""
    UPDATE {CHECKS}
    SET check_status = '{check_status}',
        automation_level = 'AUTOMATED',
        actual_value = '{safe_actual}',
        evidence = '{JOB_AUDIT}; job_audit_id={AUDIT_ID}',
        remediation = '{safe_remediation}',
        checked_at = current_timestamp(),
        updated_at = current_timestamp()
    WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
      AND check_id = '{check_id}'
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
  r.notes = CONCAT('blocking_open=', s.blocking_open, '; Job audit=', '{AUDIT_ID}'),
  r.updated_at = current_timestamp()
""")

# COMMAND ----------
# 5. Operator output

display(
    results_df.select(
        "control_id", "control_name", "control_status", "actual_value", "remediation"
    ).orderBy("control_id")
)

display(spark.sql(f"""
SELECT check_id, check_status, actual_value, remediation
FROM {CHECKS}
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
  AND check_id IN ('IDENTITY_02', 'JOB_01')
ORDER BY check_id
"""))

display(spark.sql(f"""
SELECT *
FROM {OPS}.v_production_hardening_summary
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
"""))

failed_controls = [row[0] for row in audit_rows if not row[2]]

print("Stage 14 Lakeflow Job configuration audit completed")
print(f"Job audit id: {AUDIT_ID}")
print(f"Job id: {JOB_ID}")
print(f"Job name: {job_name}")
print(f"Tasks inspected: {len(tasks)}")
print(f"Run as: {'service_principal' if service_principal_name else 'user_or_unset'}")
print(f"Configuration fingerprint: {definition_fingerprint}")
print(f"Passed controls: {len(audit_rows) - len(failed_controls)}")
print(f"Failed controls: {len(failed_controls)}")
print(f"Failed control ids: {failed_controls}")
print(f"IDENTITY_02 status: {status(identity_ok)}")
print(f"JOB_01 status: {status(job_control_ok)}")
print("Next action: correct failed Job controls, rerun this notebook, then audit Unity Catalog grants")
