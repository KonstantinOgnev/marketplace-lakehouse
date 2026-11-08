# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Environment isolation and cost controls audit
# MAGIC
# MAGIC Validates dev/prod configuration separation and evaluates cost controls
# MAGIC for the Lakeflow Job. The notebook records evidence but does not modify
# MAGIC the Job, catalogs, paths, schedules, or policies.

# COMMAND ----------

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
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
JOB_ID = int((dbutils.widgets.get("job_id") or "0").strip())

assert ENVIRONMENT in {"dev", "test", "prod"}
assert CATALOG and JOB_ID > 0

OPS = f"{CATALOG}.ops"
RUNS = f"{OPS}.production_hardening_runs"
CHECKS = f"{OPS}.production_hardening_checks"
CONFIG = f"{OPS}.production_config_registry"
RESULTS = f"{OPS}.production_environment_cost_audit_results"

for required_table in [RUNS, CHECKS, CONFIG]:
    assert spark.catalog.tableExists(required_table), f"Missing {required_table}; run notebook 140 first"

AUDIT_ID = f"env_cost_audit_{ENVIRONMENT}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
CHECKED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------
# 1. Load non-secret environment configuration

config_rows = (
    spark.table(CONFIG)
    .filter(F.col("environment").isin("dev", "prod"))
    .select("environment", "config_key", "config_value", "validation_status")
    .collect()
)

configs = {"dev": {}, "prod": {}}
statuses = {"dev": {}, "prod": {}}
for row in config_rows:
    configs[row.environment][row.config_key] = row.config_value
    statuses[row.environment][row.config_key] = row.validation_status

isolation_keys = ["catalog_name", "landing_root", "checkpoint_root", "schema_root"]
missing_config = [
    f"{env}.{key}"
    for env in ["dev", "prod"]
    for key in isolation_keys
    if not configs[env].get(key)
]
equal_config = [
    key for key in isolation_keys
    if configs["dev"].get(key) == configs["prod"].get(key)
]

dev_catalog = configs["dev"].get("catalog_name") or ""
prod_catalog = configs["prod"].get("catalog_name") or ""
prod_path_keys = ["landing_root", "checkpoint_root", "schema_root"]
prod_paths_reference_dev = [
    key for key in prod_path_keys
    if dev_catalog and dev_catalog.lower() in (configs["prod"].get(key) or "").lower()
]
prod_paths_reference_prod = all(
    prod_catalog and prod_catalog.lower() in (configs["prod"].get(key) or "").lower()
    for key in prod_path_keys
)

config_isolation_ok = (
    not missing_config
    and not equal_config
    and not prod_paths_reference_dev
    and prod_paths_reference_prod
)

# COMMAND ----------
# 2. Inspect Job compute and policy settings

workspace = WorkspaceClient()
job = workspace.jobs.get(job_id=JOB_ID).as_dict()
settings = job.get("settings") or {}
tasks = settings.get("tasks") or []
assert tasks, f"Job {JOB_ID} has no tasks"


def task_compute_mode(task):
    if task.get("existing_cluster_id"):
        return "EXISTING_CLUSTER"
    if task.get("new_cluster") or task.get("job_cluster_key"):
        return "JOBS_COMPUTE"
    if task.get("sql_task"):
        return "SQL_WAREHOUSE"
    if task.get("pipeline_task"):
        return "PIPELINE"
    if task.get("notebook_task"):
        return "SERVERLESS_NOTEBOOK"
    if task.get("environment_key"):
        return "SERVERLESS_ENVIRONMENT"
    return "UNKNOWN"


compute_modes = {
    task.get("task_key"): task_compute_mode(task)
    for task in tasks
}
existing_cluster_ids = sorted({
    task.get("existing_cluster_id")
    for task in tasks
    if task.get("existing_cluster_id")
})

cluster_autotermination = {}
cluster_errors = []
for cluster_id in existing_cluster_ids:
    try:
        cluster = workspace.clusters.get(cluster_id=cluster_id).as_dict()
        cluster_autotermination[cluster_id] = int(cluster.get("autotermination_minutes") or 0)
    except Exception as exc:
        cluster_errors.append(f"{cluster_id}:{type(exc).__name__}")

all_serverless_or_ephemeral = all(
    mode in {"SERVERLESS_NOTEBOOK", "SERVERLESS_ENVIRONMENT", "JOBS_COMPUTE", "SQL_WAREHOUSE", "PIPELINE"}
    for mode in compute_modes.values()
)
classic_autotermination_ok = all(value > 0 for value in cluster_autotermination.values())
resource_shutdown_ok = (
    all_serverless_or_ephemeral
    and classic_autotermination_ok
    and not cluster_errors
)


def find_policy_fields(value, prefix=""):
    found = []
    if isinstance(value, dict):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else key
            normalized = key.lower()
            if "policy" in normalized and child not in [None, "", [], {}]:
                found.append((path, str(child)))
            found.extend(find_policy_fields(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(find_policy_fields(child, f"{prefix}[{index}]"))
    return found


policy_fields = find_policy_fields(job)
serverless_usage_policy = [
    (path, value) for path, value in policy_fields
    if "usage" in path.lower() or "budget" in path.lower()
]
budget_policy_ok = bool(serverless_usage_policy)

max_concurrent_runs = settings.get("max_concurrent_runs")
concurrency_guard_ok = max_concurrent_runs == 1

# COMMAND ----------
# 3. Build and persist control evidence

control_rows = []


def add_control(control_id, domain, name, status, actual, remediation, blocking=False):
    control_rows.append((
        control_id, domain, name, status, actual, remediation, blocking,
    ))


add_control(
    "CONFIG_ISOLATION", "ENVIRONMENT",
    "Dev and prod catalogs and storage roots are isolated",
    "PASSED" if config_isolation_ok else "FAILED",
    (
        f"missing={','.join(missing_config) or 'none'}; "
        f"equal={','.join(equal_config) or 'none'}; "
        f"prod_refs_dev={','.join(prod_paths_reference_dev) or 'none'}; "
        f"prod_paths_reference_prod={prod_paths_reference_prod}"
    ),
    "Define distinct catalogs and landing/checkpoint/schema roots for dev and prod",
    True,
)

add_control(
    "JOB_CONCURRENCY", "COST",
    "Overlapping state-mutating runs are prevented",
    "PASSED" if concurrency_guard_ok else "FAILED",
    f"max_concurrent_runs={max_concurrent_runs}",
    "Set max_concurrent_runs=1",
)

add_control(
    "RESOURCE_SHUTDOWN", "COST",
    "Job compute is serverless/ephemeral or has auto termination",
    "PASSED" if resource_shutdown_ok else "FAILED",
    (
        f"compute_modes={json.dumps(compute_modes, sort_keys=True)}; "
        f"classic_autotermination={json.dumps(cluster_autotermination, sort_keys=True)}; "
        f"cluster_errors={','.join(cluster_errors) or 'none'}"
    ),
    "Use serverless or Jobs compute; configure auto termination for every existing cluster",
)

# Serverless usage policies are a Public Preview workspace capability. Absence
# is retained as MANUAL_REQUIRED rather than FAILED because the feature may not
# be enabled or assigned in this workspace.
add_control(
    "SERVERLESS_USAGE_POLICY", "COST",
    "Serverless usage/budget policy is attached",
    "PASSED" if budget_policy_ok else "MANUAL_REQUIRED",
    (
        "policy_fields=" + (
            ";".join(f"{path}={value}" for path, value in serverless_usage_policy)
            if serverless_usage_policy else "none"
        )
    ),
    "If available, select an approved Usage policy in Job details; otherwise document the workspace limitation",
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULTS}
(
    result_id       STRING    NOT NULL,
    audit_id        STRING    NOT NULL,
    environment     STRING    NOT NULL,
    job_id          BIGINT    NOT NULL,
    control_id      STRING    NOT NULL,
    control_domain  STRING    NOT NULL,
    control_name    STRING    NOT NULL,
    control_status  STRING    NOT NULL,
    actual_value    STRING,
    remediation     STRING,
    blocking        BOOLEAN   NOT NULL,
    checked_at      TIMESTAMP NOT NULL,
    created_at      TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Stage 14 environment-isolation and cost-control audit evidence'
TBLPROPERTIES ('quality' = 'ops')
""")

control_schema = """control_id string, control_domain string, control_name string,
                    control_status string, actual_value string, remediation string,
                    blocking boolean"""
controls_df = spark.createDataFrame(control_rows, control_schema).select(
    F.sha2(F.concat_ws("|", F.lit(AUDIT_ID), "control_id"), 256).alias("result_id"),
    F.lit(AUDIT_ID).alias("audit_id"),
    F.lit(ENVIRONMENT).alias("environment"),
    F.lit(JOB_ID).cast("long").alias("job_id"),
    "control_id", "control_domain", "control_name", "control_status",
    "actual_value", "remediation", "blocking",
    F.lit(CHECKED_AT).cast("timestamp").alias("checked_at"),
    F.lit(CHECKED_AT).cast("timestamp").alias("created_at"),
)
controls_df.write.format("delta").mode("append").saveAsTable(RESULTS)

# COMMAND ----------
# 4. Update CONFIG_01, COST_01 and COST_02

latest_audit = spark.sql(f"""
SELECT audit_run_id
FROM {RUNS}
WHERE environment = '{ENVIRONMENT}'
QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
""").first()
assert latest_audit is not None
HARDENING_AUDIT_RUN_ID = latest_audit.audit_run_id

status_by_id = {row[0]: row[3] for row in control_rows}
actual_by_id = {row[0]: row[4] for row in control_rows}
remediation_by_id = {row[0]: row[5] for row in control_rows}

updates = [
    ("CONFIG_01", status_by_id["CONFIG_ISOLATION"], "CONFIG_ISOLATION"),
    ("COST_01", status_by_id["SERVERLESS_USAGE_POLICY"], "SERVERLESS_USAGE_POLICY"),
    ("COST_02", status_by_id["RESOURCE_SHUTDOWN"], "RESOURCE_SHUTDOWN"),
]

for check_id, check_status, source_control in updates:
    safe_actual = (
        f"audit_id={AUDIT_ID}; {actual_by_id[source_control]}"
    ).replace("'", "''")
    safe_remediation = remediation_by_id[source_control].replace("'", "''")
    spark.sql(f"""
    UPDATE {CHECKS}
    SET check_status = '{check_status}',
        automation_level = 'AUTOMATED',
        actual_value = '{safe_actual}',
        evidence = '{RESULTS}; audit_id={AUDIT_ID}',
        remediation = '{safe_remediation}',
        checked_at = current_timestamp(),
        updated_at = current_timestamp()
    WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
      AND check_id = '{check_id}'
    """)

spark.sql(f"""
MERGE INTO {RUNS} r
USING (
  SELECT audit_run_id,
    COUNT(*) total_checks,
    COUNT_IF(check_status = 'PASSED') passed_checks,
    COUNT_IF(check_status = 'WARNING') warning_checks,
    COUNT_IF(check_status = 'FAILED') failed_checks,
    COUNT_IF(check_status = 'MANUAL_REQUIRED') manual_checks,
    CAST(ROUND(100.0 * COUNT_IF(check_status = 'PASSED') / COUNT(*), 2) AS DECIMAL(8,2)) readiness_pct,
    COUNT_IF(blocking_for_prod AND check_status <> 'PASSED') blocking_open
  FROM {CHECKS}
  WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
  GROUP BY audit_run_id
) s ON r.audit_run_id = s.audit_run_id
WHEN MATCHED THEN UPDATE SET
  r.total_checks=s.total_checks, r.passed_checks=s.passed_checks,
  r.warning_checks=s.warning_checks, r.failed_checks=s.failed_checks,
  r.manual_checks=s.manual_checks, r.readiness_pct=s.readiness_pct,
  r.run_status=CASE WHEN s.failed_checks>0 THEN 'BLOCKED'
                    WHEN s.blocking_open>0 THEN 'REVIEW_REQUIRED'
                    ELSE 'READY' END,
  r.notes=CONCAT('blocking_open=',s.blocking_open,'; Environment/cost audit=','{AUDIT_ID}'),
  r.updated_at=current_timestamp()
""")

# COMMAND ----------
# 5. Operator output

display(controls_df.select(
    "control_domain", "control_id", "control_name", "control_status",
    "actual_value", "remediation",
).orderBy("control_domain", "control_id"))

display(spark.sql(f"""
SELECT check_id, check_status, actual_value, remediation
FROM {CHECKS}
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
  AND check_id IN ('CONFIG_01','COST_01','COST_02')
ORDER BY check_id
"""))

display(spark.sql(f"""
SELECT * FROM {OPS}.v_production_hardening_summary
WHERE audit_run_id = '{HARDENING_AUDIT_RUN_ID}'
"""))

failed = [row[0] for row in control_rows if row[3] == "FAILED"]
manual = [row[0] for row in control_rows if row[3] == "MANUAL_REQUIRED"]

print("Stage 14 environment and cost controls audit completed")
print(f"Audit id: {AUDIT_ID}")
print(f"Job id: {JOB_ID}")
print(f"Dev/prod isolation: {status_by_id['CONFIG_ISOLATION']}")
print(f"Resource shutdown: {status_by_id['RESOURCE_SHUTDOWN']}")
print(f"Serverless usage policy: {status_by_id['SERVERLESS_USAGE_POLICY']}")
print(f"Failed controls: {failed}")
print(f"Manual controls: {manual}")
print("Next action: attach/document the serverless usage policy, then audit secret-scope ACLs")
