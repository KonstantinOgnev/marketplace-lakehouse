# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 14 — Notebook source secret scan
# MAGIC
# MAGIC Scans text-based Workspace files for likely plaintext credentials. The
# MAGIC matched value is never written to Delta: only its SHA-256 fingerprint and
# MAGIC a redacted line are persisted. The latest `SECURITY_01` hardening control
# MAGIC is updated automatically.

# COMMAND ----------

import hashlib
import os
import re
import uuid
from datetime import datetime, timezone

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
    "workspace_root": default_root,
    "max_files": "10000",
}.items():
    ensure_widget(widget_name, default_value)

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()
WORKSPACE_ROOT = (dbutils.widgets.get("workspace_root") or default_root).strip().rstrip("/")
MAX_FILES = int(dbutils.widgets.get("max_files") or "10000")

assert ENVIRONMENT in {"dev", "test", "prod"}
assert WORKSPACE_ROOT.startswith("/Workspace/"), "workspace_root must be under /Workspace"
assert 1 <= MAX_FILES <= 100000
assert os.path.isdir(WORKSPACE_ROOT), f"Workspace directory is not accessible: {WORKSPACE_ROOT}"

OPS = f"{CATALOG}.ops"
RUNS = f"{OPS}.production_hardening_runs"
CHECKS = f"{OPS}.production_hardening_checks"
FINDINGS = f"{OPS}.production_secret_scan_findings"

assert spark.catalog.tableExists(RUNS), f"Run 140 first; missing {RUNS}"
assert spark.catalog.tableExists(CHECKS), f"Run 140 first; missing {CHECKS}"

SCAN_ID = f"secret_scan_{ENVIRONMENT}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
SCANNED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

# COMMAND ----------
# 1. Durable findings table. It intentionally has no plaintext-secret column.

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FINDINGS}
(
    finding_id       STRING    NOT NULL,
    scan_id          STRING    NOT NULL,
    environment      STRING    NOT NULL,
    workspace_root   STRING    NOT NULL,
    source_path      STRING    NOT NULL,
    line_number      BIGINT    NOT NULL,
    rule_id          STRING    NOT NULL,
    severity         STRING    NOT NULL,
    secret_fingerprint STRING  NOT NULL,
    redacted_line    STRING,
    finding_status   STRING    NOT NULL,
    scanned_at       TIMESTAMP NOT NULL,
    created_at       TIMESTAMP NOT NULL
)
USING DELTA
COMMENT 'Stage 14 source-scan findings; plaintext secret values are never stored'
TBLPROPERTIES ('quality' = 'ops')
""")

# COMMAND ----------
# 2. Rules and source discovery

# Each regex must return the complete sensitive fragment as group 0. The scan
# notebook excludes itself so its rule definitions cannot become findings.
RULES = [
    ("DATABRICKS_PAT", "CRITICAL", re.compile(r"\bdapi[0-9a-fA-F]{32,}\b")),
    ("AWS_ACCESS_KEY", "CRITICAL", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("PRIVATE_KEY", "CRITICAL", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("AZURE_ACCOUNT_KEY", "CRITICAL", re.compile(r"(?i)AccountKey\s*=\s*[^;\s\"']{20,}")),
    ("JDBC_PASSWORD", "CRITICAL", re.compile(r"(?i)(?:password|pwd)=[^;&\s\"']{8,}")),
    (
        "HARDCODED_SECRET_ASSIGNMENT",
        "HIGH",
        re.compile(
            r"(?i)\b(?:password|passwd|pwd|secret|token|api[_-]?key|client[_-]?secret)\b"
            r"\s*(?::|=)\s*[\"'][^\"'\n${}]{8,}[\"']"
        ),
    ),
]

TEXT_EXTENSIONS = {".py", ".sql", ".scala", ".r", ".sh", ".json", ".yaml", ".yml", ".conf", ".txt"}
SELF_NAME = "141_notebook_secret_scan_v1.py"

source_paths = []
for directory, child_dirs, filenames in os.walk(WORKSPACE_ROOT):
    child_dirs[:] = [name for name in child_dirs if name not in {".git", ".idea", "__pycache__", ".venv"}]
    for filename in filenames:
        path = os.path.join(directory, filename)
        suffix = os.path.splitext(filename)[1].lower()
        if suffix in TEXT_EXTENSIONS and filename != SELF_NAME:
            source_paths.append(path)
            if len(source_paths) >= MAX_FILES:
                break
    if len(source_paths) >= MAX_FILES:
        break

# COMMAND ----------
# 3. Scan and redact before persistence

findings = []
unreadable_files = []
scanned_files = 0
scanned_lines = 0

for source_path in source_paths:
    try:
        with open(source_path, "r", encoding="utf-8", errors="replace") as source_file:
            scanned_files += 1
            for line_number, line in enumerate(source_file, start=1):
                scanned_lines += 1
                if "nosec-stage14" in line:
                    continue
                for rule_id, severity, pattern in RULES:
                    for match in pattern.finditer(line):
                        sensitive_fragment = match.group(0)
                        fingerprint = hashlib.sha256(sensitive_fragment.encode("utf-8")).hexdigest()
                        redacted = (line[:match.start()] + "[REDACTED]" + line[match.end():]).strip()[:1000]
                        finding_id = hashlib.sha256(
                            f"{source_path}|{line_number}|{rule_id}|{fingerprint}".encode("utf-8")
                        ).hexdigest()
                        findings.append((
                            finding_id, SCAN_ID, ENVIRONMENT, WORKSPACE_ROOT,
                            source_path, line_number, rule_id, severity,
                            fingerprint, redacted, "OPEN", SCANNED_AT, SCANNED_AT,
                        ))
    except Exception as exc:
        unreadable_files.append((source_path, type(exc).__name__))

finding_schema = T.StructType([
    T.StructField("finding_id", T.StringType(), False),
    T.StructField("scan_id", T.StringType(), False),
    T.StructField("environment", T.StringType(), False),
    T.StructField("workspace_root", T.StringType(), False),
    T.StructField("source_path", T.StringType(), False),
    T.StructField("line_number", T.LongType(), False),
    T.StructField("rule_id", T.StringType(), False),
    T.StructField("severity", T.StringType(), False),
    T.StructField("secret_fingerprint", T.StringType(), False),
    T.StructField("redacted_line", T.StringType(), True),
    T.StructField("finding_status", T.StringType(), False),
    T.StructField("scanned_at", T.TimestampType(), False),
    T.StructField("created_at", T.TimestampType(), False),
])

findings_df = spark.createDataFrame(findings, finding_schema)
findings_df.write.format("delta").mode("append").saveAsTable(FINDINGS)

# COMMAND ----------
# 4. Update SECURITY_01 in the latest hardening audit

latest_audit = spark.sql(f"""
SELECT audit_run_id
FROM {RUNS}
WHERE environment = '{ENVIRONMENT}'
QUALIFY ROW_NUMBER() OVER (ORDER BY started_at DESC) = 1
""").first()
assert latest_audit is not None, f"No Stage 14 audit exists for environment={ENVIRONMENT}"
AUDIT_RUN_ID = latest_audit.audit_run_id

critical_or_high = sum(1 for row in findings if row[7] in {"CRITICAL", "HIGH"})
scan_complete = len(unreadable_files) == 0 and len(source_paths) < MAX_FILES

if not scan_complete:
    control_status = "MANUAL_REQUIRED"
    actual_value = (
        f"scan_id={SCAN_ID}; files={scanned_files}; findings={len(findings)}; "
        f"unreadable={len(unreadable_files)}; max_files_reached={len(source_paths) >= MAX_FILES}"
    )
    remediation = "Resolve unreadable files or raise max_files, then rerun the complete scan"
elif critical_or_high > 0:
    control_status = "FAILED"
    actual_value = f"scan_id={SCAN_ID}; open_high_or_critical_findings={critical_or_high}"
    remediation = "Remove exposed values, rotate affected credentials, and rerun this scan"
else:
    control_status = "PASSED"
    actual_value = f"scan_id={SCAN_ID}; scanned_files={scanned_files}; findings=0"
    remediation = "Keep the scanner in CI/CD and rerun after every source change"

safe_actual = actual_value.replace("'", "''")
safe_remediation = remediation.replace("'", "''")
safe_evidence = f"{FINDINGS}; workspace_root={WORKSPACE_ROOT}".replace("'", "''")

spark.sql(f"""
UPDATE {CHECKS}
SET check_status = '{control_status}',
    automation_level = 'AUTOMATED',
    actual_value = '{safe_actual}',
    evidence = '{safe_evidence}',
    remediation = '{safe_remediation}',
    checked_at = current_timestamp(),
    updated_at = current_timestamp()
WHERE audit_run_id = '{AUDIT_RUN_ID}'
  AND check_id = 'SECURITY_01'
""")

# Recalculate the parent audit after replacing one manual result.
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
  WHERE audit_run_id = '{AUDIT_RUN_ID}'
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
  r.notes = CONCAT('blocking_open=', s.blocking_open, '; SECURITY_01 updated by ', '{SCAN_ID}'),
  r.updated_at = current_timestamp()
""")

# COMMAND ----------
# 5. Evidence and operator output

display(
    spark.table(FINDINGS)
    .filter(F.col("scan_id") == SCAN_ID)
    .select("severity", "rule_id", "source_path", "line_number", "redacted_line", "finding_status")
    .orderBy("severity", "source_path", "line_number")
)

display(spark.sql(f"""
SELECT check_id, check_status, actual_value, evidence, remediation
FROM {CHECKS}
WHERE audit_run_id = '{AUDIT_RUN_ID}' AND check_id = 'SECURITY_01'
"""))

display(spark.sql(f"SELECT * FROM {OPS}.v_production_hardening_summary WHERE audit_run_id = '{AUDIT_RUN_ID}'"))

print("Stage 14 notebook secret scan completed")
print(f"Scan id: {SCAN_ID}")
print(f"Workspace root: {WORKSPACE_ROOT}")
print(f"Candidate files: {len(source_paths)}")
print(f"Scanned files: {scanned_files}")
print(f"Scanned lines: {scanned_lines}")
print(f"Unreadable files: {len(unreadable_files)}")
print(f"Findings: {len(findings)}")
print(f"High or critical findings: {critical_or_high}")
print(f"SECURITY_01 status: {control_status}")
print("Next action: remediate findings if any; otherwise audit the Lakeflow Job definition")
