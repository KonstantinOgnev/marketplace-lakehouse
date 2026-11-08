# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — reconcile stale pipeline runs
# MAGIC
# MAGIC Safely closes historical `RUNNING` rows that have no terminal task.
# MAGIC The notebook defaults to **dry-run** and changes nothing until
# MAGIC `dry_run=false` is supplied.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "marketplace_lab"
OPS_SCHEMA = "ops"
PIPELINE_RUNS = f"{CATALOG}.{OPS_SCHEMA}.pipeline_runs"
TASK_RUNS = f"{CATALOG}.{OPS_SCHEMA}.pipeline_task_runs"

dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Dry run")
dbutils.widgets.text("stale_after_hours", "12", "Stale after (hours)")

dry_run = dbutils.widgets.get("dry_run").strip().lower() == "true"
stale_after_hours = int(dbutils.widgets.get("stale_after_hours"))

assert stale_after_hours >= 1, "stale_after_hours must be at least 1"

print(f"Dry run: {dry_run}")
print(f"Stale threshold: {stale_after_hours} hours")

# COMMAND ----------

# One row per stale run, with evidence collected from its audited tasks.
candidates_df = spark.sql(
    f"""
    WITH task_evidence AS (
      SELECT
        run_id,
        COUNT(*) AS audited_task_count,
        MAX(CASE WHEN task_key = 'publish_result'
                  AND task_status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS has_successful_publish,
        MAX(CASE WHEN task_key = 'failure_handler' THEN 1 ELSE 0 END) AS has_failure_handler,
        MAX(CASE WHEN task_key = 'gold_validate'
                  AND task_status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS has_successful_gold_validation,
        MAX(completed_at) AS last_task_completed_at
      FROM {TASK_RUNS}
      GROUP BY run_id
    )
    SELECT
      p.run_id,
      p.started_at,
      TIMESTAMPDIFF(HOUR, p.started_at, CURRENT_TIMESTAMP()) AS age_hours,
      COALESCE(t.audited_task_count, 0) AS audited_task_count,
      COALESCE(t.has_successful_publish, 0) AS has_successful_publish,
      COALESCE(t.has_failure_handler, 0) AS has_failure_handler,
      COALESCE(t.has_successful_gold_validation, 0) AS has_successful_gold_validation,
      t.last_task_completed_at,
      CASE
        WHEN COALESCE(t.has_successful_publish, 0) = 1 THEN 'SUCCEEDED'
        WHEN COALESCE(t.has_failure_handler, 0) = 1 THEN 'FAILED'
        WHEN COALESCE(t.has_successful_gold_validation, 0) = 1 THEN 'SUCCEEDED'
        ELSE 'FAILED'
      END AS inferred_status,
      CASE
        WHEN COALESCE(t.has_successful_publish, 0) = 1
          THEN 'RECONCILED: successful publish task found'
        WHEN COALESCE(t.has_failure_handler, 0) = 1
          THEN 'RECONCILED: failure handler task found'
        WHEN COALESCE(t.has_successful_gold_validation, 0) = 1
          THEN 'RECONCILED: successful Gold validation found (legacy run)'
        ELSE 'STALE_RUNNING_RECONCILED: no terminal task evidence'
      END AS reconciliation_reason,
      COALESCE(t.last_task_completed_at, p.started_at) AS inferred_completed_at
    FROM {PIPELINE_RUNS} p
    LEFT JOIN task_evidence t ON p.run_id = t.run_id
    WHERE p.run_status = 'RUNNING'
      AND p.started_at < CURRENT_TIMESTAMP() - INTERVAL {stale_after_hours} HOURS
    ORDER BY p.started_at
    """
)

candidate_count = candidates_df.count()
print(f"Stale RUNNING candidates: {candidate_count}")
display(candidates_df)

# COMMAND ----------

if dry_run:
    print("DRY RUN completed: pipeline_runs was not changed.")
    print("Review the table above, then rerun with dry_run=false to apply.")
else:
    if candidate_count == 0:
        print("Nothing to reconcile.")
    else:
        updates_df = candidates_df.select(
            "run_id",
            F.col("inferred_status").alias("new_status"),
            "reconciliation_reason",
            "inferred_completed_at",
        )

        target = DeltaTable.forName(spark, PIPELINE_RUNS)
        (
            target.alias("t")
            .merge(updates_df.alias("s"), "t.run_id = s.run_id AND t.run_status = 'RUNNING'")
            .whenMatchedUpdate(
                set={
                    "run_status": "s.new_status",
                    "completed_at": "COALESCE(t.completed_at, s.inferred_completed_at, current_timestamp())",
                    "error_message": "CASE WHEN s.new_status = 'FAILED' THEN COALESCE(t.error_message, s.reconciliation_reason) ELSE t.error_message END",
                    "updated_at": "current_timestamp()",
                }
            )
            .execute()
        )
        print(f"Reconciliation applied to up to {candidate_count} stale runs.")

# COMMAND ----------

validation_df = spark.sql(
    f"""
    SELECT
      run_status,
      COUNT(*) AS run_count,
      MIN(started_at) AS earliest_started_at,
      MAX(started_at) AS latest_started_at
    FROM {PIPELINE_RUNS}
    GROUP BY run_status
    ORDER BY run_status
    """
)

remaining_stale_df = spark.sql(
    f"""
    SELECT run_id, started_at,
           TIMESTAMPDIFF(HOUR, started_at, CURRENT_TIMESTAMP()) AS age_hours
    FROM {PIPELINE_RUNS}
    WHERE run_status = 'RUNNING'
      AND started_at < CURRENT_TIMESTAMP() - INTERVAL {stale_after_hours} HOURS
    ORDER BY started_at
    """
)

print("Pipeline run status summary:")
display(validation_df)
print(f"Remaining stale RUNNING rows: {remaining_stale_df.count()}")
display(remaining_stale_df)
