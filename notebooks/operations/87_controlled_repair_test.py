# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — controlled Repair run test
# MAGIC
# MAGIC This temporary Job task intentionally fails once per `job_run_id` and
# MAGIC succeeds on the next execution (the Repair run). It does not modify
# MAGIC Bronze, Silver or Gold data.

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = "marketplace_lab"
TEST_TABLE = f"{CATALOG}.ops.repair_test_runs"

dbutils.widgets.dropdown("enable_test", "false", ["false", "true"], "Enable controlled failure")
dbutils.widgets.text("job_run_id", "", "Lakeflow Job run ID")
dbutils.widgets.text("task_run_id", "", "Lakeflow task run ID")
dbutils.widgets.text("runtime_repair_count", "0", "Lakeflow runtime repair count")

enable_test = dbutils.widgets.get("enable_test").strip().lower() == "true"
job_run_id = dbutils.widgets.get("job_run_id").strip()
task_run_id = dbutils.widgets.get("task_run_id").strip()
repair_count = int(dbutils.widgets.get("runtime_repair_count").strip())

assert job_run_id, "job_run_id is required"
test_id = f"repair_{job_run_id}"

print(f"Controlled failure enabled: {enable_test}")
print(f"Test ID: {test_id}")
print(f"Repair count: {repair_count}")

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TEST_TABLE} (
      test_id STRING NOT NULL,
      job_run_id STRING NOT NULL,
      first_task_run_id STRING,
      repair_task_run_id STRING,
      attempt_count INT NOT NULL,
      test_status STRING NOT NULL,
      first_attempt_at TIMESTAMP NOT NULL,
      repaired_at TIMESTAMP,
      message STRING
    ) USING DELTA
    """
)

# COMMAND ----------

existing_df = spark.table(TEST_TABLE).filter(F.col("test_id") == test_id)
existing_count = existing_df.count()

if not enable_test:
    print("CONTROLLED REPAIR TEST DISABLED — task completed as a no-op.")
elif repair_count == 0:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if existing_count == 0:
        first_attempt_df = spark.createDataFrame(
            [(
                test_id,
                job_run_id,
                task_run_id or None,
                None,
                1,
                "FAILED_AS_PLANNED",
                now,
                None,
                "Intentional failure; waiting for a real Repair run",
            )],
            """test_id string, job_run_id string, first_task_run_id string,
               repair_task_run_id string, attempt_count int, test_status string,
               first_attempt_at timestamp, repaired_at timestamp, message string""",
        )
        first_attempt_df.write.mode("append").saveAsTable(TEST_TABLE)
    else:
        # Automatic task retry: record the attempt but continue to fail because
        # job.repair_count is still zero.
        spark.sql(
            f"""
            UPDATE {TEST_TABLE}
            SET attempt_count = attempt_count + 1,
                message = 'Automatic retry rejected; waiting for a real Repair run'
            WHERE test_id = '{test_id}'
            """
        )
    display(spark.table(TEST_TABLE).filter(F.col("test_id") == test_id))
    raise RuntimeError(
        "CONTROLLED_FAILURE: repair_count=0. Automatic retries must fail. "
        "Use Repair run; repair_count>0 will allow success."
    )
elif repair_count > 0 and existing_count == 1:
    existing = existing_df.first()
    assert existing.test_status == "FAILED_AS_PLANNED", (
        f"Unexpected existing test status: {existing.test_status}"
    )

    repair_update_df = spark.createDataFrame(
        [(
            test_id,
            task_run_id or None,
            existing.attempt_count + 1,
            "REPAIRED_SUCCEEDED",
            datetime.now(timezone.utc).replace(tzinfo=None),
            "Repair execution succeeded after the controlled first-attempt failure",
        )],
        """test_id string, repair_task_run_id string, attempt_count int,
           test_status string, repaired_at timestamp, message string""",
    )
    repair_update_df.createOrReplaceTempView("repair_test_update")
    spark.sql(
        f"""
        MERGE INTO {TEST_TABLE} t
        USING repair_test_update s
          ON t.test_id = s.test_id
        WHEN MATCHED THEN UPDATE SET
          t.repair_task_run_id = s.repair_task_run_id,
          t.attempt_count = s.attempt_count,
          t.test_status = s.test_status,
          t.repaired_at = s.repaired_at,
          t.message = s.message
        """
    )

    print("CONTROLLED REPAIR TEST PASSED")
    print("First attempt: FAILED_AS_PLANNED")
    print("Repair attempt: REPAIRED_SUCCEEDED")
    display(spark.table(TEST_TABLE).filter(F.col("test_id") == test_id))
elif repair_count > 0:
    raise RuntimeError(f"Duplicate test rows found for test_id={test_id}: {existing_count}")
