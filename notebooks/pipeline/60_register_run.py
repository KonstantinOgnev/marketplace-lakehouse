# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 8 — Register pipeline run
# MAGIC
# MAGIC Validates and normalizes Lakeflow Job parameters.
# MAGIC Produces one common run context for all downstream tasks.

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql import types as T


CATALOG = "marketplace_lab"
OPS_SCHEMA = "ops"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{OPS_SCHEMA}")


# ------------------------------------------------------------------
# 1. Create only missing widgets.
#    Existing Job parameters must not be overwritten.
# ------------------------------------------------------------------

DEFAULT_LOAD_DATE = datetime.now(timezone.utc).date().isoformat()

WIDGET_DEFAULTS = {
    "environment": "dev",
    "load_date": DEFAULT_LOAD_DATE,
    "mode": "incremental",
    "start_date": "",
    "end_date": "",
    "run_id": "",
    "source_set": "all",
    "data_scale": "development",
    "job_id": "",
    "job_run_id": "",
    "task_run_id": "",
    "repair_count": "0"
}


def ensure_widget(name: str, default_value: str) -> None:
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default_value)


for widget_name, widget_default in WIDGET_DEFAULTS.items():
    ensure_widget(widget_name, widget_default)


def widget_value(name: str) -> str:
    return (dbutils.widgets.get(name) or "").strip()


# ------------------------------------------------------------------
# 2. Read and normalize parameters.
# ------------------------------------------------------------------

ENVIRONMENT = widget_value("environment").lower()
LOAD_DATE_RAW = widget_value("load_date")
MODE = widget_value("mode").lower().replace("_", "-")
START_DATE_RAW = widget_value("start_date")
END_DATE_RAW = widget_value("end_date")
RUN_ID_RAW = widget_value("run_id")
SOURCE_SET_RAW = widget_value("source_set").lower()
DATA_SCALE = widget_value("data_scale").lower()

JOB_ID = widget_value("job_id") or "MANUAL"
JOB_RUN_ID = widget_value("job_run_id") or "MANUAL"
TASK_RUN_ID = widget_value("task_run_id") or "MANUAL"
REPAIR_COUNT_RAW = widget_value("repair_count") or "0"


# ------------------------------------------------------------------
# 3. Validate scalar parameters.
# ------------------------------------------------------------------

ALLOWED_ENVIRONMENTS = {"dev", "test", "prod"}
ALLOWED_MODES = {
    "incremental",
    "backfill",
    "replay",
    "full-refresh"
}
ALLOWED_DATA_SCALES = {
    "development",
    "standard",
    "large"
}

assert ENVIRONMENT in ALLOWED_ENVIRONMENTS, (
    f"Invalid environment '{ENVIRONMENT}'. "
    f"Allowed values: {sorted(ALLOWED_ENVIRONMENTS)}"
)

assert MODE in ALLOWED_MODES, (
    f"Invalid mode '{MODE}'. "
    f"Allowed values: {sorted(ALLOWED_MODES)}"
)

assert DATA_SCALE in ALLOWED_DATA_SCALES, (
    f"Invalid data_scale '{DATA_SCALE}'. "
    f"Allowed values: {sorted(ALLOWED_DATA_SCALES)}"
)

try:
    REPAIR_COUNT = int(REPAIR_COUNT_RAW)
except ValueError as exc:
    raise ValueError(
        f"repair_count must be an integer, received '{REPAIR_COUNT_RAW}'"
    ) from exc

assert REPAIR_COUNT >= 0, "repair_count cannot be negative"


# ------------------------------------------------------------------
# 4. Normalize dates.
# ------------------------------------------------------------------

def parse_iso_date(value: str, parameter_name: str):
    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"{parameter_name} must use YYYY-MM-DD format; "
            f"received '{value}'"
        ) from exc


LOAD_DATE = parse_iso_date(LOAD_DATE_RAW, "load_date")
START_DATE = parse_iso_date(START_DATE_RAW, "start_date")
END_DATE = parse_iso_date(END_DATE_RAW, "end_date")

assert LOAD_DATE is not None, "load_date is required"

if MODE in {"incremental", "replay"}:
    START_DATE = START_DATE or LOAD_DATE
    END_DATE = END_DATE or LOAD_DATE

elif MODE == "backfill":
    assert START_DATE is not None, (
        "start_date is required when mode='backfill'"
    )
    assert END_DATE is not None, (
        "end_date is required when mode='backfill'"
    )

elif MODE == "full-refresh":
    # A full refresh can process all available data.
    # If a range is supplied, both boundaries must be supplied.
    assert (START_DATE is None) == (END_DATE is None), (
        "For full-refresh, specify both start_date and end_date "
        "or leave both empty"
    )

if START_DATE is not None and END_DATE is not None:
    assert START_DATE <= END_DATE, (
        f"start_date {START_DATE} is later than end_date {END_DATE}"
    )


# ------------------------------------------------------------------
# 5. Normalize source_set.
# ------------------------------------------------------------------

DATASET_ORDER = [
    "sellers",
    "products",
    "orders",
    "payments",
    "user_events"
]

SOURCE_ALIASES = {
    "seller": "sellers",
    "product": "products",
    "order": "orders",
    "payment": "payments",
    "events": "user_events",
    "event": "user_events",
    "user-event": "user_events",
    "user-events": "user_events"
}

if not SOURCE_SET_RAW or SOURCE_SET_RAW == "all":
    REQUESTED_SOURCE_SET = DATASET_ORDER.copy()
else:
    requested_sources = [
        item.strip()
        for item in SOURCE_SET_RAW.split(",")
        if item.strip()
    ]

    REQUESTED_SOURCE_SET = [
        SOURCE_ALIASES.get(item, item)
        for item in requested_sources
    ]

unknown_sources = sorted(
    set(REQUESTED_SOURCE_SET) - set(DATASET_ORDER)
)

assert not unknown_sources, (
    f"Unknown datasets in source_set: {unknown_sources}. "
    f"Allowed datasets: {DATASET_ORDER}"
)

REQUESTED_SOURCE_SET = [
    dataset
    for dataset in DATASET_ORDER
    if dataset in set(REQUESTED_SOURCE_SET)
]

assert REQUESTED_SOURCE_SET, "source_set cannot be empty"


# Silver dependencies:
# products    -> sellers
# orders      -> products, sellers
# payments    -> orders, products, sellers
# user_events -> orders, products, sellers

SOURCE_DEPENDENCIES = {
    "sellers": set(),
    "products": {"sellers"},
    "orders": {"sellers", "products"},
    "payments": {"sellers", "products", "orders"},
    "user_events": {"sellers", "products", "orders"}
}

effective_sources = set(REQUESTED_SOURCE_SET)

for source_name in REQUESTED_SOURCE_SET:
    effective_sources.update(SOURCE_DEPENDENCIES[source_name])

EFFECTIVE_SOURCE_SET = [
    dataset
    for dataset in DATASET_ORDER
    if dataset in effective_sources
]


# ------------------------------------------------------------------
# 6. Generate one stable run_id.
# ------------------------------------------------------------------

if RUN_ID_RAW:
    RUN_ID = RUN_ID_RAW
elif JOB_RUN_ID != "MANUAL":
    RUN_ID = f"{ENVIRONMENT}_{MODE}_{JOB_RUN_ID}"
else:
    utc_timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    RUN_ID = (
        f"{ENVIRONMENT}_{MODE}_"
        f"{utc_timestamp}_{uuid4().hex[:8]}"
    )

RUN_ID = (
    RUN_ID.strip()
    .replace(" ", "_")
    .replace("/", "_")
)

assert len(RUN_ID) <= 200, "run_id cannot exceed 200 characters"


# ------------------------------------------------------------------
# 7. Create the normalized run context.
# ------------------------------------------------------------------

run_context_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("job_id", T.StringType(), False),
    T.StructField("job_run_id", T.StringType(), False),
    T.StructField("task_run_id", T.StringType(), False),
    T.StructField("repair_count", T.IntegerType(), False),
    T.StructField("environment", T.StringType(), False),
    T.StructField("mode", T.StringType(), False),
    T.StructField("load_date", T.DateType(), False),
    T.StructField("start_date", T.DateType(), True),
    T.StructField("end_date", T.DateType(), True),
    T.StructField(
        "requested_source_set",
        T.ArrayType(T.StringType()),
        False
    ),
    T.StructField(
        "effective_source_set",
        T.ArrayType(T.StringType()),
        False
    ),
    T.StructField("data_scale", T.StringType(), False),
    T.StructField("registered_at", T.TimestampType(), False)
])

REGISTERED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

run_context_df = spark.createDataFrame(
    [(
        RUN_ID,
        JOB_ID,
        JOB_RUN_ID,
        TASK_RUN_ID,
        REPAIR_COUNT,
        ENVIRONMENT,
        MODE,
        LOAD_DATE,
        START_DATE,
        END_DATE,
        REQUESTED_SOURCE_SET,
        EFFECTIVE_SOURCE_SET,
        DATA_SCALE,
        REGISTERED_AT
    )],
    schema=run_context_schema
)


# ------------------------------------------------------------------
# 8. Display validation result.
# ------------------------------------------------------------------

print("Run parameters validated")
print(f"Run ID:                 {RUN_ID}")
print(f"Environment:            {ENVIRONMENT}")
print(f"Mode:                   {MODE}")
print(f"Load date:              {LOAD_DATE}")
print(f"Start date:             {START_DATE}")
print(f"End date:               {END_DATE}")
print(f"Requested sources:      {REQUESTED_SOURCE_SET}")
print(f"Effective sources:      {EFFECTIVE_SOURCE_SET}")
print(f"Data scale:             {DATA_SCALE}")
print(f"Job ID:                 {JOB_ID}")
print(f"Job run ID:             {JOB_RUN_ID}")
print(f"Task run ID:            {TASK_RUN_ID}")
print(f"Repair count:           {REPAIR_COUNT}")

display(run_context_df)

# COMMAND ----------

# COMMAND ----------
# Stage 8 — Create operational tables and register the pipeline run.

PIPELINE_RUNS_TABLE = f"{CATALOG}.{OPS_SCHEMA}.pipeline_runs"
PIPELINE_TASK_RUNS_TABLE = f"{CATALOG}.{OPS_SCHEMA}.pipeline_task_runs"


# ------------------------------------------------------------------
# 1. Pipeline-level registry.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PIPELINE_RUNS_TABLE}
(
    run_id                  STRING    NOT NULL COMMENT 'Unique logical pipeline run identifier',
    job_id                  STRING             COMMENT 'Lakeflow Job identifier',
    job_run_id              STRING             COMMENT 'Lakeflow Job run identifier',
    environment             STRING    NOT NULL COMMENT 'dev, test or prod',
    mode                    STRING    NOT NULL COMMENT 'incremental, backfill, replay or full-refresh',
    load_date               DATE      NOT NULL COMMENT 'Primary processing date',
    start_date              DATE               COMMENT 'Beginning of processing interval',
    end_date                DATE               COMMENT 'End of processing interval',
    requested_source_set    ARRAY<STRING>       COMMENT 'Datasets requested by the caller',
    effective_source_set    ARRAY<STRING>       COMMENT 'Datasets including required dependencies',
    data_scale              STRING              COMMENT 'Requested compute or data profile',
    run_status              STRING    NOT NULL COMMENT 'Current pipeline status',
    started_at              TIMESTAMP NOT NULL COMMENT 'Pipeline registration timestamp',
    completed_at            TIMESTAMP          COMMENT 'Pipeline completion timestamp',
    published_at            TIMESTAMP          COMMENT 'Gold publication timestamp',
    error_task              STRING             COMMENT 'Task that reported the failure',
    error_message           STRING             COMMENT 'Last pipeline error',
    repair_count            INT       NOT NULL COMMENT 'Lakeflow Job repair number',
    created_at              TIMESTAMP NOT NULL COMMENT 'Registry row creation timestamp',
    updated_at              TIMESTAMP NOT NULL COMMENT 'Registry row update timestamp'
)
USING DELTA
COMMENT 'Operational registry of Marketplace Lakehouse pipeline runs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ------------------------------------------------------------------
# 2. Task-level registry.
# ------------------------------------------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {PIPELINE_TASK_RUNS_TABLE}
(
    run_id                  STRING    NOT NULL COMMENT 'Logical pipeline run identifier',
    task_key                STRING    NOT NULL COMMENT 'Lakeflow task key',
    task_run_id             STRING             COMMENT 'Physical Lakeflow task run identifier',
    repair_count            INT       NOT NULL COMMENT 'Lakeflow Job repair number',
    dataset                 STRING             COMMENT 'Dataset processed by the task',
    task_status             STRING    NOT NULL COMMENT 'Task execution status',
    started_at              TIMESTAMP NOT NULL COMMENT 'Task start timestamp',
    completed_at            TIMESTAMP          COMMENT 'Task completion timestamp',
    input_row_count         BIGINT              COMMENT 'Rows read by the task',
    output_row_count        BIGINT              COMMENT 'Rows successfully written',
    rejected_row_count      BIGINT              COMMENT 'Rows rejected or quarantined',
    source_file_count       BIGINT              COMMENT 'Source files processed',
    error_code              STRING             COMMENT 'Normalized error code',
    error_message           STRING             COMMENT 'Task error details',
    metrics                 MAP<STRING, STRING> COMMENT 'Additional task-specific metrics',
    created_at              TIMESTAMP NOT NULL COMMENT 'Registry row creation timestamp',
    updated_at              TIMESTAMP NOT NULL COMMENT 'Registry row update timestamp'
)
USING DELTA
COMMENT 'Task-level audit registry for Marketplace Lakehouse pipeline runs'
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
)
""")


# ------------------------------------------------------------------
# 3. Prepare one source row for the pipeline registry.
# ------------------------------------------------------------------

pipeline_registration_df = (
    run_context_df
    .select(
        "run_id",
        "job_id",
        "job_run_id",
        "environment",
        "mode",
        "load_date",
        "start_date",
        "end_date",
        "requested_source_set",
        "effective_source_set",
        "data_scale",
        "repair_count",
        F.col("registered_at").alias("started_at")
    )
    .withColumn("run_status", F.lit("RUNNING"))
    .withColumn("completed_at", F.lit(None).cast("timestamp"))
    .withColumn("published_at", F.lit(None).cast("timestamp"))
    .withColumn("error_task", F.lit(None).cast("string"))
    .withColumn("error_message", F.lit(None).cast("string"))
    .withColumn("created_at", F.col("started_at"))
    .withColumn("updated_at", F.current_timestamp())
)

pipeline_registration_df.createOrReplaceTempView(
    "pipeline_registration_source"
)


# ------------------------------------------------------------------
# 4. Idempotently register the pipeline run.
# ------------------------------------------------------------------

spark.sql(f"""
MERGE INTO {PIPELINE_RUNS_TABLE} AS target
USING pipeline_registration_source AS source
ON target.run_id = source.run_id

WHEN MATCHED THEN UPDATE SET
    target.job_id               = source.job_id,
    target.job_run_id           = source.job_run_id,
    target.environment          = source.environment,
    target.mode                 = source.mode,
    target.load_date            = source.load_date,
    target.start_date           = source.start_date,
    target.end_date             = source.end_date,
    target.requested_source_set = source.requested_source_set,
    target.effective_source_set = source.effective_source_set,
    target.data_scale           = source.data_scale,
    target.repair_count         = source.repair_count,

    target.run_status = CASE
        WHEN target.run_status = 'PUBLISHED'
            THEN target.run_status
        ELSE source.run_status
    END,

    target.completed_at = CASE
        WHEN target.run_status = 'PUBLISHED'
            THEN target.completed_at
        ELSE NULL
    END,

    target.published_at = CASE
        WHEN target.run_status = 'PUBLISHED'
            THEN target.published_at
        ELSE NULL
    END,

    target.error_task = CASE
        WHEN target.run_status = 'PUBLISHED'
            THEN target.error_task
        ELSE NULL
    END,

    target.error_message = CASE
        WHEN target.run_status = 'PUBLISHED'
            THEN target.error_message
        ELSE NULL
    END,

    target.updated_at = source.updated_at

WHEN NOT MATCHED THEN INSERT
(
    run_id,
    job_id,
    job_run_id,
    environment,
    mode,
    load_date,
    start_date,
    end_date,
    requested_source_set,
    effective_source_set,
    data_scale,
    run_status,
    started_at,
    completed_at,
    published_at,
    error_task,
    error_message,
    repair_count,
    created_at,
    updated_at
)
VALUES
(
    source.run_id,
    source.job_id,
    source.job_run_id,
    source.environment,
    source.mode,
    source.load_date,
    source.start_date,
    source.end_date,
    source.requested_source_set,
    source.effective_source_set,
    source.data_scale,
    source.run_status,
    source.started_at,
    source.completed_at,
    source.published_at,
    source.error_task,
    source.error_message,
    source.repair_count,
    source.created_at,
    source.updated_at
)
""")


# ------------------------------------------------------------------
# 5. Register successful completion of register_run itself.
# ------------------------------------------------------------------

register_task_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("task_key", T.StringType(), False),
    T.StructField("task_run_id", T.StringType(), True),
    T.StructField("repair_count", T.IntegerType(), False),
    T.StructField("dataset", T.StringType(), True),
    T.StructField("task_status", T.StringType(), False),
    T.StructField("started_at", T.TimestampType(), False),
    T.StructField("completed_at", T.TimestampType(), True),
    T.StructField("input_row_count", T.LongType(), True),
    T.StructField("output_row_count", T.LongType(), True),
    T.StructField("rejected_row_count", T.LongType(), True),
    T.StructField("source_file_count", T.LongType(), True),
    T.StructField("error_code", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField(
        "metrics",
        T.MapType(T.StringType(), T.StringType()),
        True
    ),
    T.StructField("created_at", T.TimestampType(), False),
    T.StructField("updated_at", T.TimestampType(), False)
])

TASK_COMPLETED_AT = datetime.now(timezone.utc).replace(tzinfo=None)

register_task_df = spark.createDataFrame(
    [(
        RUN_ID,
        "register_run",
        TASK_RUN_ID,
        REPAIR_COUNT,
        None,
        "SUCCEEDED",
        REGISTERED_AT,
        TASK_COMPLETED_AT,
        None,
        1,
        0,
        0,
        None,
        None,
        {
            "environment": ENVIRONMENT,
            "mode": MODE,
            "load_date": LOAD_DATE.isoformat(),
            "requested_sources": ",".join(REQUESTED_SOURCE_SET),
            "effective_sources": ",".join(EFFECTIVE_SOURCE_SET)
        },
        REGISTERED_AT,
        TASK_COMPLETED_AT
    )],
    schema=register_task_schema
)

register_task_df.createOrReplaceTempView(
    "register_task_source"
)


spark.sql(f"""
MERGE INTO {PIPELINE_TASK_RUNS_TABLE} AS target
USING register_task_source AS source
ON  target.run_id = source.run_id
AND target.task_key = source.task_key
AND target.repair_count = source.repair_count

WHEN MATCHED THEN UPDATE SET
    target.task_run_id        = source.task_run_id,
    target.dataset            = source.dataset,
    target.task_status        = source.task_status,
    target.started_at         = source.started_at,
    target.completed_at       = source.completed_at,
    target.input_row_count    = source.input_row_count,
    target.output_row_count   = source.output_row_count,
    target.rejected_row_count = source.rejected_row_count,
    target.source_file_count  = source.source_file_count,
    target.error_code         = source.error_code,
    target.error_message      = source.error_message,
    target.metrics            = source.metrics,
    target.updated_at         = source.updated_at

WHEN NOT MATCHED THEN INSERT *
""")


# ------------------------------------------------------------------
# 6. Publish normalized values for downstream Lakeflow tasks.
# ------------------------------------------------------------------

TASK_VALUES = {
    "run_id": RUN_ID,
    "environment": ENVIRONMENT,
    "mode": MODE,
    "load_date": LOAD_DATE.isoformat(),
    "start_date": START_DATE.isoformat() if START_DATE else "",
    "end_date": END_DATE.isoformat() if END_DATE else "",
    "requested_source_set": ",".join(REQUESTED_SOURCE_SET),
    "effective_source_set": ",".join(EFFECTIVE_SOURCE_SET),
    "data_scale": DATA_SCALE
}

task_values_published = True
task_values_message = "Published inside Lakeflow Job"

try:
    for task_value_key, task_value in TASK_VALUES.items():
        dbutils.jobs.taskValues.set(
            key=task_value_key,
            value=task_value
        )
except Exception as exc:
    # Expected when the notebook is executed interactively rather than as a Job.
    task_values_published = False
    task_values_message = (
        "Interactive execution: task values were not published. "
        f"{type(exc).__name__}: {str(exc)[:300]}"
    )


# ------------------------------------------------------------------
# 7. Final verification.
# ------------------------------------------------------------------

registered_run_df = spark.sql(f"""
SELECT
    run_id,
    environment,
    mode,
    load_date,
    start_date,
    end_date,
    requested_source_set,
    effective_source_set,
    data_scale,
    run_status,
    repair_count,
    started_at,
    updated_at
FROM {PIPELINE_RUNS_TABLE}
WHERE run_id = '{RUN_ID}'
""")

registered_task_df = spark.sql(f"""
SELECT
    run_id,
    task_key,
    task_run_id,
    repair_count,
    task_status,
    output_row_count,
    metrics,
    started_at,
    completed_at
FROM {PIPELINE_TASK_RUNS_TABLE}
WHERE run_id = '{RUN_ID}'
  AND task_key = 'register_run'
  AND repair_count = {REPAIR_COUNT}
""")

assert registered_run_df.count() == 1, (
    f"Expected exactly one pipeline registry row for run_id={RUN_ID}"
)

assert registered_task_df.count() == 1, (
    f"Expected exactly one register_run task row for run_id={RUN_ID}"
)

assert registered_run_df.first()["run_status"] in {
    "RUNNING",
    "PUBLISHED"
}, "Unexpected pipeline status"

assert registered_task_df.first()["task_status"] == "SUCCEEDED", (
    "register_run task was not registered as SUCCEEDED"
)

print("Pipeline run registered successfully")
print(f"Pipeline table:       {PIPELINE_RUNS_TABLE}")
print(f"Task table:           {PIPELINE_TASK_RUNS_TABLE}")
print(f"Run ID:               {RUN_ID}")
print(f"Task values published: {task_values_published}")
print(f"Task values message:   {task_values_message}")

display(registered_run_df)
display(registered_task_df)