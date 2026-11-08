# Databricks notebook source
# MAGIC %md
# MAGIC # Stage 9 — Ops inventory
# MAGIC Read-only inventory of the existing observability layer.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)

ensure_widget("catalog_name", "marketplace_lab")
CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
OPS_SCHEMA = "ops"
OPS_NAMESPACE = f"{CATALOG}.{OPS_SCHEMA}"

assert spark.catalog.databaseExists(OPS_NAMESPACE), f"Missing schema: {OPS_NAMESPACE}"
print(f"Inspecting schema: {OPS_NAMESPACE}")

# COMMAND ----------
# 1. Objects currently registered in ops

show_tables_df = spark.sql(f"SHOW TABLES IN {OPS_NAMESPACE}")
show_columns = set(show_tables_df.columns)

namespace_column = next(
    (name for name in ["namespace", "database"] if name in show_columns),
    None,
)
table_name_column = next(
    (name for name in ["tableName", "table_name"] if name in show_columns),
    None,
)
temporary_column = next(
    (name for name in ["isTemporary", "is_temporary"] if name in show_columns),
    None,
)

assert namespace_column, f"Namespace column not found: {show_tables_df.columns}"
assert table_name_column, f"Table-name column not found: {show_tables_df.columns}"
assert temporary_column, f"Temporary flag column not found: {show_tables_df.columns}"

ops_objects_df = (
    show_tables_df
    .select(
        F.col(namespace_column).alias("namespace"),
        F.col(table_name_column).alias("object_name"),
        F.col(temporary_column).alias("is_temporary"),
    )
    .orderBy("object_name")
)

display(ops_objects_df)

object_names = [row["object_name"] for row in ops_objects_df.collect()]
print(f"Ops objects found: {len(object_names)}")

# COMMAND ----------
# 2. Physical and logical inventory

inventory_rows = []
schema_rows = []

for object_name in object_names:
    full_name = f"{OPS_NAMESPACE}.{object_name}"
    object_type = "UNKNOWN"
    storage_format = None
    location = None
    size_in_bytes = None
    num_files = None
    row_count = None
    read_status = "READABLE"
    error_message = None

    try:
        df = spark.table(full_name)
        row_count = df.count()
        for position, field in enumerate(df.schema.fields, start=1):
            schema_rows.append({
                "object_name": object_name,
                "ordinal_position": position,
                "column_name": field.name,
                "data_type": field.dataType.simpleString(),
                "nullable": field.nullable,
            })
    except Exception as exc:
        read_status = "UNREADABLE"
        error_message = str(exc)[:1000]

    try:
        detail = spark.sql(f"DESCRIBE DETAIL {full_name}").first().asDict()
        storage_format = detail.get("format")
        location = detail.get("location")
        size_in_bytes = detail.get("sizeInBytes")
        num_files = detail.get("numFiles")
        object_type = "VIEW" if storage_format is None else "TABLE"
    except Exception:
        try:
            extended = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}")
            type_rows = (
                extended
                .filter(F.col("col_name") == "Type")
                .select("data_type")
                .collect()
            )
            object_type = type_rows[0]["data_type"] if type_rows else "VIEW_OR_TABLE"
        except Exception:
            object_type = "UNKNOWN"

    inventory_rows.append({
        "object_name": object_name,
        "full_name": full_name,
        "object_type": object_type,
        "storage_format": storage_format,
        "row_count": row_count,
        "column_count": sum(1 for row in schema_rows if row["object_name"] == object_name),
        "num_files": num_files,
        "size_in_bytes": size_in_bytes,
        "location": location,
        "read_status": read_status,
        "error_message": error_message,
    })

inventory_schema = T.StructType([
    T.StructField("object_name", T.StringType(), False),
    T.StructField("full_name", T.StringType(), False),
    T.StructField("object_type", T.StringType(), True),
    T.StructField("storage_format", T.StringType(), True),
    T.StructField("row_count", T.LongType(), True),
    T.StructField("column_count", T.IntegerType(), True),
    T.StructField("num_files", T.LongType(), True),
    T.StructField("size_in_bytes", T.LongType(), True),
    T.StructField("location", T.StringType(), True),
    T.StructField("read_status", T.StringType(), False),
    T.StructField("error_message", T.StringType(), True),
])

schema_inventory_schema = T.StructType([
    T.StructField("object_name", T.StringType(), False),
    T.StructField("ordinal_position", T.IntegerType(), False),
    T.StructField("column_name", T.StringType(), False),
    T.StructField("data_type", T.StringType(), False),
    T.StructField("nullable", T.BooleanType(), False),
])

inventory_df = spark.createDataFrame(inventory_rows, inventory_schema)
schema_inventory_df = spark.createDataFrame(schema_rows, schema_inventory_schema)

display(inventory_df.orderBy("object_name"))
display(schema_inventory_df.orderBy("object_name", "ordinal_position"))

# COMMAND ----------
# 3. Coverage of the Stage 9 target model

target_candidates = {
    "pipeline_runs": ["pipeline_runs"],
    "task_runs": ["task_runs", "pipeline_task_runs"],
    "data_quality_results": ["data_quality_results"],
    "source_batch_manifest": [
        "source_batch_manifest",
        "batch_registry",
        "landing_manifest",
        "main_manifest",
    ],
    "table_metrics": ["table_metrics"],
}

coverage_rows = []
available = set(object_names)

for target_name, candidates in target_candidates.items():
    matched = next((candidate for candidate in candidates if candidate in available), None)
    if matched == target_name:
        status = "EXISTS"
    elif matched:
        status = "EXISTING_EQUIVALENT"
    else:
        status = "MISSING"

    coverage_rows.append({
        "target_object": target_name,
        "matched_object": matched,
        "coverage_status": status,
        "recommended_action": (
            "KEEP"
            if status == "EXISTS"
            else "REVIEW_AND_CREATE_COMPATIBILITY_VIEW"
            if status == "EXISTING_EQUIVALENT"
            else "CREATE"
        ),
    })

coverage_df = spark.createDataFrame(coverage_rows)
display(coverage_df.orderBy("target_object"))

missing_targets = [
    row["target_object"]
    for row in coverage_rows
    if row["coverage_status"] == "MISSING"
]
equivalent_targets = [
    row["target_object"]
    for row in coverage_rows
    if row["coverage_status"] == "EXISTING_EQUIVALENT"
]

print("Stage 9 ops inventory completed")
print(f"Existing ops objects:       {len(object_names)}")
print(f"Missing target objects:     {missing_targets}")
print(f"Targets with equivalents:   {equivalent_targets}")

# COMMAND ----------
# 4. Recent operational activity

if "pipeline_runs" in available:
    display(
        spark.table(f"{OPS_NAMESPACE}.pipeline_runs")
        .orderBy(F.col("created_at").desc())
        .limit(20)
    )

task_audit_name = next(
    (name for name in ["task_runs", "pipeline_task_runs"] if name in available),
    None,
)

if task_audit_name:
    task_audit_df = spark.table(f"{OPS_NAMESPACE}.{task_audit_name}")
    status_column = next(
        (name for name in ["task_status", "status"] if name in task_audit_df.columns),
        None,
    )
    if status_column:
        display(
            task_audit_df
            .groupBy(status_column)
            .count()
            .orderBy(status_column)
        )
    display(
        task_audit_df
        .orderBy(F.col("created_at").desc())
        .limit(50)
    )
