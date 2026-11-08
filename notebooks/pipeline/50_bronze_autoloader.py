# Databricks notebook source
# Databricks notebook source
# 1. Configure Bronze Auto Loader streams

from pyspark.sql import functions as F
from pyspark.sql import types as T


# ---------------------------------------------------------
# 1. Common paths
# ---------------------------------------------------------

catalog_name = "marketplace_lab"
bronze_schema = "bronze"

landing_root = (
    "/Volumes/marketplace_lab/bronze/landing"
)

autoloader_state_root = (
    "/Volumes/marketplace_lab/ops/checkpoints/"
    "_autoloader"
)

checkpoint_root = (
    f"{autoloader_state_root}/checkpoints"
)

schema_root = (
    f"{autoloader_state_root}/schemas"
)


# ---------------------------------------------------------
# 2. Independent source configurations
# ---------------------------------------------------------

SOURCE_CONFIG = {
    "user_events": {
        "format": "json",
        "source_path": (
            f"{landing_root}/user_events"
        ),
        "target_table": (
            f"{catalog_name}.{bronze_schema}."
            "user_events_raw"
        ),
        "checkpoint_path": (
            f"{checkpoint_root}/user_events"
        ),
        "schema_path": (
            f"{schema_root}/user_events"
        ),
    },

    "orders": {
        "format": "json",
        "source_path": (
            f"{landing_root}/orders"
        ),
        "target_table": (
            f"{catalog_name}.{bronze_schema}."
            "orders_raw"
        ),
        "checkpoint_path": (
            f"{checkpoint_root}/orders"
        ),
        "schema_path": (
            f"{schema_root}/orders"
        ),
    },

    "payments": {
        "format": "csv",
        "source_path": (
            f"{landing_root}/payments"
        ),
        "target_table": (
            f"{catalog_name}.{bronze_schema}."
            "payments_raw"
        ),
        "checkpoint_path": (
            f"{checkpoint_root}/payments"
        ),
        "schema_path": (
            f"{schema_root}/payments"
        ),
    },

    "products": {
        "format": "parquet",
        "source_path": (
            f"{landing_root}/products"
        ),
        "target_table": (
            f"{catalog_name}.{bronze_schema}."
            "products_raw"
        ),
        "checkpoint_path": (
            f"{checkpoint_root}/products"
        ),
        "schema_path": (
            f"{schema_root}/products"
        ),
    },

    "sellers": {
        "format": "csv",
        "source_path": (
            f"{landing_root}/sellers"
        ),
        "target_table": (
            f"{catalog_name}.{bronze_schema}."
            "sellers_raw"
        ),
        "checkpoint_path": (
            f"{checkpoint_root}/sellers"
        ),
        "schema_path": (
            f"{schema_root}/sellers"
        ),
    },
}


# ---------------------------------------------------------
# 3. Helpers
# ---------------------------------------------------------

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


# ---------------------------------------------------------
# 4. Validate source paths
# ---------------------------------------------------------

configuration_rows = []

for dataset, config in SOURCE_CONFIG.items():

    source_exists = path_exists(
        config["source_path"]
    )

    configuration_rows.append(
        (
            dataset,
            config["format"],
            config["source_path"],
            source_exists,
            config["target_table"],
            config["checkpoint_path"],
            config["schema_path"],
            (
                config["checkpoint_path"] !=
                config["schema_path"]
            ),
        )
    )


configuration_schema = """
    dataset STRING,
    source_format STRING,
    source_path STRING,
    source_exists BOOLEAN,
    target_table STRING,
    checkpoint_path STRING,
    schema_path STRING,
    state_paths_are_separate BOOLEAN
"""

autoloader_config_df = spark.createDataFrame(
    configuration_rows,
    configuration_schema
)

missing_sources = (
    autoloader_config_df
    .filter(~F.col("source_exists"))
    .count()
)

duplicate_checkpoint_count = (
    len(SOURCE_CONFIG) -
    len({
        config["checkpoint_path"]
        for config in SOURCE_CONFIG.values()
    })
)

duplicate_schema_path_count = (
    len(SOURCE_CONFIG) -
    len({
        config["schema_path"]
        for config in SOURCE_CONFIG.values()
    })
)

assert missing_sources == 0
assert duplicate_checkpoint_count == 0
assert duplicate_schema_path_count == 0


# ---------------------------------------------------------
# 5. Create Auto Loader state roots
# ---------------------------------------------------------

dbutils.fs.mkdirs(checkpoint_root)
dbutils.fs.mkdirs(schema_root)

assert path_exists(checkpoint_root)
assert path_exists(schema_root)


# ---------------------------------------------------------
# 6. Results
# ---------------------------------------------------------

print("Auto Loader configuration validated")
print(f"Configured streams:       {len(SOURCE_CONFIG)}")
print(f"Missing source paths:     {missing_sources}")
print(
    f"Duplicate checkpoints:    "
    f"{duplicate_checkpoint_count}"
)
print(
    f"Duplicate schema paths:   "
    f"{duplicate_schema_path_count}"
)
print(f"Checkpoint root:          {checkpoint_root}")
print(f"Schema root:              {schema_root}")

display(
    autoloader_config_df.orderBy("dataset")
)

# COMMAND ----------

# COMMAND ----------
# 2. Load User Events into Bronze with Auto Loader

dataset = "user_events"
config = SOURCE_CONFIG[dataset]

source_path = config["source_path"]
target_table = config["target_table"]
checkpoint_path = config["checkpoint_path"]
schema_path = config["schema_path"]


# ---------------------------------------------------------
# 1. Build streaming DataFrame
# ---------------------------------------------------------

user_events_stream_df = (
    spark.readStream
    .format("cloudFiles")

    .option(
        "cloudFiles.format",
        "json"
    )

    .option(
        "cloudFiles.schemaLocation",
        schema_path
    )

    .option(
        "cloudFiles.schemaEvolutionMode",
        "rescue"
    )

    .option(
        "cloudFiles.inferColumnTypes",
        "true"
    )

    .option(
        "cloudFiles.includeExistingFiles",
        "true"
    )

    .option(
        "rescuedDataColumn",
        "_rescued_data"
    )

    .option(
        "mode",
        "PERMISSIVE"
    )

    .load(source_path)
)


# ---------------------------------------------------------
# 2. Add Bronze metadata
# ---------------------------------------------------------

user_events_bronze_df = (
    user_events_stream_df

    .withColumn(
        "_ingest_ts",
        F.current_timestamp()
    )

    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )

    .withColumn(
        "_batch_id",
        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"batch_id=([^/]+)",
            1
        )
    )

    .withColumn(
        "_load_date",
        F.to_date(
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"load_date=([^/]+)",
                1
            ),
            "yyyy-MM-dd"
        )
    )
)


# ---------------------------------------------------------
# 3. Register lifecycle transitions
# ---------------------------------------------------------

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'DISCOVERED',
    discovered_at = COALESCE(
        discovered_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'user_events'
  AND batch_id = '20260922_162158'
  AND batch_status = 'LANDED'
""")

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'PROCESSING',
    processing_started_at = COALESCE(
        processing_started_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'user_events'
  AND batch_id = '20260922_162158'
  AND batch_status IN (
      'DISCOVERED',
      'PROCESSING'
  )
""")


# ---------------------------------------------------------
# 4. Run Trigger.AvailableNow
# ---------------------------------------------------------

try:
    query = (
        user_events_bronze_df
        .writeStream

        .queryName(
            "bronze_user_events_available_now"
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .option(
            "mergeSchema",
            "true"
        )

        .trigger(
            availableNow=True
        )

        .toTable(target_table)
    )

    query.awaitTermination()

    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = current_timestamp(),
        bronze_table =
            'marketplace_lab.bronze.user_events_raw',
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = 'user_events'
      AND batch_id = '20260922_162158'
      AND batch_status = 'PROCESSING'
    """)

except Exception:
    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'FAILED',
        error_message =
            'Auto Loader execution failed; inspect notebook logs',
        updated_at = current_timestamp()
    WHERE dataset = 'user_events'
      AND batch_id = '20260922_162158'
    """)

    raise


# ---------------------------------------------------------
# 5. Validate Bronze table
# ---------------------------------------------------------

user_events_bronze_count = (
    spark.table(target_table)
    .count()
)

rescued_event_count = (
    spark.table(target_table)
    .filter(
        F.col("_rescued_data").isNotNull()
    )
    .count()
)

missing_batch_metadata = (
    spark.table(target_table)
    .filter(
        (F.col("_batch_id").isNull()) |
        (F.col("_batch_id") == "") |
        (F.col("_load_date").isNull()) |
        (F.col("_source_file").isNull())
    )
    .count()
)

source_file_count = (
    spark.table(target_table)
    .select("_source_file")
    .distinct()
    .count()
)

registry_status = (
    spark.table(
        "marketplace_lab.ops.batch_registry"
    )
    .filter(
        (F.col("dataset") == "user_events") &
        (
            F.col("batch_id") ==
            "20260922_162158"
        )
    )
    .select("batch_status")
    .first()["batch_status"]
)

assert user_events_bronze_count == 50_400
assert missing_batch_metadata == 0
assert source_file_count == 12
assert registry_status == "BRONZE_WRITTEN"

print("User Events Auto Loader completed")
print(f"Bronze rows:          {user_events_bronze_count}")
print(f"Source files:         {source_file_count}")
print(f"Rescued rows:         {rescued_event_count}")
print(f"Missing metadata:     {missing_batch_metadata}")
print(f"Registry status:      {registry_status}")
print(f"Target table:         {target_table}")
print(f"Checkpoint location:  {checkpoint_path}")
print(f"Schema location:      {schema_path}")

display(
    spark.table(target_table)
    .groupBy(
        "_load_date",
        "_batch_id"
    )
    .agg(
        F.count("*").alias("row_count"),

        F.sum(
            F.when(
                F.col("_rescued_data").isNotNull(),
                1
            ).otherwise(0)
        ).alias("rescued_row_count"),

        F.countDistinct(
            "_source_file"
        ).alias("source_file_count")
    )
    .orderBy(
        "_load_date",
        "_batch_id"
    )
)

# COMMAND ----------

# COMMAND ----------
# 3. Diagnose malformed User Events handling

target_table = (
    "marketplace_lab.bronze.user_events_raw"
)

bronze_events_df = spark.table(target_table)

table_columns = bronze_events_df.columns

event_time_type = dict(
    bronze_events_df.dtypes
).get("event_time")

rescued_count = (
    bronze_events_df
    .filter(
        F.col("_rescued_data").isNotNull()
    )
    .count()
)

corrupted_source_count = (
    bronze_events_df
    .filter(
        F.col("_source_file").contains(
            "/corrupted_records.json"
        )
    )
    .count()
)

invalid_key_source_count = (
    bronze_events_df
    .filter(
        F.col("_source_file").contains(
            "/invalid_key_records.json"
        )
    )
    .count()
)

null_event_id_count = (
    bronze_events_df
    .filter(F.col("event_id").isNull())
    .count()
)

null_user_id_count = (
    bronze_events_df
    .filter(F.col("user_id").isNull())
    .count()
)

invalid_event_time_text_count = (
    bronze_events_df
    .filter(
        F.col("event_time").cast("string") ==
        "invalid-date"
    )
    .count()
)

if "_corrupt_record" in table_columns:
    corrupt_record_count = (
        bronze_events_df
        .filter(
            F.col("_corrupt_record").isNotNull()
        )
        .count()
    )
else:
    corrupt_record_count = 0


print("User Events malformed-data diagnostics")
print(f"event_time type:          {event_time_type}")
print(f"Rescued rows:             {rescued_count}")
print(f"Corrupted-source rows:    {corrupted_source_count}")
print(f"Invalid-key source rows:  {invalid_key_source_count}")
print(f"Null event_id rows:       {null_event_id_count}")
print(f"Null user_id rows:        {null_user_id_count}")
print(f"invalid-date values:      {invalid_event_time_text_count}")
print(f"_corrupt_record rows:     {corrupt_record_count}")
print(f"Columns:                  {table_columns}")

display(
    bronze_events_df
    .filter(
        F.col("_source_file").contains(
            "/corrupted_records.json"
        )
    )
    .limit(20)
)

# COMMAND ----------

# COMMAND ----------
# 4. Reset User Events Bronze stream before schema correction

dataset = "user_events"
config = SOURCE_CONFIG[dataset]

source_path = config["source_path"]
target_table = config["target_table"]
checkpoint_path = config["checkpoint_path"]
schema_path = config["schema_path"]


def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


# ---------------------------------------------------------
# 1. Pre-reset safety checks
# ---------------------------------------------------------

source_data_files = [
    item
    for load_date_dir in dbutils.fs.ls(source_path)
    if load_date_dir.isDir()
    for batch_dir in dbutils.fs.ls(load_date_dir.path)
    if batch_dir.isDir()
    for item in dbutils.fs.ls(batch_dir.path)
    if not item.name.startswith("_")
    and not item.name.startswith(".")
]

assert len(source_data_files) == 12, (
    f"Expected 12 source files, "
    f"but found {len(source_data_files)}"
)

assert spark.catalog.tableExists(target_table), (
    f"Bronze table does not exist: {target_table}"
)

existing_bronze_rows = (
    spark.table(target_table).count()
)

assert existing_bronze_rows == 50_400


# ---------------------------------------------------------
# 2. Remove only derived Bronze state
# ---------------------------------------------------------

spark.sql(
    f"DROP TABLE IF EXISTS {target_table}"
)

checkpoint_removed = (
    dbutils.fs.rm(
        checkpoint_path,
        recurse=True
    )
    if path_exists(checkpoint_path)
    else True
)

schema_removed = (
    dbutils.fs.rm(
        schema_path,
        recurse=True
    )
    if path_exists(schema_path)
    else True
)


# ---------------------------------------------------------
# 3. Return registry to LANDED
# ---------------------------------------------------------

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'LANDED',
    discovered_at = NULL,
    processing_started_at = NULL,
    processed_at = NULL,
    bronze_table = NULL,
    error_message = NULL,
    updated_at = current_timestamp()
WHERE dataset = 'user_events'
  AND batch_id = '20260922_162158'
""")


# ---------------------------------------------------------
# 4. Post-reset validation
# ---------------------------------------------------------

table_still_exists = (
    spark.catalog.tableExists(target_table)
)

checkpoint_still_exists = (
    path_exists(checkpoint_path)
)

schema_still_exists = (
    path_exists(schema_path)
)

registry_status = (
    spark.table(
        "marketplace_lab.ops.batch_registry"
    )
    .filter(
        (F.col("dataset") == "user_events") &
        (
            F.col("batch_id") ==
            "20260922_162158"
        )
    )
    .select("batch_status")
    .first()["batch_status"]
)

assert not table_still_exists
assert not checkpoint_still_exists
assert not schema_still_exists
assert registry_status == "LANDED"
assert path_exists(source_path)

print("User Events Bronze state reset completed")
print(f"Source files preserved: {len(source_data_files)}")
print(f"Bronze table exists:    {table_still_exists}")
print(f"Checkpoint exists:      {checkpoint_still_exists}")
print(f"Schema path exists:     {schema_still_exists}")
print(f"Registry status:        {registry_status}")

# COMMAND ----------

# COMMAND ----------
# 5. Reload User Events with explicit schema and corrupt-data rescue

dataset = "user_events"
config = SOURCE_CONFIG[dataset]

source_path = config["source_path"]
target_table = config["target_table"]
checkpoint_path = config["checkpoint_path"]
schema_path = config["schema_path"]


# ---------------------------------------------------------
# 1. Explicit source schema
# ---------------------------------------------------------

user_events_schema = T.StructType([
    T.StructField(
        "event_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "event_type",
        T.StringType(),
        True
    ),
    T.StructField(
        "event_time",
        T.TimestampType(),
        True
    ),
    T.StructField(
        "user_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "product_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "order_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "session_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "price",
        T.DecimalType(12, 2),
        True
    ),
    T.StructField(
        "currency",
        T.StringType(),
        True
    ),
    T.StructField(
        "quantity",
        T.IntegerType(),
        True
    ),
    T.StructField(
        "country",
        T.StringType(),
        True
    ),
    T.StructField(
        "device",
        T.StringType(),
        True
    ),
    T.StructField(
        "ingestion_time",
        T.TimestampType(),
        True
    ),

    # Required by the permissive JSON parser.
    T.StructField(
        "_corrupt_record",
        T.StringType(),
        True
    ),
])


# ---------------------------------------------------------
# 2. Build corrected Auto Loader stream
# ---------------------------------------------------------

user_events_stream_df = (
    spark.readStream
    .format("cloudFiles")

    .option(
        "cloudFiles.format",
        "json"
    )

    .option(
        "cloudFiles.schemaLocation",
        schema_path
    )

    .option(
        "cloudFiles.schemaEvolutionMode",
        "rescue"
    )

    .option(
        "cloudFiles.includeExistingFiles",
        "true"
    )

    .option(
        "rescuedDataColumn",
        "_auto_rescued_data"
    )

    .option(
        "columnNameOfCorruptRecord",
        "_corrupt_record"
    )

    .option(
        "mode",
        "PERMISSIVE"
    )

    .option(
        "timestampFormat",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    )

    .schema(user_events_schema)

    .load(source_path)
)


# ---------------------------------------------------------
# 3. Normalize rescued data and add lineage
# ---------------------------------------------------------

user_events_bronze_df = (
    user_events_stream_df

    # Auto Loader type rescue has priority.
    # Raw malformed JSON is used when parsing failed.
    .withColumn(
        "_rescued_data",
        F.coalesce(
            F.col("_auto_rescued_data"),
            F.col("_corrupt_record")
        )
    )

    .drop(
        "_auto_rescued_data",
        "_corrupt_record"
    )

    .withColumn(
        "_ingest_ts",
        F.current_timestamp()
    )

    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )

    .withColumn(
        "_batch_id",
        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"batch_id=([^/]+)",
            1
        )
    )

    .withColumn(
        "_load_date",
        F.to_date(
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"load_date=([^/]+)",
                1
            ),
            "yyyy-MM-dd"
        )
    )
)


# ---------------------------------------------------------
# 4. Lifecycle status
# ---------------------------------------------------------

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'DISCOVERED',
    discovered_at = COALESCE(
        discovered_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'user_events'
  AND batch_id = '20260922_162158'
  AND batch_status = 'LANDED'
""")

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'PROCESSING',
    processing_started_at = COALESCE(
        processing_started_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'user_events'
  AND batch_id = '20260922_162158'
  AND batch_status IN (
      'DISCOVERED',
      'PROCESSING'
  )
""")


# ---------------------------------------------------------
# 5. Run AvailableNow
# ---------------------------------------------------------

try:
    query = (
        user_events_bronze_df
        .writeStream

        .queryName(
            "bronze_user_events_available_now_v2"
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .option(
            "mergeSchema",
            "true"
        )

        .trigger(
            availableNow=True
        )

        .toTable(target_table)
    )

    query.awaitTermination()

    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = current_timestamp(),
        bronze_table =
            'marketplace_lab.bronze.user_events_raw',
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = 'user_events'
      AND batch_id = '20260922_162158'
      AND batch_status = 'PROCESSING'
    """)

except Exception:
    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'FAILED',
        error_message =
            'Corrected Auto Loader execution failed',
        updated_at = current_timestamp()
    WHERE dataset = 'user_events'
      AND batch_id = '20260922_162158'
    """)

    raise


# ---------------------------------------------------------
# 6. Validate corrected Bronze table
# ---------------------------------------------------------

bronze_events_df = spark.table(
    target_table
)

bronze_row_count = (
    bronze_events_df.count()
)

rescued_row_count = (
    bronze_events_df
    .filter(
        F.col("_rescued_data").isNotNull()
    )
    .count()
)

corrupted_source_count = (
    bronze_events_df
    .filter(
        F.col("_source_file").contains(
            "/corrupted_records.json"
        )
    )
    .count()
)

corrupted_rows_without_rescue = (
    bronze_events_df
    .filter(
        F.col("_source_file").contains(
            "/corrupted_records.json"
        ) &
        F.col("_rescued_data").isNull()
    )
    .count()
)

invalid_key_count = (
    bronze_events_df
    .filter(
        F.col("_source_file").contains(
            "/invalid_key_records.json"
        )
    )
    .count()
)

missing_metadata_count = (
    bronze_events_df
    .filter(
        F.col("_source_file").isNull() |
        F.col("_load_date").isNull() |
        F.col("_batch_id").isNull() |
        (F.col("_batch_id") == "")
    )
    .count()
)

source_file_count = (
    bronze_events_df
    .select("_source_file")
    .distinct()
    .count()
)

event_time_type = dict(
    bronze_events_df.dtypes
)["event_time"]

registry_status = (
    spark.table(
        "marketplace_lab.ops.batch_registry"
    )
    .filter(
        (F.col("dataset") == "user_events") &
        (
            F.col("batch_id") ==
            "20260922_162158"
        )
    )
    .select("batch_status")
    .first()["batch_status"]
)


assert bronze_row_count == 50_400
assert rescued_row_count == 150
assert corrupted_source_count == 150
assert corrupted_rows_without_rescue == 0
assert invalid_key_count == 250
assert missing_metadata_count == 0
assert source_file_count == 12
assert event_time_type == "timestamp"
assert registry_status == "BRONZE_WRITTEN"


print("Corrected User Events Auto Loader completed")
print(f"Bronze rows:                 {bronze_row_count}")
print(f"Rescued rows:                {rescued_row_count}")
print(f"Corrupted-source rows:       {corrupted_source_count}")
print(
    f"Corrupted rows not rescued:  "
    f"{corrupted_rows_without_rescue}"
)
print(f"Invalid-key rows:            {invalid_key_count}")
print(f"Source files:                {source_file_count}")
print(f"Missing metadata:            {missing_metadata_count}")
print(f"event_time type:             {event_time_type}")
print(f"Registry status:             {registry_status}")

display(
    bronze_events_df
    .filter(
        F.col("_rescued_data").isNotNull()
    )
    .select(
        "event_id",
        "event_type",
        "event_time",
        "user_id",
        "_rescued_data",
        "_source_file",
        "_batch_id",
        "_load_date",
        "_ingest_ts"
    )
    .limit(20)
)

# COMMAND ----------

# COMMAND ----------
# 6. Load Orders into Bronze with explicit evolved schema

dataset = "orders"
config = SOURCE_CONFIG[dataset]

source_path = config["source_path"]
target_table = config["target_table"]
checkpoint_path = config["checkpoint_path"]
schema_path = config["schema_path"]


# ---------------------------------------------------------
# 1. Explicit nested schema, including schema version 2
# ---------------------------------------------------------

order_item_schema = T.StructType([
    T.StructField(
        "product_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "quantity",
        T.IntegerType(),
        True
    ),
    T.StructField(
        "unit_price",
        T.DecimalType(12, 2),
        True
    ),
    T.StructField(
        "line_amount",
        T.DecimalType(14, 2),
        True
    ),
])

orders_schema = T.StructType([
    T.StructField(
        "order_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "user_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "seller_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "items",
        T.ArrayType(
            order_item_schema,
            containsNull=True
        ),
        True
    ),
    T.StructField(
        "total_quantity",
        T.LongType(),
        True
    ),
    T.StructField(
        "order_amount",
        T.DecimalType(18, 2),
        True
    ),
    T.StructField(
        "currency",
        T.StringType(),
        True
    ),
    T.StructField(
        "order_status",
        T.StringType(),
        True
    ),
    T.StructField(
        "created_at",
        T.TimestampType(),
        True
    ),
    T.StructField(
        "updated_at",
        T.TimestampType(),
        True
    ),

    # Schema version 2 fields
    T.StructField(
        "sales_channel",
        T.StringType(),
        True
    ),
    T.StructField(
        "coupon_code",
        T.StringType(),
        True
    ),
    T.StructField(
        "delivery_priority",
        T.StringType(),
        True
    ),
    T.StructField(
        "schema_version",
        T.IntegerType(),
        True
    ),

    T.StructField(
        "_corrupt_record",
        T.StringType(),
        True
    ),
])


# ---------------------------------------------------------
# 2. Build stream
# ---------------------------------------------------------

orders_stream_df = (
    spark.readStream
    .format("cloudFiles")

    .option(
        "cloudFiles.format",
        "json"
    )

    .option(
        "cloudFiles.schemaLocation",
        schema_path
    )

    .option(
        "cloudFiles.schemaEvolutionMode",
        "rescue"
    )

    .option(
        "cloudFiles.includeExistingFiles",
        "true"
    )

    .option(
        "rescuedDataColumn",
        "_auto_rescued_data"
    )

    .option(
        "columnNameOfCorruptRecord",
        "_corrupt_record"
    )

    .option(
        "mode",
        "PERMISSIVE"
    )

    .option(
        "timestampFormat",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    )

    .schema(orders_schema)

    .load(source_path)
)


# ---------------------------------------------------------
# 3. Add Bronze metadata
# ---------------------------------------------------------

orders_bronze_df = (
    orders_stream_df

    .withColumn(
        "_rescued_data",
        F.coalesce(
            F.col("_auto_rescued_data"),
            F.col("_corrupt_record")
        )
    )

    .drop(
        "_auto_rescued_data",
        "_corrupt_record"
    )

    .withColumn(
        "_ingest_ts",
        F.current_timestamp()
    )

    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )

    .withColumn(
        "_batch_id",
        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"batch_id=([^/]+)",
            1
        )
    )

    .withColumn(
        "_load_date",
        F.to_date(
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"load_date=([^/]+)",
                1
            ),
            "yyyy-MM-dd"
        )
    )
)


# ---------------------------------------------------------
# 4. Registry: both Orders batches
# ---------------------------------------------------------

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'DISCOVERED',
    discovered_at = COALESCE(
        discovered_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'orders'
  AND batch_status = 'LANDED'
""")

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'PROCESSING',
    processing_started_at = COALESCE(
        processing_started_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'orders'
  AND batch_status IN (
      'DISCOVERED',
      'PROCESSING'
  )
""")


# ---------------------------------------------------------
# 5. Run AvailableNow
# ---------------------------------------------------------

try:
    query = (
        orders_bronze_df
        .writeStream

        .queryName(
            "bronze_orders_available_now"
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .option(
            "mergeSchema",
            "true"
        )

        .trigger(
            availableNow=True
        )

        .toTable(target_table)
    )

    query.awaitTermination()

    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = current_timestamp(),
        bronze_table =
            'marketplace_lab.bronze.orders_raw',
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = 'orders'
      AND batch_status = 'PROCESSING'
    """)

except Exception:
    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'FAILED',
        error_message =
            'Orders Auto Loader execution failed',
        updated_at = current_timestamp()
    WHERE dataset = 'orders'
      AND batch_status = 'PROCESSING'
    """)

    raise


# ---------------------------------------------------------
# 6. Validate Orders Bronze
# ---------------------------------------------------------

bronze_orders_df = spark.table(
    target_table
)

orders_bronze_count = (
    bronze_orders_df.count()
)

orders_v1_count = (
    bronze_orders_df
    .filter(
        F.col("_batch_id") ==
        "20260922_162158"
    )
    .count()
)

orders_v2_count = (
    bronze_orders_df
    .filter(
        F.col("_batch_id") ==
        "20260923_162158"
    )
    .count()
)

orders_v2_schema_count = (
    bronze_orders_df
    .filter(
        (F.col("_batch_id") == "20260923_162158") &
        (F.col("schema_version") == 2) &
        F.col("sales_channel").isNotNull() &
        F.col("delivery_priority").isNotNull()
    )
    .count()
)

rescued_orders_count = (
    bronze_orders_df
    .filter(
        F.col("_rescued_data").isNotNull()
    )
    .count()
)

missing_orders_metadata = (
    bronze_orders_df
    .filter(
        F.col("_source_file").isNull() |
        F.col("_load_date").isNull() |
        F.col("_batch_id").isNull() |
        (F.col("_batch_id") == "")
    )
    .count()
)

orders_source_file_count = (
    bronze_orders_df
    .select("_source_file")
    .distinct()
    .count()
)

invalid_registry_count = (
    spark.table(
        "marketplace_lab.ops.batch_registry"
    )
    .filter(
        (F.col("dataset") == "orders") &
        (
            F.col("batch_status") !=
            "BRONZE_WRITTEN"
        )
    )
    .count()
)


assert orders_bronze_count == 10_500
assert orders_v1_count == 10_000
assert orders_v2_count == 500
assert orders_v2_schema_count == 500
assert rescued_orders_count == 0
assert missing_orders_metadata == 0
assert orders_source_file_count == 6
assert invalid_registry_count == 0


print("Orders Auto Loader completed")
print(f"Total Bronze rows:       {orders_bronze_count}")
print(f"Schema-v1 rows:          {orders_v1_count}")
print(f"Schema-v2 rows:          {orders_v2_count}")
print(f"Valid schema-v2 rows:    {orders_v2_schema_count}")
print(f"Rescued rows:            {rescued_orders_count}")
print(f"Source files:            {orders_source_file_count}")
print(f"Missing metadata:        {missing_orders_metadata}")
print(f"Invalid registry states: {invalid_registry_count}")
print(f"Target table:            {target_table}")

display(
    bronze_orders_df
    .groupBy(
        "_load_date",
        "_batch_id",
        "schema_version"
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct(
            "_source_file"
        ).alias("source_file_count"),
        F.sum(
            F.when(
                F.col("_rescued_data").isNotNull(),
                1
            ).otherwise(0)
        ).alias("rescued_row_count")
    )
    .orderBy(
        "_load_date",
        "_batch_id",
        "schema_version"
    )
)

# COMMAND ----------

# COMMAND ----------
# 7. Load Payments into Bronze with corrupt CSV rescue

dataset = "payments"
config = SOURCE_CONFIG[dataset]

source_path = config["source_path"]
target_table = config["target_table"]
checkpoint_path = config["checkpoint_path"]
schema_path = config["schema_path"]


# ---------------------------------------------------------
# 1. Explicit Payments schema
# ---------------------------------------------------------

payments_schema = T.StructType([
    T.StructField(
        "payment_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "order_id",
        T.StringType(),
        True
    ),
    T.StructField(
        "payment_status",
        T.StringType(),
        True
    ),
    T.StructField(
        "payment_method",
        T.StringType(),
        True
    ),
    T.StructField(
        "amount",
        T.DecimalType(18, 2),
        True
    ),
    T.StructField(
        "currency",
        T.StringType(),
        True
    ),
    T.StructField(
        "payment_time",
        T.TimestampType(),
        True
    ),
    T.StructField(
        "_corrupt_record",
        T.StringType(),
        True
    ),
])


# ---------------------------------------------------------
# 2. Build Payments Auto Loader stream
# ---------------------------------------------------------

payments_stream_df = (
    spark.readStream
    .format("cloudFiles")

    .option(
        "cloudFiles.format",
        "csv"
    )

    .option(
        "cloudFiles.schemaLocation",
        schema_path
    )

    .option(
        "cloudFiles.schemaEvolutionMode",
        "rescue"
    )

    .option(
        "cloudFiles.includeExistingFiles",
        "true"
    )

    .option(
        "rescuedDataColumn",
        "_auto_rescued_data"
    )

    .option(
        "columnNameOfCorruptRecord",
        "_corrupt_record"
    )

    .option(
        "mode",
        "PERMISSIVE"
    )

    .option(
        "header",
        "true"
    )

    .option(
        "delimiter",
        ","
    )

    .option(
        "timestampFormat",
        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
    )

    .schema(payments_schema)

    .load(source_path)
)


# ---------------------------------------------------------
# 3. Normalize rescued data and add lineage
# ---------------------------------------------------------

payments_bronze_df = (
    payments_stream_df

    .withColumn(
        "_rescued_data",
        F.coalesce(
            F.col("_auto_rescued_data"),
            F.col("_corrupt_record")
        )
    )

    .drop(
        "_auto_rescued_data",
        "_corrupt_record"
    )

    .withColumn(
        "_ingest_ts",
        F.current_timestamp()
    )

    .withColumn(
        "_source_file",
        F.col("_metadata.file_path")
    )

    .withColumn(
        "_batch_id",
        F.regexp_extract(
            F.col("_metadata.file_path"),
            r"batch_id=([^/]+)",
            1
        )
    )

    .withColumn(
        "_load_date",
        F.to_date(
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"load_date=([^/]+)",
                1
            ),
            "yyyy-MM-dd"
        )
    )
)


# ---------------------------------------------------------
# 4. Registry lifecycle
# ---------------------------------------------------------

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'DISCOVERED',
    discovered_at = COALESCE(
        discovered_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'payments'
  AND batch_id = '20260922_162158'
  AND batch_status = 'LANDED'
""")

spark.sql("""
UPDATE marketplace_lab.ops.batch_registry
SET
    batch_status = 'PROCESSING',
    processing_started_at = COALESCE(
        processing_started_at,
        current_timestamp()
    ),
    updated_at = current_timestamp()
WHERE dataset = 'payments'
  AND batch_id = '20260922_162158'
  AND batch_status IN (
      'DISCOVERED',
      'PROCESSING'
  )
""")


# ---------------------------------------------------------
# 5. Run AvailableNow
# ---------------------------------------------------------

try:
    query = (
        payments_bronze_df
        .writeStream

        .queryName(
            "bronze_payments_available_now"
        )

        .option(
            "checkpointLocation",
            checkpoint_path
        )

        .option(
            "mergeSchema",
            "true"
        )

        .trigger(
            availableNow=True
        )

        .toTable(target_table)
    )

    query.awaitTermination()

    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = current_timestamp(),
        bronze_table =
            'marketplace_lab.bronze.payments_raw',
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = 'payments'
      AND batch_id = '20260922_162158'
      AND batch_status = 'PROCESSING'
    """)

except Exception:
    spark.sql("""
    UPDATE marketplace_lab.ops.batch_registry
    SET
        batch_status = 'FAILED',
        error_message =
            'Payments Auto Loader execution failed',
        updated_at = current_timestamp()
    WHERE dataset = 'payments'
      AND batch_id = '20260922_162158'
    """)

    raise


# ---------------------------------------------------------
# 6. Validate Payments Bronze
# ---------------------------------------------------------

bronze_payments_df = spark.table(
    target_table
)

payments_bronze_count = (
    bronze_payments_df.count()
)

payments_source_file_count = (
    bronze_payments_df
    .select("_source_file")
    .distinct()
    .count()
)

rescued_payments_count = (
    bronze_payments_df
    .filter(
        F.col("_rescued_data").isNotNull()
    )
    .count()
)

invalid_business_count = (
    bronze_payments_df
    .filter(
        F.col("_source_file").contains(
            "/invalid_business_records.csv"
        )
    )
    .count()
)

unknown_status_count = (
    bronze_payments_df
    .filter(
        F.col("payment_status") ==
        "UNKNOWN_STATUS"
    )
    .count()
)

invalid_order_key_count = (
    bronze_payments_df
    .filter(
        F.col("order_id").startswith(
            "UNKNOWN-"
        )
    )
    .count()
)

corrupted_payment_count = (
    bronze_payments_df
    .filter(
        F.col("_source_file").contains(
            "/corrupted_records.csv"
        )
    )
    .count()
)

corrupted_not_rescued = (
    bronze_payments_df
    .filter(
        F.col("_source_file").contains(
            "/corrupted_records.csv"
        ) &
        F.col("_rescued_data").isNull()
    )
    .count()
)

missing_payment_metadata = (
    bronze_payments_df
    .filter(
        F.col("_source_file").isNull() |
        F.col("_load_date").isNull() |
        F.col("_batch_id").isNull() |
        (F.col("_batch_id") == "")
    )
    .count()
)

registry_status = (
    spark.table(
        "marketplace_lab.ops.batch_registry"
    )
    .filter(
        (F.col("dataset") == "payments") &
        (
            F.col("batch_id") ==
            "20260922_162158"
        )
    )
    .select("batch_status")
    .first()["batch_status"]
)


assert payments_bronze_count == 10_080
assert payments_source_file_count == 7
assert invalid_business_count == 50
assert unknown_status_count == 50
assert invalid_order_key_count == 50
assert corrupted_payment_count == 30
assert corrupted_not_rescued == 0
assert missing_payment_metadata == 0
assert registry_status == "BRONZE_WRITTEN"


print("Payments Auto Loader completed")
print(f"Bronze rows:              {payments_bronze_count}")
print(f"Source files:             {payments_source_file_count}")
print(f"Rescued rows:             {rescued_payments_count}")
print(f"Invalid business rows:    {invalid_business_count}")
print(f"Unknown statuses:         {unknown_status_count}")
print(f"Invalid order keys:       {invalid_order_key_count}")
print(f"Corrupted CSV rows:       {corrupted_payment_count}")
print(f"Corrupted not rescued:    {corrupted_not_rescued}")
print(f"Missing metadata:         {missing_payment_metadata}")
print(f"Registry status:          {registry_status}")

display(
    bronze_payments_df
    .filter(
        F.col("_rescued_data").isNotNull() |
        (F.col("payment_status") == "UNKNOWN_STATUS")
    )
    .select(
        "payment_id",
        "order_id",
        "payment_status",
        "payment_method",
        "amount",
        "currency",
        "payment_time",
        "_rescued_data",
        "_source_file",
        "_batch_id",
        "_load_date"
    )
    .limit(100)
)

# COMMAND ----------

# COMMAND ----------
# Products -> Bronze через Auto Loader

from pyspark.sql import functions as F

dataset = "products"

source_path = (
    "/Volumes/marketplace_lab/bronze/landing/products"
)

target_table = (
    "marketplace_lab.bronze.products_raw"
)

checkpoint_path = (
    "/Volumes/marketplace_lab/ops/checkpoints/"
    "_autoloader/checkpoints/products"
)

schema_path = (
    "/Volumes/marketplace_lab/ops/checkpoints/"
    "_autoloader/schemas/products"
)

registry_table = "marketplace_lab.ops.batch_registry"


# ---------------------------------------------------------
# 1. Получаем фактическую схему исходных Parquet-файлов.
#
# recursiveFileLookup отключает автоматическое добавление
# load_date и batch_id из структуры каталогов.
# Полученная схема далее явно передаётся Auto Loader.
# ---------------------------------------------------------

products_schema = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(source_path)
    .schema
)

print("Products source schema:")
print(products_schema.simpleString())


# ---------------------------------------------------------
# 2. Регистрируем начало обработки
# ---------------------------------------------------------

spark.sql(f"""
UPDATE {registry_table}
SET
    batch_status = 'DISCOVERED',
    discovered_at = COALESCE(discovered_at, current_timestamp()),
    updated_at = current_timestamp(),
    error_message = NULL
WHERE dataset = '{dataset}'
  AND batch_status IN ('LANDED', 'DISCOVERED', 'PROCESSING')
""")

spark.sql(f"""
UPDATE {registry_table}
SET
    batch_status = 'PROCESSING',
    processing_started_at =
        COALESCE(processing_started_at, current_timestamp()),
    updated_at = current_timestamp(),
    error_message = NULL
WHERE dataset = '{dataset}'
  AND batch_status IN ('DISCOVERED', 'PROCESSING')
""")


# ---------------------------------------------------------
# 3. Auto Loader stream
# ---------------------------------------------------------

try:
    products_stream_df = (
        spark.readStream
        .format("cloudFiles")
        .schema(products_schema)
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("rescuedDataColumn", "_auto_rescued_data")
        .load(source_path)

        # Технические поля происхождения данных
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )
        .withColumn(
            "_batch_id",
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"batch_id=([^/]+)",
                1
            )
        )
        .withColumn(
            "_load_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"load_date=([^/]+)",
                    1
                )
            )
        )
        .withColumn(
            "_ingest_ts",
            F.current_timestamp()
        )
        .withColumn(
            "_rescued_data",
            F.col("_auto_rescued_data")
        )
        .drop("_auto_rescued_data")
    )

    products_query = (
        products_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    products_query.awaitTermination()

    spark.sql(f"""
    UPDATE {registry_table}
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = COALESCE(processed_at, current_timestamp()),
        bronze_table = '{target_table}',
        updated_at = current_timestamp(),
        error_message = NULL
    WHERE dataset = '{dataset}'
      AND batch_status IN (
          'LANDED',
          'DISCOVERED',
          'PROCESSING',
          'BRONZE_WRITTEN'
      )
    """)

except Exception as error:
    escaped_error = (
        str(error)
        .replace("\\", "\\\\")
        .replace("'", "''")[:4000]
    )

    spark.sql(f"""
    UPDATE {registry_table}
    SET
        batch_status = 'FAILED',
        error_message = '{escaped_error}',
        updated_at = current_timestamp()
    WHERE dataset = '{dataset}'
      AND batch_status IN (
          'LANDED',
          'DISCOVERED',
          'PROCESSING'
      )
    """)

    raise


# ---------------------------------------------------------
# 4. Контроль результата
# ---------------------------------------------------------

products_bronze_df = spark.table(target_table)

bronze_rows = products_bronze_df.count()

source_file_count = (
    products_bronze_df
    .select("_source_file")
    .distinct()
    .count()
)

rescued_rows = (
    products_bronze_df
    .filter(F.col("_rescued_data").isNotNull())
    .count()
)

missing_metadata_rows = (
    products_bronze_df
    .filter(
        F.col("_source_file").isNull()
        | (F.col("_source_file") == "")
        | F.col("_batch_id").isNull()
        | (F.col("_batch_id") == "")
        | F.col("_load_date").isNull()
        | F.col("_ingest_ts").isNull()
    )
    .count()
)

invalid_registry_states = (
    spark.table(registry_table)
    .filter(
        (F.col("dataset") == dataset)
        & (F.col("batch_status") != "BRONZE_WRITTEN")
    )
    .count()
)

registry_statuses = [
    row["batch_status"]
    for row in (
        spark.table(registry_table)
        .filter(F.col("dataset") == dataset)
        .select("batch_status")
        .distinct()
        .collect()
    )
]


# ---------------------------------------------------------
# 5. Assertions
# ---------------------------------------------------------

assert bronze_rows == 2000, (
    f"Expected 2000 product rows, found {bronze_rows}"
)

assert source_file_count == 2, (
    f"Expected 2 source files, found {source_file_count}"
)

assert rescued_rows == 0, (
    f"Expected 0 rescued rows, found {rescued_rows}"
)

assert missing_metadata_rows == 0, (
    f"Rows with missing metadata: {missing_metadata_rows}"
)

assert invalid_registry_states == 0, (
    f"Registry rows outside BRONZE_WRITTEN: "
    f"{invalid_registry_states}"
)


# ---------------------------------------------------------
# 6. Итог
# ---------------------------------------------------------

print("Products Auto Loader completed")
print(f"Bronze rows:             {bronze_rows}")
print(f"Source files:            {source_file_count}")
print(f"Rescued rows:            {rescued_rows}")
print(f"Missing metadata:        {missing_metadata_rows}")
print(f"Registry status:         {registry_statuses}")
print(f"Target table:            {target_table}")
print(f"Checkpoint location:     {checkpoint_path}")
print(f"Schema location:         {schema_path}")

display(
    products_bronze_df
    .groupBy(
        "_load_date",
        "_batch_id"
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("_source_file").alias("source_file_count"),
        F.sum(
            F.when(
                F.col("_rescued_data").isNotNull(),
                1
            ).otherwise(0)
        ).alias("rescued_row_count")
    )
    .orderBy("_load_date", "_batch_id")
)

# COMMAND ----------

# COMMAND ----------
# Sellers -> Bronze через Auto Loader

from pyspark.sql import functions as F

dataset = "sellers"

source_path = (
    "/Volumes/marketplace_lab/bronze/landing/sellers"
)

target_table = (
    "marketplace_lab.bronze.sellers_raw"
)

checkpoint_path = (
    "/Volumes/marketplace_lab/ops/checkpoints/"
    "_autoloader/checkpoints/sellers"
)

schema_path = (
    "/Volumes/marketplace_lab/ops/checkpoints/"
    "_autoloader/schemas/sellers"
)

registry_table = "marketplace_lab.ops.batch_registry"


# ---------------------------------------------------------
# 1. Фиксируем схему существующих CSV-файлов
# ---------------------------------------------------------

sellers_schema = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.csv")
    .load(source_path)
    .schema
)

print("Sellers source schema:")
print(sellers_schema.simpleString())


# ---------------------------------------------------------
# 2. Регистрируем начало обработки
# ---------------------------------------------------------

spark.sql(f"""
UPDATE {registry_table}
SET
    batch_status = 'DISCOVERED',
    discovered_at = COALESCE(discovered_at, current_timestamp()),
    processing_started_at = NULL,
    processed_at = NULL,
    bronze_table = NULL,
    error_message = NULL,
    updated_at = current_timestamp()
WHERE dataset = '{dataset}'
  AND batch_status IN (
      'LANDED',
      'FAILED',
      'DISCOVERED',
      'PROCESSING'
  )
""")

spark.sql(f"""
UPDATE {registry_table}
SET
    batch_status = 'PROCESSING',
    processing_started_at =
        COALESCE(processing_started_at, current_timestamp()),
    updated_at = current_timestamp(),
    error_message = NULL
WHERE dataset = '{dataset}'
  AND batch_status = 'DISCOVERED'
""")


# ---------------------------------------------------------
# 3. Auto Loader stream
# ---------------------------------------------------------

try:
    sellers_stream_df = (
        spark.readStream
        .format("cloudFiles")
        .schema(sellers_schema)
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .option("mode", "PERMISSIVE")
        .option("pathGlobFilter", "*.csv")
        .option("rescuedDataColumn", "_auto_rescued_data")
        .load(source_path)

        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )
        .withColumn(
            "_batch_id",
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"batch_id=([^/]+)",
                1
            )
        )
        .withColumn(
            "_load_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"load_date=([^/]+)",
                    1
                )
            )
        )
        .withColumn(
            "_ingest_ts",
            F.current_timestamp()
        )
        .withColumn(
            "_rescued_data",
            F.col("_auto_rescued_data")
        )
        .drop("_auto_rescued_data")
    )

    sellers_query = (
        sellers_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target_table)
    )

    sellers_query.awaitTermination()

    spark.sql(f"""
    UPDATE {registry_table}
    SET
        batch_status = 'BRONZE_WRITTEN',
        processed_at = COALESCE(
            processed_at,
            current_timestamp()
        ),
        bronze_table = '{target_table}',
        error_message = NULL,
        updated_at = current_timestamp()
    WHERE dataset = '{dataset}'
      AND batch_status IN (
          'LANDED',
          'DISCOVERED',
          'PROCESSING',
          'BRONZE_WRITTEN'
      )
    """)

except Exception as error:
    escaped_error = (
        str(error)
        .replace("\\", "\\\\")
        .replace("'", "''")[:4000]
    )

    spark.sql(f"""
    UPDATE {registry_table}
    SET
        batch_status = 'FAILED',
        error_message = '{escaped_error}',
        updated_at = current_timestamp()
    WHERE dataset = '{dataset}'
      AND batch_status IN (
          'LANDED',
          'DISCOVERED',
          'PROCESSING'
      )
    """)

    raise


# ---------------------------------------------------------
# 4. Контроль результата
# ---------------------------------------------------------

sellers_bronze_df = spark.table(target_table)

bronze_rows = sellers_bronze_df.count()

source_file_count = (
    sellers_bronze_df
    .select("_source_file")
    .distinct()
    .count()
)

rescued_rows = (
    sellers_bronze_df
    .filter(F.col("_rescued_data").isNotNull())
    .count()
)

missing_metadata_rows = (
    sellers_bronze_df
    .filter(
        F.col("_source_file").isNull()
        | (F.col("_source_file") == "")
        | F.col("_batch_id").isNull()
        | (F.col("_batch_id") == "")
        | F.col("_load_date").isNull()
        | F.col("_ingest_ts").isNull()
    )
    .count()
)

invalid_registry_states = (
    spark.table(registry_table)
    .filter(
        (F.col("dataset") == dataset)
        & (F.col("batch_status") != "BRONZE_WRITTEN")
    )
    .count()
)

registry_statuses = [
    row["batch_status"]
    for row in (
        spark.table(registry_table)
        .filter(F.col("dataset") == dataset)
        .select("batch_status")
        .distinct()
        .collect()
    )
]


# ---------------------------------------------------------
# 5. Assertions
# ---------------------------------------------------------

assert bronze_rows == 100, (
    f"Expected 100 seller rows, found {bronze_rows}"
)

assert source_file_count == 1, (
    f"Expected 1 source file, found {source_file_count}"
)

assert rescued_rows == 0, (
    f"Expected 0 rescued rows, found {rescued_rows}"
)

assert missing_metadata_rows == 0, (
    f"Rows with missing metadata: {missing_metadata_rows}"
)

assert invalid_registry_states == 0, (
    f"Registry rows outside BRONZE_WRITTEN: "
    f"{invalid_registry_states}"
)


# ---------------------------------------------------------
# 6. Итог
# ---------------------------------------------------------

print("Sellers Auto Loader completed")
print(f"Bronze rows:             {bronze_rows}")
print(f"Source files:            {source_file_count}")
print(f"Rescued rows:            {rescued_rows}")
print(f"Missing metadata:        {missing_metadata_rows}")
print(f"Registry status:         {registry_statuses}")
print(f"Target table:            {target_table}")
print(f"Checkpoint location:     {checkpoint_path}")
print(f"Schema location:         {schema_path}")

display(
    sellers_bronze_df
    .groupBy(
        "_load_date",
        "_batch_id"
    )
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("_source_file").alias(
            "source_file_count"
        ),
        F.sum(
            F.when(
                F.col("_rescued_data").isNotNull(),
                1
            ).otherwise(0)
        ).alias("rescued_row_count")
    )
    .orderBy("_load_date", "_batch_id")
)

# COMMAND ----------

# COMMAND ----------
# Сводная проверка всех Bronze-таблиц и batch_registry

from pyspark.sql import functions as F

registry_table = "marketplace_lab.ops.batch_registry"

bronze_targets = {
    "user_events": {
        "table": "marketplace_lab.bronze.user_events_raw",
        "expected_rows": 50400,
        "expected_registry_rows": 1
    },
    "orders": {
        "table": "marketplace_lab.bronze.orders_raw",
        "expected_rows": 10500,
        "expected_registry_rows": 2
    },
    "payments": {
        "table": "marketplace_lab.bronze.payments_raw",
        "expected_rows": 10080,
        "expected_registry_rows": 1
    },
    "products": {
        "table": "marketplace_lab.bronze.products_raw",
        "expected_rows": 2000,
        "expected_registry_rows": 1
    },
    "sellers": {
        "table": "marketplace_lab.bronze.sellers_raw",
        "expected_rows": 100,
        "expected_registry_rows": 1
    }
}

validation_results = []


# ---------------------------------------------------------
# 1. Проверка Bronze-таблиц
# ---------------------------------------------------------

for dataset, config in bronze_targets.items():
    table_name = config["table"]
    expected_rows = config["expected_rows"]

    bronze_df = spark.table(table_name)

    actual_rows = bronze_df.count()

    source_file_count = (
        bronze_df
        .select("_source_file")
        .distinct()
        .count()
    )

    batch_count = (
        bronze_df
        .select("_batch_id")
        .distinct()
        .count()
    )

    rescued_rows = (
        bronze_df
        .filter(F.col("_rescued_data").isNotNull())
        .count()
    )

    missing_metadata_rows = (
        bronze_df
        .filter(
            F.col("_source_file").isNull()
            | (F.col("_source_file") == "")
            | F.col("_batch_id").isNull()
            | (F.col("_batch_id") == "")
            | F.col("_load_date").isNull()
            | F.col("_ingest_ts").isNull()
        )
        .count()
    )

    row_count_status = (
        "PASSED"
        if actual_rows == expected_rows
        else "FAILED"
    )

    metadata_status = (
        "PASSED"
        if missing_metadata_rows == 0
        else "FAILED"
    )

    validation_results.append({
        "dataset": dataset,
        "bronze_table": table_name,
        "expected_rows": expected_rows,
        "actual_rows": actual_rows,
        "source_file_count": source_file_count,
        "batch_count": batch_count,
        "rescued_rows": rescued_rows,
        "missing_metadata_rows": missing_metadata_rows,
        "row_count_status": row_count_status,
        "metadata_status": metadata_status
    })


validation_df = (
    spark.createDataFrame(validation_results)
    .select(
        "dataset",
        "bronze_table",
        "expected_rows",
        "actual_rows",
        "source_file_count",
        "batch_count",
        "rescued_rows",
        "missing_metadata_rows",
        "row_count_status",
        "metadata_status"
    )
    .orderBy("dataset")
)


# ---------------------------------------------------------
# 2. Проверка реестра batch_registry
# ---------------------------------------------------------

registry_summary_df = (
    spark.table(registry_table)
    .groupBy("dataset")
    .agg(
        F.count("*").alias("registry_row_count"),
        F.countDistinct("batch_id").alias(
            "registered_batch_count"
        ),
        F.collect_set("batch_status").alias(
            "registry_statuses"
        ),
        F.collect_set("bronze_table").alias(
            "registered_bronze_tables"
        ),
        F.sum(
            F.when(
                F.col("batch_status") != "BRONZE_WRITTEN",
                1
            ).otherwise(0)
        ).alias("invalid_status_count"),
        F.sum(
            F.when(
                F.col("bronze_table").isNull(),
                1
            ).otherwise(0)
        ).alias("missing_bronze_table_count")
    )
    .orderBy("dataset")
)


# ---------------------------------------------------------
# 3. Assertions для Bronze
# ---------------------------------------------------------

failed_row_counts = [
    row
    for row in validation_results
    if row["actual_rows"] != row["expected_rows"]
]

missing_metadata = [
    row
    for row in validation_results
    if row["missing_metadata_rows"] != 0
]

assert len(failed_row_counts) == 0, (
    f"Bronze row-count validation failed: "
    f"{failed_row_counts}"
)

assert len(missing_metadata) == 0, (
    f"Missing lineage metadata detected: "
    f"{missing_metadata}"
)


# ---------------------------------------------------------
# 4. Assertions для batch_registry
# ---------------------------------------------------------

registry_df = spark.table(registry_table)

expected_total_registry_rows = sum(
    item["expected_registry_rows"]
    for item in bronze_targets.values()
)

actual_total_registry_rows = (
    registry_df
    .filter(
        F.col("dataset").isin(
            list(bronze_targets.keys())
        )
    )
    .count()
)

invalid_registry_rows = (
    registry_df
    .filter(
        F.col("dataset").isin(
            list(bronze_targets.keys())
        )
        & (
            (F.col("batch_status") != "BRONZE_WRITTEN")
            | F.col("bronze_table").isNull()
            | F.col("processed_at").isNull()
        )
    )
    .count()
)

assert actual_total_registry_rows == expected_total_registry_rows, (
    f"Expected {expected_total_registry_rows} registry rows, "
    f"found {actual_total_registry_rows}"
)

assert invalid_registry_rows == 0, (
    f"Invalid batch_registry rows: {invalid_registry_rows}"
)


# ---------------------------------------------------------
# 5. Общий результат
# ---------------------------------------------------------

total_bronze_rows = sum(
    row["actual_rows"]
    for row in validation_results
)

total_rescued_rows = sum(
    row["rescued_rows"]
    for row in validation_results
)

print("Bronze consolidated validation passed")
print(f"Bronze datasets:          {len(bronze_targets)}")
print(f"Bronze tables:            {len(bronze_targets)}")
print(f"Registered batches:       {actual_total_registry_rows}")
print(f"Total Bronze rows:        {total_bronze_rows}")
print(f"Total rescued rows:       {total_rescued_rows}")
print(f"Invalid registry rows:    {invalid_registry_rows}")
print(f"Missing lineage metadata: 0")

display(validation_df)
display(registry_summary_df)

# COMMAND ----------

# COMMAND ----------
# Автономная проверка идемпотентности Auto Loader
#
# Не зависит от Python-переменных из предыдущих ячеек.
# Использует существующие Bronze-схемы и checkpoints.

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ---------------------------------------------------------
# 1. Конфигурация всех источников
# ---------------------------------------------------------

idempotency_config = {
    "user_events": {
        "format": "json",
        "extension": "*.json",
        "source_path": (
            "/Volumes/marketplace_lab/bronze/"
            "landing/user_events"
        ),
        "target_table": (
            "marketplace_lab.bronze.user_events_raw"
        ),
        "checkpoint_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/checkpoints/user_events"
        ),
        "schema_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/schemas/user_events"
        )
    },
    "orders": {
        "format": "json",
        "extension": "*.json",
        "source_path": (
            "/Volumes/marketplace_lab/bronze/"
            "landing/orders"
        ),
        "target_table": (
            "marketplace_lab.bronze.orders_raw"
        ),
        "checkpoint_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/checkpoints/orders"
        ),
        "schema_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/schemas/orders"
        )
    },
    "payments": {
        "format": "csv",
        "extension": "*.csv",
        "source_path": (
            "/Volumes/marketplace_lab/bronze/"
            "landing/payments"
        ),
        "target_table": (
            "marketplace_lab.bronze.payments_raw"
        ),
        "checkpoint_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/checkpoints/payments"
        ),
        "schema_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/schemas/payments"
        )
    },
    "products": {
        "format": "parquet",
        "extension": "*.parquet",
        "source_path": (
            "/Volumes/marketplace_lab/bronze/"
            "landing/products"
        ),
        "target_table": (
            "marketplace_lab.bronze.products_raw"
        ),
        "checkpoint_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/checkpoints/products"
        ),
        "schema_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/schemas/products"
        )
    },
    "sellers": {
        "format": "csv",
        "extension": "*.csv",
        "source_path": (
            "/Volumes/marketplace_lab/bronze/"
            "landing/sellers"
        ),
        "target_table": (
            "marketplace_lab.bronze.sellers_raw"
        ),
        "checkpoint_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/checkpoints/sellers"
        ),
        "schema_path": (
            "/Volumes/marketplace_lab/ops/checkpoints/"
            "_autoloader/schemas/sellers"
        )
    }
}


technical_columns = {
    "_ingest_ts",
    "_source_file",
    "_batch_id",
    "_load_date",
    "_rescued_data",
    "_auto_rescued_data",
    "_auto_corrupt_record"
}


# ---------------------------------------------------------
# 2. Сохраняем количество строк до повторного запуска
# ---------------------------------------------------------

before_counts = {
    dataset: spark.table(
        config["target_table"]
    ).count()
    for dataset, config in idempotency_config.items()
}

print("Rows before repeated run:")

for dataset, row_count in before_counts.items():
    print(f"{dataset}: {row_count}")


# ---------------------------------------------------------
# 3. Функция создания повторного Auto Loader stream
# ---------------------------------------------------------

def build_repeat_stream(config):
    target_df = spark.table(config["target_table"])
    target_schema = target_df.schema

    # Оставляем только исходные бизнес-поля.
    source_fields = [
        field
        for field in target_schema.fields
        if field.name not in technical_columns
    ]

    # Для JSON и CSV добавляем временное поле,
    # в которое Spark помещает повреждённую запись.
    if config["format"] in ("json", "csv"):
        source_fields.append(
            StructField(
                "_auto_corrupt_record",
                StringType(),
                True
            )
        )

    source_schema = StructType(source_fields)

    reader = (
        spark.readStream
        .format("cloudFiles")
        .schema(source_schema)
        .option(
            "cloudFiles.format",
            config["format"]
        )
        .option(
            "cloudFiles.schemaLocation",
            config["schema_path"]
        )
        .option(
            "cloudFiles.schemaEvolutionMode",
            "rescue"
        )
        .option(
            "cloudFiles.includeExistingFiles",
            "true"
        )
        .option(
            "rescuedDataColumn",
            "_auto_rescued_data"
        )
        .option(
            "pathGlobFilter",
            config["extension"]
        )
    )

    if config["format"] == "csv":
        reader = (
            reader
            .option("header", "true")
            .option("delimiter", ",")
            .option("mode", "PERMISSIVE")
            .option(
                "columnNameOfCorruptRecord",
                "_auto_corrupt_record"
            )
        )

    if config["format"] == "json":
        reader = (
            reader
            .option("mode", "PERMISSIVE")
            .option(
                "columnNameOfCorruptRecord",
                "_auto_corrupt_record"
            )
        )

    stream_df = reader.load(config["source_path"])

    # Для Parquet временного corrupt-поля нет.
    if "_auto_corrupt_record" in stream_df.columns:
        rescued_expression = F.coalesce(
            F.col("_auto_rescued_data"),
            F.col("_auto_corrupt_record")
        )
    else:
        rescued_expression = F.col(
            "_auto_rescued_data"
        )

    stream_df = (
        stream_df
        .withColumn(
            "_source_file",
            F.col("_metadata.file_path")
        )
        .withColumn(
            "_batch_id",
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"batch_id=([^/]+)",
                1
            )
        )
        .withColumn(
            "_load_date",
            F.to_date(
                F.regexp_extract(
                    F.col("_metadata.file_path"),
                    r"load_date=([^/]+)",
                    1
                )
            )
        )
        .withColumn(
            "_ingest_ts",
            F.current_timestamp()
        )
        .withColumn(
            "_rescued_data",
            rescued_expression
        )
        .drop(
            "_auto_rescued_data",
            "_auto_corrupt_record"
        )
    )

    # Восстанавливаем точный порядок колонок целевой таблицы.
    target_columns = target_schema.fieldNames()

    return stream_df.select(
        *[F.col(column) for column in target_columns]
    )


# ---------------------------------------------------------
# 4. Повторный запуск с существующими checkpoints
# ---------------------------------------------------------

for dataset, config in idempotency_config.items():
    print(f"Restarting Auto Loader: {dataset}")

    repeat_stream_df = build_repeat_stream(config)

    repeat_query = (
        repeat_stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            config["checkpoint_path"]
        )
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(config["target_table"])
    )

    repeat_query.awaitTermination()

    print(f"Completed: {dataset}")


# ---------------------------------------------------------
# 5. Сравнение результатов
# ---------------------------------------------------------

idempotency_results = []

for dataset, config in idempotency_config.items():
    rows_before = before_counts[dataset]

    rows_after = spark.table(
        config["target_table"]
    ).count()

    added_rows = rows_after - rows_before

    idempotency_results.append({
        "dataset": dataset,
        "bronze_table": config["target_table"],
        "rows_before": rows_before,
        "rows_after": rows_after,
        "added_rows": added_rows,
        "validation_status": (
            "PASSED"
            if added_rows == 0
            else "FAILED"
        )
    })


idempotency_df = (
    spark.createDataFrame(idempotency_results)
    .select(
        "dataset",
        "bronze_table",
        "rows_before",
        "rows_after",
        "added_rows",
        "validation_status"
    )
    .orderBy("dataset")
)


# ---------------------------------------------------------
# 6. Финальные assertions
# ---------------------------------------------------------

failed_datasets = [
    result
    for result in idempotency_results
    if result["added_rows"] != 0
]

assert len(failed_datasets) == 0, (
    "Auto Loader idempotency validation failed: "
    f"{failed_datasets}"
)

total_rows_before = sum(
    result["rows_before"]
    for result in idempotency_results
)

total_rows_after = sum(
    result["rows_after"]
    for result in idempotency_results
)

total_added_rows = sum(
    result["added_rows"]
    for result in idempotency_results
)


print()
print("Auto Loader idempotency validation passed")
print(f"Validated streams:       {len(idempotency_results)}")
print(f"Rows before rerun:       {total_rows_before}")
print(f"Rows after rerun:        {total_rows_after}")
print(f"Rows added on rerun:     {total_added_rows}")
print("Checkpoints reused:      5")
print("Duplicate ingestion:     0")

display(idempotency_df)