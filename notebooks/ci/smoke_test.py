# Databricks notebook source
# MAGIC %md
# MAGIC # Marketplace Lakehouse — deployment smoke test
# MAGIC
# MAGIC Validates that the deployed identity can resolve the required catalog,
# MAGIC schemas, and representative Bronze/Silver/Gold/Ops tables.

# COMMAND ----------

from pyspark.sql import functions as F


def ensure_widget(name, default=""):
    try:
        dbutils.widgets.get(name)
    except Exception:
        dbutils.widgets.text(name, default)


ensure_widget("catalog_name", "marketplace_lab")
ensure_widget("environment", "dev")

CATALOG = (dbutils.widgets.get("catalog_name") or "marketplace_lab").strip()
ENVIRONMENT = (dbutils.widgets.get("environment") or "dev").strip().lower()

assert ENVIRONMENT in {"dev", "test", "prod"}
assert CATALOG

required_schemas = ["bronze", "silver", "gold", "ops", "quarantine"]
required_tables = [
    f"{CATALOG}.bronze.orders_raw",
    f"{CATALOG}.silver.orders",
    f"{CATALOG}.gold.fact_orders",
    f"{CATALOG}.ops.pipeline_runs",
]

results = []
for schema_name in required_schemas:
    full_name = f"{CATALOG}.{schema_name}"
    results.append(("SCHEMA", full_name, spark.catalog.databaseExists(full_name)))

for table_name in required_tables:
    results.append(("TABLE", table_name, spark.catalog.tableExists(table_name)))

results_df = spark.createDataFrame(results, "object_type string, object_name string, exists boolean")
display(results_df.orderBy("object_type", "object_name"))

missing = [name for _, name, exists in results if not exists]
assert not missing, f"Smoke test failed; missing objects: {missing}"

identity = spark.sql("SELECT current_user() AS identity").first().identity
print("Marketplace deployment smoke test passed")
print(f"Environment: {ENVIRONMENT}")
print(f"Catalog: {CATALOG}")
print(f"Execution identity: {identity}")
print(f"Validated objects: {len(results)}")
