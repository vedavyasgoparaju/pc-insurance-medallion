# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # PC Insurance Swarm Toolkit Functions Registration
# MAGIC
# MAGIC Registers 7 UC functions in `pc_insurance_dev.metadata`:
# MAGIC
# MAGIC 1-2: SQL functions (persistent)
# MAGIC 3-7: Python UDFs (session-scoped, must re-run)

# COMMAND ----------

# DBTITLE 1,Set Catalog & Schema
# MAGIC %sql
# MAGIC USE CATALOG pc_insurance_dev;
# MAGIC USE SCHEMA metadata;

# COMMAND ----------

# DBTITLE 1,Function 1: read_mapping_document
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION read_mapping_document(x_center STRING, layer STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Retrieves active mapping JSON'
# MAGIC RETURN (
# MAGIC   SELECT mapping_json
# MAGIC   FROM mapping_documents
# MAGIC   WHERE x_center = read_mapping_document.x_center
# MAGIC     AND layer = read_mapping_document.layer
# MAGIC     AND is_active = true
# MAGIC   ORDER BY version DESC LIMIT 1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Function 2: get_pipeline_error_log
# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION get_pipeline_error_log(run_id STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Retrieves lineage from system.access.table_lineage'
# MAGIC RETURN (
# MAGIC   SELECT CONCAT('{"run_id": "', run_id, '", "source": "',
# MAGIC     COALESCE(FIRST(source_table_full_name), ''), '", "target": "',
# MAGIC     COALESCE(FIRST(target_table_full_name), '"}')
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE entity_run_id = run_id LIMIT 1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Import & Setup Python UDFs
from pyspark.sql.types import StringType
import json

print("Registering Python UDFs...")

# COMMAND ----------

# DBTITLE 1,Functions 3-7: Python UDFs
# 3. update_mapping_document
def f3(x_center, layer, payload_json):
    update = json.loads(payload_json)
    mapping = json.dumps({"column_mappings": update.get("column_mappings", {}), "transformation_rules": update.get("transformation_rules", [])})
    return json.dumps({"x_center": x_center, "layer": layer, "new_mapping_json": mapping, "sql": [f"UPDATE mapping_documents SET is_active=false WHERE x_center='{x_center}' AND layer='{layer}' AND is_active=true"], "status": "plan"})
spark.udf.register("update_mapping_document", f3, StringType())

# 4. execute_sandbox_metadata_run
def f4(x_center, staging_catalog):
    return json.dumps({"staging_catalog": staging_catalog, "sql": [f"CREATE CATALOG IF NOT EXISTS {staging_catalog}", f"CREATE SCHEMA IF NOT EXISTS {staging_catalog}.metadata"], "status": "plan"})
spark.udf.register("execute_sandbox_metadata_run", f4, StringType())

# 5. update_uc_catalog_comments
def f5(table_name, comments_json):
    comments = json.loads(comments_json)
    sql = [f"COMMENT ON COLUMN {table_name}.{col} IS '{comm.replace(chr(39), chr(39)+chr(39))}'" for col, comm in comments.items()]
    return json.dumps({"table_name": table_name, "sql": sql, "status": "plan"})
spark.udf.register("update_uc_catalog_comments", f5, StringType())

# 6. write_technical_markdown_doc
def f6(file_path, content):
    return json.dumps({"file_path": file_path, "content_length": len(content), "dbutils_cmd": f'dbutils.fs.put("{file_path}", content, overwrite=True)', "status": "plan"})
spark.udf.register("write_technical_markdown_doc", f6, StringType())

# 7. trigger_pipeline_repair
def f7(run_id):
    return json.dumps({"run_id": run_id, "sdk_cmd": f'w.jobs.repair_run(run_id={run_id}, rerun_all_failed_tasks=True)', "status": "plan"})
spark.udf.register("trigger_pipeline_repair", f7, StringType())

print("All 7 functions registered!")

# COMMAND ----------

