# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # PC Insurance Swarm Toolkit Functions Registration
# MAGIC
# MAGIC Registers 7 persistent UC SQL functions in `pc_insurance_dev.metadata`.
# MAGIC All functions are SQL-based (persistent in Unity Catalog) and return JSON execution plans.

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
# MAGIC   SELECT CONCAT(
# MAGIC     '{"run_id": "', run_id, '",',
# MAGIC     '"source": "', COALESCE(MAX(source_table_full_name), ''), '",',
# MAGIC     '"target": "', COALESCE(MAX(target_table_full_name), ''), '"}'
# MAGIC   )
# MAGIC   FROM system.access.table_lineage
# MAGIC   WHERE entity_run_id = run_id
# MAGIC   LIMIT 1
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Function 3: update_mapping_document
# MAGIC %sql
# MAGIC -- Function 3: update_mapping_document
# MAGIC CREATE OR REPLACE FUNCTION update_mapping_document(x_center STRING, layer STRING, update_payload STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Returns SQL execution plan for updating mapping metadata.'
# MAGIC RETURN to_json(named_struct(
# MAGIC   'x_center', x_center,
# MAGIC   'layer', layer,
# MAGIC   'new_mapping_json', COALESCE(get_json_object(update_payload, '$.column_mappings'), '{}'),
# MAGIC   'sql_to_execute', array(concat(
# MAGIC     'UPDATE pc_insurance_dev.metadata.mapping_documents SET is_active=false WHERE x_center=', quote(x_center), ' AND layer=', quote(layer), ' AND is_active=true'
# MAGIC   )),
# MAGIC   'status', 'merge_plan_generated'
# MAGIC ));

# COMMAND ----------

# DBTITLE 1,Functions 4-7: SQL Functions
# MAGIC %sql
# MAGIC -- Function 4: execute_sandbox_metadata_run
# MAGIC CREATE OR REPLACE FUNCTION execute_sandbox_metadata_run(x_center STRING, sandbox_catalog STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Returns SQL plan for sandbox catalog creation and mapping clone.'
# MAGIC RETURN to_json(named_struct(
# MAGIC   'sandbox_catalog', sandbox_catalog,
# MAGIC   'x_center', x_center,
# MAGIC   'sql_statements', array(
# MAGIC     concat('CREATE CATALOG IF NOT EXISTS ', sandbox_catalog),
# MAGIC     concat('CREATE SCHEMA IF NOT EXISTS ', sandbox_catalog, '.metadata'),
# MAGIC     concat('CREATE TABLE IF NOT EXISTS ', sandbox_catalog, '.metadata.mapping_documents AS SELECT * FROM pc_insurance_dev.metadata.mapping_documents WHERE x_center=', quote(x_center), ' AND is_active=true')
# MAGIC   ),
# MAGIC   'status', 'sandbox_preparation_plan'
# MAGIC ));
# MAGIC
# MAGIC -- Function 5: update_uc_catalog_comments
# MAGIC CREATE OR REPLACE FUNCTION update_uc_catalog_comments(table_name STRING, column_comments STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Returns SQL plan for updating UC catalog comments.'
# MAGIC RETURN to_json(named_struct(
# MAGIC   'table_name', table_name,
# MAGIC   'sql_statements', array(concat('COMMENT ON COLUMN ', table_name, '.* IS updated_by_swarm_agent')),
# MAGIC   'status', 'comment_update_plan'
# MAGIC ));
# MAGIC
# MAGIC -- Function 6: write_technical_markdown_doc
# MAGIC CREATE OR REPLACE FUNCTION write_technical_markdown_doc(file_path STRING, content STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Returns execution plan for writing technical documentation to UC volume or workspace.'
# MAGIC RETURN to_json(named_struct(
# MAGIC   'file_path', file_path,
# MAGIC   'content_length', length(content),
# MAGIC   'dbutils_command', concat('dbutils.fs.put(', quote(file_path), ', content, overwrite=True)'),
# MAGIC   'status', CASE WHEN file_path LIKE '/Volumes/%' OR file_path LIKE '/Workspace/%' THEN 'write_plan_ready' ELSE 'error' END,
# MAGIC   'message', CASE WHEN file_path LIKE '/Volumes/%' OR file_path LIKE '/Workspace/%' THEN '' ELSE 'Path must start with /Volumes/ or /Workspace/' END
# MAGIC ));
# MAGIC
# MAGIC -- Function 7: trigger_pipeline_repair
# MAGIC CREATE OR REPLACE FUNCTION trigger_pipeline_repair(run_id STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'UC Toolkit: Returns REST API call plan for triggering pipeline repair.'
# MAGIC RETURN to_json(named_struct(
# MAGIC   'run_id', run_id,
# MAGIC   'api_endpoint', '/api/2.1/jobs/repair-run',
# MAGIC   'payload', to_json(named_struct(
# MAGIC     'run_id', CAST(run_id AS LONG),
# MAGIC     'rerun_all_failed_tasks', true,
# MAGIC     'rerun_dependent_tasks', true
# MAGIC   )),
# MAGIC   'sdk_command', concat('w.jobs.repair_run(run_id=', run_id, ', rerun_all_failed_tasks=True, rerun_dependent_tasks=True)'),
# MAGIC   'status', 'repair_plan_ready'
# MAGIC ));
# MAGIC
# MAGIC SELECT 'All 7 SQL functions registered successfully' AS status;

# COMMAND ----------

# DBTITLE 1,Verify All Functions
# MAGIC %sql
# MAGIC -- Verify all 7 functions are registered
# MAGIC SHOW USER FUNCTIONS IN pc_insurance_dev.metadata;