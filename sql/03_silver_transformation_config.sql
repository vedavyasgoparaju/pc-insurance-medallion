-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer Metadata Configuration
-- MAGIC 
-- MAGIC This script creates the metadata tables required for metadata-driven Silver layer transformations:
-- MAGIC - `silver_transformation_config`: Configuration for each Silver transformation
-- MAGIC - `silver_load_audit`: Audit log for Silver transformation runs
-- MAGIC - `silver_reconciliation`: Reconciliation of source vs target counts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create silver_transformation_config Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pc_insurance.reference.silver_transformation_config (
  transformation_id STRING COMMENT 'Unique identifier for the transformation',
  transformation_name STRING COMMENT 'Human-readable name',
  source_schema STRING COMMENT 'Source schema (e.g., bronze)',
  source_table STRING COMMENT 'Source table name',
  target_schema STRING COMMENT 'Target schema (e.g., silver)',
  target_table STRING COMMENT 'Target table name',
  transformation_type STRING COMMENT 'Type: FULL_LOAD, INCREMENTAL, SCD2',
  business_keys ARRAY<STRING> COMMENT 'Business key columns for deduplication/SCD2',
  partition_columns ARRAY<STRING> COMMENT 'Partition columns',
  column_mappings MAP<STRING, STRING> COMMENT 'Source to target column mappings',
  cleansing_rules MAP<STRING, STRING> COMMENT 'Data cleansing rules (column -> rule)',
  dedup_strategy STRING COMMENT 'Deduplication strategy: LATEST, FIRST, CUSTOM',
  dedup_order_column STRING COMMENT 'Column to order by for deduplication',
  scd2_enabled BOOLEAN COMMENT 'Enable SCD Type 2 tracking',
  execution_order INT COMMENT 'Order of execution (lower runs first)',
  is_active BOOLEAN COMMENT 'Whether this transformation is active',
  created_at TIMESTAMP COMMENT 'Record creation timestamp',
  updated_at TIMESTAMP COMMENT 'Record update timestamp',
  created_by STRING COMMENT 'User who created the record',
  notes STRING COMMENT 'Additional notes'
)
USING DELTA
LOCATION 'dbfs:/mnt/pc_insurance/reference/silver_transformation_config'
COMMENT 'Metadata configuration for Silver layer transformations';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert Initial Configuration Records

-- COMMAND ----------

-- Clear existing data (for idempotency)
TRUNCATE TABLE pc_insurance.reference.silver_transformation_config;

-- Policy Dimension
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (
  'SILVER_POLICY_DIM',
  'Policy Dimension',
  'bronze',
  'policies_raw',
  'silver',
  'policy_dim',
  'SCD2',
  ARRAY('policy_id'),
  ARRAY('state', 'line_of_business'),
  MAP(
    'policy_id', 'policy_id',
    'policy_number', 'policy_number',
    'policy_status', 'policy_status',
    'policy_type', 'policy_type',
    'line_of_business', 'line_of_business',
    'customer_id', 'customer_id',
    'agent_id', 'agent_id',
    'effective_date', 'effective_date',
    'expiry_date', 'expiry_date',
    'premium_amount', 'premium_amount',
    'coverage_limit', 'coverage_limit',
    'deductible', 'deductible',
    'state', 'state',
    'territory_code', 'territory_code'
  ),
  MAP(
    'policy_status', 'UPPER(TRIM(policy_status))',
    'state', 'UPPER(TRIM(state))',
    'premium_amount', 'COALESCE(premium_amount, 0)'
  ),
  'LATEST',
  'ingestion_timestamp',
  TRUE,
  1,
  TRUE,
  current_timestamp(),
  current_timestamp(),
  'system',
  'Policy dimension with SCD2 tracking'
);

-- Claim Dimension
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (
  'SILVER_CLAIM_DIM',
  'Claim Dimension',
  'bronze',
  'claims_raw',
  'silver',
  'claim_dim',
  'SCD2',
  ARRAY('claim_id'),
  ARRAY('claim_type'),
  MAP(
    'claim_id', 'claim_id',
    'claim_number', 'claim_number',
    'policy_id', 'policy_id',
    'customer_id', 'customer_id',
    'claim_type', 'claim_type',
    'claim_status', 'claim_status',
    'loss_date', 'loss_date',
    'report_date', 'report_date',
    'close_date', 'close_date',
    'adjuster_id', 'adjuster_id',
    'fraud_flag', 'fraud_flag',
    'litigation_flag', 'litigation_flag'
  ),
  MAP(
    'claim_status', 'UPPER(TRIM(claim_status))',
    'claim_type', 'UPPER(TRIM(claim_type))',
    'fraud_flag', 'COALESCE(fraud_flag, FALSE)',
    'litigation_flag', 'COALESCE(litigation_flag, FALSE)'
  ),
  'LATEST',
  'ingestion_timestamp',
  TRUE,
  2,
  TRUE,
  current_timestamp(),
  current_timestamp(),
  'system',
  'Claim dimension with SCD2 tracking'
);

-- Customer Dimension
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (
  'SILVER_CUSTOMER_DIM',
  'Customer Dimension',
  'bronze',
  'customers_raw',
  'silver',
  'customer_dim',
  'SCD2',
  ARRAY('customer_id'),
  ARRAY('state'),
  MAP(
    'customer_id', 'customer_id',
    'customer_name', 'customer_name_masked',
    'customer_type', 'customer_type',
    'date_of_birth', 'date_of_birth',
    'gender', 'gender',
    'city', 'city',
    'state', 'state',
    'zip_code', 'zip_code',
    'credit_score', 'credit_score',
    'occupation', 'occupation',
    'annual_income', 'annual_income',
    'years_with_company', 'years_with_company'
  ),
  MAP(
    'customer_name', 'CONCAT(LEFT(customer_name, 1), REPEAT(''*'', LENGTH(customer_name)-1))',
    'state', 'UPPER(TRIM(state))',
    'credit_score', 'COALESCE(credit_score, 0)'
  ),
  'LATEST',
  'ingestion_timestamp',
  TRUE,
  3,
  TRUE,
  current_timestamp(),
  current_timestamp(),
  'system',
  'Customer dimension with PII masking and SCD2'
);

-- Agent Dimension
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (
  'SILVER_AGENT_DIM',
  'Agent Dimension',
  'bronze',
  'agents_raw',
  'silver',
  'agent_dim',
  'SCD2',
  ARRAY('agent_id'),
  ARRAY('license_state'),
  MAP(
    'agent_id', 'agent_id',
    'agent_name', 'agent_name',
    'agency_name', 'agency_name',
    'agent_license_number', 'agent_license_number',
    'license_state', 'license_state',
    'agent_status', 'agent_status',
    'commission_rate', 'commission_rate',
    'appointment_date', 'appointment_date'
  ),
  MAP(
    'agent_status', 'UPPER(TRIM(agent_status))',
    'license_state', 'UPPER(TRIM(license_state))'
  ),
  'LATEST',
  'ingestion_timestamp',
  TRUE,
  4,
  TRUE,
  current_timestamp(),
  current_timestamp(),
  'system',
  'Agent dimension with SCD2 tracking'
);

-- Premium Fact
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (
  'SILVER_PREMIUM_FACT',
  'Premium Fact',
  'bronze',
  'premiums_raw',
  'silver',
  'premium_fact',
  'INCREMENTAL',
  ARRAY('transaction_id'),
  ARRAY('transaction_date'),
  MAP(
    'transaction_id', 'transaction_id',
    'policy_id', 'policy_id',
    'customer_id', 'customer_id',
    'transaction_type', 'transaction_type',
    'transaction_date', 'transaction_date',
    'premium_amount', 'premium_amount',
    'written_premium', 'written_premium',
    'earned_premium', 'earned_premium',
    'unearned_premium', 'unearned_premium',
    'commission_amount', 'commission_amount',
    'payment_frequency', 'payment_frequency',
    'billing_status', 'billing_status'
  ),
  MAP(
    'transaction_type', 'UPPER(TRIM(transaction_type))',
    'billing_status', 'UPPER(TRIM(billing_status))',
    'premium_amount', 'COALESCE(premium_amount, 0)',
    'commission_amount', 'COALESCE(commission_amount, 0)'
  ),
  'LATEST',
  'transaction_date',
  FALSE,
  5,
  TRUE,
  current_timestamp(),
  current_timestamp(),
  'system',
  'Premium fact table with incremental load'
);

-- Claim Fact
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (
  'SILVER_CLAIM_FACT',
  'Claim Fact',
  'bronze',
  'claims_raw',
  'silver',
  'claim_fact',
  'INCREMENTAL',
  ARRAY('claim_id', 'report_date'),
  ARRAY('loss_date'),
  MAP(
    'claim_id', 'claim_id',
    'policy_id', 'policy_id',
    'customer_id', 'customer_id',
    'loss_date', 'loss_date',
    'report_date', 'report_date',
    'close_date', 'close_date',
    'incurred_loss', 'incurred_loss',
    'paid_loss', 'paid_loss',
    'reserved_amount', 'reserved_amount',
    'expense_amount', 'expense_amount',
    'deductible_applied', 'deductible_applied',
    'subrogation_amount', 'subrogation_amount'
  ),
  MAP(
    'incurred_loss', 'COALESCE(incurred_loss, 0)',
    'paid_loss', 'COALESCE(paid_loss, 0)',
    'reserved_amount', 'COALESCE(reserved_amount, 0)',
    'expense_amount', 'COALESCE(expense_amount, 0)'
  ),
  'LATEST',
  'report_date',
  FALSE,
  6,
  TRUE,
  current_timestamp(),
  current_timestamp(),
  'system',
  'Claim fact table with incremental load'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create silver_load_audit Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pc_insurance.reference.silver_load_audit (
  audit_id STRING COMMENT 'Unique audit record ID',
  transformation_id STRING COMMENT 'FK to silver_transformation_config',
  run_timestamp TIMESTAMP COMMENT 'When the transformation ran',
  status STRING COMMENT 'SUCCESS, FAILED, RUNNING',
  source_row_count LONG COMMENT 'Number of rows read from source',
  target_row_count LONG COMMENT 'Number of rows written to target',
  inserted_count LONG COMMENT 'Number of rows inserted',
  updated_count LONG COMMENT 'Number of rows updated',
  deleted_count LONG COMMENT 'Number of rows deleted',
  error_message STRING COMMENT 'Error message if failed',
  execution_time_seconds DOUBLE COMMENT 'Execution time in seconds',
  run_by STRING COMMENT 'User or system that ran the transformation'
)
USING DELTA
PARTITIONED BY (DATE(run_timestamp))
LOCATION 'dbfs:/mnt/pc_insurance/reference/silver_load_audit'
COMMENT 'Audit log for Silver layer transformation runs';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create silver_reconciliation Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pc_insurance.reference.silver_reconciliation (
  recon_id STRING COMMENT 'Unique reconciliation record ID',
  transformation_id STRING COMMENT 'FK to silver_transformation_config',
  recon_timestamp TIMESTAMP COMMENT 'When reconciliation was performed',
  source_count LONG COMMENT 'Count from source table',
  target_count LONG COMMENT 'Count from target table',
  count_match BOOLEAN COMMENT 'Whether counts match',
  count_diff LONG COMMENT 'Difference (target - source)',
  recon_status STRING COMMENT 'PASS, FAIL, WARNING',
  notes STRING COMMENT 'Additional reconciliation notes'
)
USING DELTA
PARTITIONED BY (DATE(recon_timestamp))
LOCATION 'dbfs:/mnt/pc_insurance/reference/silver_reconciliation'
COMMENT 'Reconciliation results for Silver transformations';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Configuration

-- COMMAND ----------

SELECT 
  transformation_id,
  transformation_name,
  source_table,
  target_table,
  transformation_type,
  execution_order,
  is_active
FROM pc_insurance.reference.silver_transformation_config
ORDER BY execution_order;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration Complete

-- COMMAND ----------

SELECT 'Silver metadata configuration complete' AS status,
       COUNT(*) AS active_transformations
FROM pc_insurance.reference.silver_transformation_config
WHERE is_active = TRUE;
