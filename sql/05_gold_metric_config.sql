-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer Metadata Configuration
-- MAGIC 
-- MAGIC This script creates the metadata tables required for metadata-driven Gold layer KPI generation:
-- MAGIC - `gold_metric_config`: Configuration for each Gold KPI metric
-- MAGIC - `gold_refresh_audit`: Audit log for Gold metric refresh runs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create gold_metric_config Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pc_insurance.reference.gold_metric_config (
  metric_id STRING COMMENT 'Unique identifier for the metric',
  metric_name STRING COMMENT 'Human-readable metric name',
  metric_category STRING COMMENT 'Category: LOSS_RATIO, FREQUENCY_SEVERITY, RETENTION, GROWTH, EXPOSURE, DASHBOARD',
  target_schema STRING COMMENT 'Target schema (e.g., gold)',
  target_table STRING COMMENT 'Target table name',
  source_tables ARRAY<STRING> COMMENT 'Source tables required for this metric',
  dimension_columns ARRAY<STRING> COMMENT 'Dimension columns for grouping',
  measure_columns ARRAY<STRING> COMMENT 'Measure columns to calculate',
  calculation_logic STRING COMMENT 'SQL expression or formula for the metric',
  aggregation_grain STRING COMMENT 'Grain of aggregation (e.g., LOB, STATE, AGENT, MONTH)',
  refresh_frequency STRING COMMENT 'DAILY, WEEKLY, MONTHLY, ON_DEMAND',
  execution_order INT COMMENT 'Order of execution (lower runs first)',
  is_active BOOLEAN COMMENT 'Whether this metric is active',
  owner STRING COMMENT 'Business owner of the metric',
  created_at TIMESTAMP COMMENT 'Record creation timestamp',
  updated_at TIMESTAMP COMMENT 'Record update timestamp',
  notes STRING COMMENT 'Additional notes and business context'
)
USING DELTA
LOCATION 'dbfs:/mnt/pc_insurance/reference/gold_metric_config'
COMMENT 'Metadata configuration for Gold layer KPI metrics';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert Initial Metric Configuration

-- COMMAND ----------

-- Clear existing data (for idempotency)
TRUNCATE TABLE pc_insurance.reference.gold_metric_config;

-- Loss Ratio by Line of Business
INSERT INTO pc_insurance.reference.gold_metric_config VALUES (
  'GOLD_LOSS_RATIO_LOB',
  'Loss Ratio by Line of Business',
  'LOSS_RATIO',
  'gold',
  'loss_ratio_by_lob',
  ARRAY('silver.policy_dim', 'silver.claim_fact'),
  ARRAY('line_of_business'),
  ARRAY('total_incurred_loss', 'total_earned_premium', 'loss_ratio', 'claim_count', 'avg_loss_per_claim'),
  'SUM(incurred_loss) / SUM(earned_premium) AS loss_ratio',
  'LINE_OF_BUSINESS',
  'DAILY',
  1,
  TRUE,
  'Underwriting',
  current_timestamp(),
  current_timestamp(),
  'Loss ratio calculated as incurred loss divided by earned premium, grouped by line of business'
);

-- Claim Frequency and Severity by State
INSERT INTO pc_insurance.reference.gold_metric_config VALUES (
  'GOLD_FREQ_SEV_STATE',
  'Claim Frequency and Severity by State',
  'FREQUENCY_SEVERITY',
  'gold',
  'claim_frequency_severity',
  ARRAY('silver.policy_dim', 'silver.claim_fact'),
  ARRAY('state'),
  ARRAY('policy_count', 'claim_count', 'claim_frequency', 'total_incurred_loss', 'avg_severity', 'pure_premium'),
  'COUNT(DISTINCT claim_id) / COUNT(DISTINCT policy_id) AS claim_frequency, AVG(incurred_loss) AS avg_severity',
  'STATE',
  'DAILY',
  2,
  TRUE,
  'Actuarial',
  current_timestamp(),
  current_timestamp(),
  'Claim frequency (claims per policy) and severity (average loss per claim) by state'
);

-- Retention Rate by Agent
INSERT INTO pc_insurance.reference.gold_metric_config VALUES (
  'GOLD_RETENTION_AGENT',
  'Retention Rate by Agent',
  'RETENTION',
  'gold',
  'retention_by_agent',
  ARRAY('silver.policy_dim', 'silver.agent_dim'),
  ARRAY('agent_id', 'agent_name'),
  ARRAY('total_policies', 'renewed_policies', 'retention_rate', 'total_premium', 'avg_premium_per_policy'),
  'SUM(CASE WHEN policy_status = ''RENEWED'' THEN 1 ELSE 0 END) / COUNT(*) AS retention_rate',
  'AGENT',
  'MONTHLY',
  3,
  TRUE,
  'Sales',
  current_timestamp(),
  current_timestamp(),
  'Policy retention rate calculated as renewed policies divided by total policies, by agent'
);

-- Premium Growth Trends
INSERT INTO pc_insurance.reference.gold_metric_config VALUES (
  'GOLD_PREMIUM_GROWTH',
  'Premium Growth Trends',
  'GROWTH',
  'gold',
  'premium_growth',
  ARRAY('silver.premium_fact'),
  ARRAY('year_month'),
  ARRAY('written_premium', 'earned_premium', 'policy_count', 'avg_premium_per_policy', 'mom_growth_rate', 'yoy_growth_rate'),
  'LAG(SUM(written_premium), 1) OVER (ORDER BY year_month) AS prev_month_premium',
  'MONTH',
  'MONTHLY',
  4,
  TRUE,
  'Finance',
  current_timestamp(),
  current_timestamp(),
  'Month-over-month and year-over-year premium growth trends'
);

-- Exposure Summary by State and LOB
INSERT INTO pc_insurance.reference.gold_metric_config VALUES (
  'GOLD_EXPOSURE_SUMMARY',
  'Exposure Summary by State and LOB',
  'EXPOSURE',
  'gold',
  'exposure_summary',
  ARRAY('silver.policy_dim'),
  ARRAY('state', 'line_of_business'),
  ARRAY('policy_count', 'total_coverage_limit', 'total_premium', 'avg_coverage_per_policy', 'avg_premium_per_policy'),
  'SUM(coverage_limit) AS total_coverage_limit, SUM(premium_amount) AS total_premium',
  'STATE_LOB',
  'DAILY',
  5,
  TRUE,
  'Risk Management',
  current_timestamp(),
  current_timestamp(),
  'Total exposure (coverage limits) and premium by state and line of business'
);

-- Underwriting Dashboard Summary
INSERT INTO pc_insurance.reference.gold_metric_config VALUES (
  'GOLD_UW_DASHBOARD',
  'Underwriting Dashboard Summary',
  'DASHBOARD',
  'gold',
  'uw_dashboard_summary',
  ARRAY('silver.policy_dim', 'silver.claim_fact', 'silver.premium_fact'),
  ARRAY('line_of_business', 'state'),
  ARRAY('policy_count', 'total_premium', 'total_incurred_loss', 'loss_ratio', 'claim_count', 'claim_frequency', 'avg_severity'),
  'Comprehensive underwriting metrics combining policies, premiums, and claims',
  'LOB_STATE',
  'DAILY',
  6,
  TRUE,
  'Underwriting',
  current_timestamp(),
  current_timestamp(),
  'Executive dashboard with key underwriting metrics by LOB and state'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create gold_refresh_audit Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pc_insurance.reference.gold_refresh_audit (
  audit_id STRING COMMENT 'Unique audit record ID',
  metric_id STRING COMMENT 'FK to gold_metric_config',
  refresh_timestamp TIMESTAMP COMMENT 'When the metric was refreshed',
  status STRING COMMENT 'SUCCESS, FAILED, RUNNING',
  source_row_count LONG COMMENT 'Number of rows read from sources',
  target_row_count LONG COMMENT 'Number of rows written to target',
  metric_value DOUBLE COMMENT 'Calculated metric value (if single value)',
  error_message STRING COMMENT 'Error message if failed',
  execution_time_seconds DOUBLE COMMENT 'Execution time in seconds',
  refresh_by STRING COMMENT 'User or system that triggered refresh',
  data_quality_score DOUBLE COMMENT 'DQ score for this refresh (0-1)',
  notes STRING COMMENT 'Additional notes'
)
USING DELTA
PARTITIONED BY (DATE(refresh_timestamp))
LOCATION 'dbfs:/mnt/pc_insurance/reference/gold_refresh_audit'
COMMENT 'Audit log for Gold metric refresh runs';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Helper Views

-- COMMAND ----------

-- View for active metrics
CREATE OR REPLACE VIEW pc_insurance.reference.gold_active_metrics AS
SELECT 
  metric_id,
  metric_name,
  metric_category,
  target_table,
  refresh_frequency,
  execution_order,
  owner
FROM pc_insurance.reference.gold_metric_config
WHERE is_active = TRUE
ORDER BY execution_order;

-- View for metric refresh status
CREATE OR REPLACE VIEW pc_insurance.reference.gold_refresh_status AS
SELECT 
  c.metric_id,
  c.metric_name,
  c.target_table,
  a.refresh_timestamp AS last_refresh,
  a.status AS last_status,
  a.target_row_count AS last_row_count,
  a.execution_time_seconds AS last_execution_time,
  a.data_quality_score AS last_dq_score
FROM pc_insurance.reference.gold_metric_config c
LEFT JOIN (
  SELECT 
    metric_id,
    refresh_timestamp,
    status,
    target_row_count,
    execution_time_seconds,
    data_quality_score,
    ROW_NUMBER() OVER (PARTITION BY metric_id ORDER BY refresh_timestamp DESC) AS rn
  FROM pc_insurance.reference.gold_refresh_audit
) a ON c.metric_id = a.metric_id AND a.rn = 1
WHERE c.is_active = TRUE
ORDER BY c.execution_order;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Configuration

-- COMMAND ----------

SELECT 
  metric_id,
  metric_name,
  metric_category,
  target_table,
  aggregation_grain,
  refresh_frequency,
  execution_order,
  is_active,
  owner
FROM pc_insurance.reference.gold_metric_config
ORDER BY execution_order;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuration Complete

-- COMMAND ----------

SELECT 'Gold metadata configuration complete' AS status,
       COUNT(*) AS active_metrics,
       COUNT(DISTINCT metric_category) AS metric_categories
FROM pc_insurance.reference.gold_metric_config
WHERE is_active = TRUE;
