-- Gold Layer DDL
-- ============================================
USE CATALOG pc_insurance;
USE SCHEMA gold;

CREATE TABLE IF NOT EXISTS loss_ratio_by_lob (
    reporting_period STRING, period_type STRING, line_of_business STRING,
    earned_premium DECIMAL(18,2), incurred_losses DECIMAL(18,2),
    expense_amount DECIMAL(18,2), loss_ratio DECIMAL(10,4),
    expense_ratio DECIMAL(10,4), combined_ratio DECIMAL(10,4),
    policy_count BIGINT, claim_count BIGINT, loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS claim_frequency_severity (
    reporting_period STRING, period_type STRING, line_of_business STRING, state STRING,
    exposure_units BIGINT, claim_count BIGINT, claim_frequency DECIMAL(10,4),
    incurred_losses DECIMAL(18,2), claim_severity DECIMAL(18,2),
    avg_paid_loss DECIMAL(18,2), avg_reserved DECIMAL(18,2), loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS retention_by_agent (
    reporting_period STRING, period_type STRING, agent_id STRING, agent_name STRING,
    agency_name STRING, total_policies BIGINT, renewed_policies BIGINT,
    cancelled_policies BIGINT, new_business_policies BIGINT,
    retention_rate DECIMAL(10,4), new_business_growth DECIMAL(10,4),
    total_written_premium DECIMAL(18,2), loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS premium_growth (
    reporting_period STRING, period_type STRING, line_of_business STRING,
    total_written_premium DECIMAL(18,2), total_earned_premium DECIMAL(18,2),
    new_business_premium DECIMAL(18,2), renewal_premium DECIMAL(18,2),
    endorsement_premium DECIMAL(18,2), cancelled_premium DECIMAL(18,2),
    growth_rate DECIMAL(10,4), loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS exposure_summary (
    reporting_period STRING, period_type STRING, line_of_business STRING, state STRING,
    total_policies BIGINT, active_policies BIGINT, cancelled_policies BIGINT,
    total_coverage_limit DECIMAL(18,2), avg_premium_per_policy DECIMAL(18,2),
    total_earned_exposure BIGINT, loaded_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS uw_dashboard_summary (
    reporting_period STRING, period_type STRING, line_of_business STRING,
    earned_premium DECIMAL(18,2), written_premium DECIMAL(18,2),
    incurred_losses DECIMAL(18,2), expense_amount DECIMAL(18,2),
    loss_ratio DECIMAL(10,4), combined_ratio DECIMAL(10,4),
    new_policies BIGINT, renewed_policies BIGINT, cancelled_policies BIGINT,
    open_claims BIGINT, closed_claims BIGINT, avg_claim_severity DECIMAL(18,2),
    loaded_at TIMESTAMP
) USING DELTA;
