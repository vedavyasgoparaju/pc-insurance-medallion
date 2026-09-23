-- Bronze Layer DDL
-- ============================================
USE CATALOG pc_insurance;
USE SCHEMA bronze;

CREATE TABLE IF NOT EXISTS policies_raw (
    policy_id STRING NOT NULL, policy_number STRING, policy_status STRING,
    policy_type STRING, line_of_business STRING, customer_id STRING, agent_id STRING,
    effective_date DATE, expiry_date DATE, premium_amount DECIMAL(12,2),
    coverage_limit DECIMAL(12,2), deductible DECIMAL(10,2), state STRING,
    territory_code STRING, endorsement_count INT, cancellation_date DATE,
    cancellation_reason STRING, source_system STRING, ingestion_timestamp TIMESTAMP,
    raw_payload STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS claims_raw (
    claim_id STRING NOT NULL, claim_number STRING, policy_id STRING, customer_id STRING,
    claim_type STRING, claim_status STRING, loss_date DATE, report_date DATE,
    close_date DATE, incurred_loss DECIMAL(12,2), paid_loss DECIMAL(12,2),
    reserved_amount DECIMAL(12,2), expense_amount DECIMAL(12,2),
    deductible_applied DECIMAL(10,2), subrogation_amount DECIMAL(12,2),
    adjuster_id STRING, fraud_flag BOOLEAN, litigation_flag BOOLEAN,
    source_system STRING, ingestion_timestamp TIMESTAMP, raw_payload STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS premiums_raw (
    transaction_id STRING NOT NULL, policy_id STRING, customer_id STRING,
    transaction_type STRING, transaction_date DATE, premium_amount DECIMAL(12,2),
    written_premium DECIMAL(12,2), earned_premium DECIMAL(12,2),
    unearned_premium DECIMAL(12,2), commission_amount DECIMAL(12,2),
    payment_frequency STRING, billing_status STRING, source_system STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS customers_raw (
    customer_id STRING NOT NULL, customer_name STRING, customer_type STRING,
    date_of_birth DATE, gender STRING, address_line1 STRING, address_line2 STRING,
    city STRING, state STRING, zip_code STRING, phone STRING, email STRING,
    credit_score INT, occupation STRING, annual_income DECIMAL(12,2),
    years_with_company INT, source_system STRING, ingestion_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS agents_raw (
    agent_id STRING NOT NULL, agent_name STRING, agency_name STRING,
    agent_license_number STRING, license_state STRING, agent_status STRING,
    commission_rate DECIMAL(5,2), appointment_date DATE,
    source_system STRING, ingestion_timestamp TIMESTAMP
) USING DELTA;
