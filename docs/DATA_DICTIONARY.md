# P&C Insurance Data Dictionary

## Overview

This document provides detailed definitions for all tables, columns, and metrics in the P&C Insurance Medallion architecture.

---

## Bronze Layer Tables

### pc_insurance.bronze.policies_raw

**Description**: Raw policy data from the Policy Administration System

| Column | Data Type | Description | Source | Nullable |
|--------|-----------|-------------|--------|----------|
| policy_id | STRING | Unique policy identifier | Policy Admin | No |
| policy_number | STRING | Human-readable policy number | Policy Admin | No |
| policy_status | STRING | Policy status (ACTIVE, EXPIRED, CANCELLED) | Policy Admin | No |
| policy_type | STRING | Type of policy (NEW, RENEWAL, ENDORSEMENT) | Policy Admin | No |
| line_of_business | STRING | Line of business (Auto, Property, Liability, etc.) | Policy Admin | No |
| customer_id | STRING | Foreign key to customer | Policy Admin | No |
| agent_id | STRING | Foreign key to agent | Policy Admin | No |
| effective_date | DATE | Policy effective date | Policy Admin | No |
| expiry_date | DATE | Policy expiration date | Policy Admin | No |
| premium_amount | DECIMAL(12,2) | Total premium amount | Policy Admin | No |
| coverage_limit | DECIMAL(12,2) | Maximum coverage limit | Policy Admin | No |
| deductible | DECIMAL(10,2) | Policy deductible amount | Policy Admin | No |
| state | STRING | State where policy is issued | Policy Admin | No |
| territory_code | STRING | Territory code for rating | Policy Admin | Yes |
| endorsement_count | INT | Number of endorsements | Policy Admin | Yes |
| cancellation_date | DATE | Date policy was cancelled | Policy Admin | Yes |
| cancellation_reason | STRING | Reason for cancellation | Policy Admin | Yes |
| source_system | STRING | Source system identifier | Policy Admin | No |
| ingestion_timestamp | TIMESTAMP | When record was ingested | ETL | No |

**Partitioning**: By `ingestion_timestamp` (date)  
**Row Count**: ~1,000 (demo data)

---

### pc_insurance.bronze.claims_raw

**Description**: Raw claims data from the Claims Management System

| Column | Data Type | Description | Source | Nullable |
|--------|-----------|-------------|--------|----------|
| claim_id | STRING | Unique claim identifier | Claims System | No |
| claim_number | STRING | Human-readable claim number | Claims System | No |
| policy_id | STRING | Foreign key to policy | Claims System | No |
| customer_id | STRING | Foreign key to customer | Claims System | No |
| claim_type | STRING | Type of claim (Collision, Theft, Fire, etc.) | Claims System | No |
| claim_status | STRING | Claim status (OPEN, CLOSED, PENDING) | Claims System | No |
| loss_date | DATE | Date of loss occurrence | Claims System | No |
| report_date | DATE | Date claim was reported | Claims System | No |
| close_date | DATE | Date claim was closed | Claims System | Yes |
| incurred_loss | DECIMAL(12,2) | Total incurred loss amount | Claims System | No |
| paid_loss | DECIMAL(12,2) | Amount paid to date | Claims System | No |
| reserved_amount | DECIMAL(12,2) | Amount reserved for future payments | Claims System | No |
| expense_amount | DECIMAL(12,2) | Claim adjustment expenses | Claims System | Yes |
| deductible_applied | DECIMAL(10,2) | Deductible amount applied | Claims System | Yes |
| subrogation_amount | DECIMAL(12,2) | Amount recovered via subrogation | Claims System | Yes |
| adjuster_id | STRING | Assigned claims adjuster | Claims System | Yes |
| fraud_flag | BOOLEAN | Suspected fraud indicator | Claims System | No |
| litigation_flag | BOOLEAN | Litigation involved indicator | Claims System | No |

**Partitioning**: By `loss_date` (date)  
**Row Count**: ~300 (demo data)

---

### pc_insurance.bronze.premiums_raw

**Description**: Raw premium transaction data from the Billing System

| Column | Data Type | Description | Source | Nullable |
|--------|-----------|-------------|--------|----------|
| transaction_id | STRING | Unique transaction identifier | Billing System | No |
| policy_id | STRING | Foreign key to policy | Billing System | No |
| customer_id | STRING | Foreign key to customer | Billing System | No |
| transaction_type | STRING | Type (NEW, RENEWAL, ENDORSEMENT, CANCELLATION) | Billing System | No |
| transaction_date | DATE | Date of transaction | Billing System | No |
| premium_amount | DECIMAL(12,2) | Transaction premium amount | Billing System | No |
| written_premium | DECIMAL(12,2) | Written premium amount | Billing System | No |
| earned_premium | DECIMAL(12,2) | Earned premium amount | Billing System | No |
| unearned_premium | DECIMAL(12,2) | Unearned premium amount | Billing System | No |
| commission_amount | DECIMAL(12,2) | Commission paid to agent | Billing System | Yes |
| payment_frequency | STRING | Payment frequency (MONTHLY, QUARTERLY, ANNUAL) | Billing System | No |
| billing_status | STRING | Billing status (PAID, PENDING, OVERDUE) | Billing System | No |

**Partitioning**: By `transaction_date` (date)  
**Row Count**: ~1,200 (demo data)

---

### pc_insurance.bronze.customers_raw

**Description**: Raw customer data from the CRM system

| Column | Data Type | Description | Source | Nullable |
|--------|-----------|-------------|--------|----------|
| customer_id | STRING | Unique customer identifier | CRM | No |
| customer_name | STRING | Customer full name (PII) | CRM | No |
| customer_type | STRING | Type (INDIVIDUAL, BUSINESS) | CRM | No |
| date_of_birth | DATE | Customer date of birth (PII) | CRM | Yes |
| gender | STRING | Customer gender | CRM | Yes |
| address_line1 | STRING | Address line 1 (PII) | CRM | Yes |
| address_line2 | STRING | Address line 2 (PII) | CRM | Yes |
| city | STRING | City | CRM | Yes |
| state | STRING | State | CRM | No |
| zip_code | STRING | ZIP code | CRM | Yes |
| phone | STRING | Phone number (PII) | CRM | Yes |
| email | STRING | Email address (PII) | CRM | Yes |
| credit_score | INT | Credit score | CRM | Yes |
| occupation | STRING | Occupation | CRM | Yes |
| annual_income | DECIMAL(12,2) | Annual income | CRM | Yes |
| years_with_company | INT | Years as customer | CRM | Yes |

**Partitioning**: By `state`  
**Row Count**: ~500 (demo data)

---

### pc_insurance.bronze.agents_raw

**Description**: Raw agent data from the Agent Administration System

| Column | Data Type | Description | Source | Nullable |
|--------|-----------|-------------|--------|----------|
| agent_id | STRING | Unique agent identifier | Agent Admin | No |
| agent_name | STRING | Agent full name | Agent Admin | No |
| agency_name | STRING | Agency name | Agent Admin | Yes |
| agent_license_number | STRING | License number | Agent Admin | No |
| license_state | STRING | State where licensed | Agent Admin | No |
| agent_status | STRING | Agent status (ACTIVE, INACTIVE, SUSPENDED) | Agent Admin | No |
| commission_rate | DECIMAL(5,2) | Commission rate percentage | Agent Admin | Yes |
| appointment_date | DATE | Date agent was appointed | Agent Admin | Yes |

**Partitioning**: By `license_state`  
**Row Count**: ~50 (demo data)

---

## Silver Layer Tables

### pc_insurance.silver.policy_dim

**Description**: Cleansed and historized policy dimension (SCD Type 2)

| Column | Data Type | Description | Transformation |
|--------|-----------|-------------|----------------|
| policy_key | BIGINT | Surrogate key | Generated |
| policy_id | STRING | Business key | From bronze.policies_raw |
| policy_number | STRING | Policy number | Cleansed, trimmed |
| policy_status | STRING | Policy status | Uppercase, standardized |
| policy_type | STRING | Policy type | Uppercase, standardized |
| line_of_business | STRING | Line of business | Uppercase, standardized |
| customer_id | STRING | FK to customer_dim | From bronze |
| agent_id | STRING | FK to agent_dim | From bronze |
| effective_date | DATE | Effective date | Validated |
| expiry_date | DATE | Expiry date | Validated |
| premium_amount | DECIMAL(12,2) | Premium amount | Coalesced to 0 if null |
| coverage_limit | DECIMAL(12,2) | Coverage limit | Coalesced to 0 if null |
| deductible | DECIMAL(10,2) | Deductible | Coalesced to 0 if null |
| state | STRING | State | Uppercase, validated |
| territory_code | STRING | Territory code | Trimmed |
| is_current | BOOLEAN | Current version flag | SCD2 |
| effective_from | TIMESTAMP | SCD2 effective from | SCD2 |
| effective_to | TIMESTAMP | SCD2 effective to | SCD2 |
| created_at | TIMESTAMP | Record creation time | System |
| updated_at | TIMESTAMP | Record update time | System |

**Partitioning**: By `state`, `line_of_business`  
**SCD2**: Tracks changes to policy attributes

---

### pc_insurance.silver.claim_dim

**Description**: Cleansed and historized claim dimension (SCD Type 2)

| Column | Data Type | Description | Transformation |
|--------|-----------|-------------|----------------|
| claim_key | BIGINT | Surrogate key | Generated |
| claim_id | STRING | Business key | From bronze.claims_raw |
| claim_number | STRING | Claim number | Cleansed, trimmed |
| policy_id | STRING | FK to policy_dim | From bronze |
| customer_id | STRING | FK to customer_dim | From bronze |
| claim_type | STRING | Claim type | Uppercase, standardized |
| claim_status | STRING | Claim status | Uppercase, standardized |
| loss_date | DATE | Loss date | Validated |
| report_date | DATE | Report date | Validated |
| close_date | DATE | Close date | Validated |
| adjuster_id | STRING | Adjuster ID | Trimmed |
| fraud_flag | BOOLEAN | Fraud indicator | Coalesced to FALSE |
| litigation_flag | BOOLEAN | Litigation indicator | Coalesced to FALSE |
| is_current | BOOLEAN | Current version flag | SCD2 |
| effective_from | TIMESTAMP | SCD2 effective from | SCD2 |
| effective_to | TIMESTAMP | SCD2 effective to | SCD2 |
| created_at | TIMESTAMP | Record creation time | System |
| updated_at | TIMESTAMP | Record update time | System |

**Partitioning**: By `claim_type`  
**SCD2**: Tracks changes to claim attributes

---

### pc_insurance.silver.customer_dim

**Description**: Cleansed and historized customer dimension with PII masking (SCD Type 2)

| Column | Data Type | Description | Transformation |
|--------|-----------|-------------|----------------|
| customer_key | BIGINT | Surrogate key | Generated |
| customer_id | STRING | Business key | From bronze.customers_raw |
| customer_name_masked | STRING | Masked customer name | First char + asterisks |
| customer_type | STRING | Customer type | Uppercase, standardized |
| date_of_birth | DATE | Date of birth | Validated |
| gender | STRING | Gender | Uppercase |
| city | STRING | City | Trimmed |
| state | STRING | State | Uppercase, validated |
| zip_code | STRING | ZIP code | Validated format |
| credit_score | INT | Credit score | Coalesced to 0 |
| occupation | STRING | Occupation | Trimmed |
| annual_income | DECIMAL(12,2) | Annual income | Coalesced to 0 |
| years_with_company | INT | Years with company | Coalesced to 0 |
| is_current | BOOLEAN | Current version flag | SCD2 |
| effective_from | TIMESTAMP | SCD2 effective from | SCD2 |
| effective_to | TIMESTAMP | SCD2 effective to | SCD2 |
| created_at | TIMESTAMP | Record creation time | System |
| updated_at | TIMESTAMP | Record update time | System |

**Partitioning**: By `state`  
**SCD2**: Tracks changes to customer attributes  
**PII Masking**: Customer name masked for privacy

---

### pc_insurance.silver.agent_dim

**Description**: Cleansed and historized agent dimension (SCD Type 2)

| Column | Data Type | Description | Transformation |
|--------|-----------|-------------|----------------|
| agent_key | BIGINT | Surrogate key | Generated |
| agent_id | STRING | Business key | From bronze.agents_raw |
| agent_name | STRING | Agent name | Trimmed |
| agency_name | STRING | Agency name | Trimmed |
| agent_license_number | STRING | License number | Trimmed |
| license_state | STRING | License state | Uppercase, validated |
| agent_status | STRING | Agent status | Uppercase, standardized |
| commission_rate | DECIMAL(5,2) | Commission rate | Coalesced to 0 |
| appointment_date | DATE | Appointment date | Validated |
| is_current | BOOLEAN | Current version flag | SCD2 |
| effective_from | TIMESTAMP | SCD2 effective from | SCD2 |
| effective_to | TIMESTAMP | SCD2 effective to | SCD2 |
| created_at | TIMESTAMP | Record creation time | System |
| updated_at | TIMESTAMP | Record update time | System |

**Partitioning**: By `license_state`  
**SCD2**: Tracks changes to agent attributes

---

### pc_insurance.silver.premium_fact

**Description**: Cleansed premium transaction fact table

| Column | Data Type | Description | Transformation |
|--------|-----------|-------------|----------------|
| premium_fact_key | BIGINT | Surrogate key | Generated |
| transaction_id | STRING | Business key | From bronze.premiums_raw |
| policy_id | STRING | FK to policy_dim | From bronze |
| customer_id | STRING | FK to customer_dim | From bronze |
| transaction_type | STRING | Transaction type | Uppercase, standardized |
| transaction_date | DATE | Transaction date | Validated |
| premium_amount | DECIMAL(12,2) | Premium amount | Coalesced to 0 |
| written_premium | DECIMAL(12,2) | Written premium | Coalesced to 0 |
| earned_premium | DECIMAL(12,2) | Earned premium | Coalesced to 0 |
| unearned_premium | DECIMAL(12,2) | Unearned premium | Coalesced to 0 |
| commission_amount | DECIMAL(12,2) | Commission amount | Coalesced to 0 |
| payment_frequency | STRING | Payment frequency | Uppercase |
| billing_status | STRING | Billing status | Uppercase, standardized |
| created_at | TIMESTAMP | Record creation time | System |

**Partitioning**: By `transaction_date` (date)

---

### pc_insurance.silver.claim_fact

**Description**: Cleansed claim fact table

| Column | Data Type | Description | Transformation |
|--------|-----------|-------------|----------------|
| claim_fact_key | BIGINT | Surrogate key | Generated |
| claim_id | STRING | Business key | From bronze.claims_raw |
| policy_id | STRING | FK to policy_dim | From bronze |
| customer_id | STRING | FK to customer_dim | From bronze |
| loss_date | DATE | Loss date | Validated |
| report_date | DATE | Report date | Validated |
| close_date | DATE | Close date | Validated |
| incurred_loss | DECIMAL(12,2) | Incurred loss | Coalesced to 0 |
| paid_loss | DECIMAL(12,2) | Paid loss | Coalesced to 0 |
| reserved_amount | DECIMAL(12,2) | Reserved amount | Coalesced to 0 |
| expense_amount | DECIMAL(12,2) | Expense amount | Coalesced to 0 |
| deductible_applied | DECIMAL(10,2) | Deductible applied | Coalesced to 0 |
| subrogation_amount | DECIMAL(12,2) | Subrogation amount | Coalesced to 0 |
| created_at | TIMESTAMP | Record creation time | System |

**Partitioning**: By `loss_date` (date)

---

## Gold Layer Tables

### pc_insurance.gold.loss_ratio_by_lob

**Description**: Loss ratio metrics aggregated by line of business

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| line_of_business | STRING | Line of business | From policy_dim |
| total_incurred_loss | DECIMAL(18,2) | Total incurred losses | SUM(claim_fact.incurred_loss) |
| total_earned_premium | DECIMAL(18,2) | Total earned premium | SUM(premium_fact.earned_premium) |
| loss_ratio | DECIMAL(10,4) | Loss ratio | total_incurred_loss / total_earned_premium |
| claim_count | BIGINT | Number of claims | COUNT(DISTINCT claim_id) |
| avg_loss_per_claim | DECIMAL(18,2) | Average loss per claim | total_incurred_loss / claim_count |
| as_of_date | DATE | Snapshot date | Current date |

**Grain**: One row per line of business  
**Refresh**: Daily

---

### pc_insurance.gold.claim_frequency_severity

**Description**: Claim frequency and severity metrics by state

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| state | STRING | State | From policy_dim |
| policy_count | BIGINT | Number of policies | COUNT(DISTINCT policy_id) |
| claim_count | BIGINT | Number of claims | COUNT(DISTINCT claim_id) |
| claim_frequency | DECIMAL(10,4) | Claims per policy | claim_count / policy_count |
| total_incurred_loss | DECIMAL(18,2) | Total incurred losses | SUM(incurred_loss) |
| avg_severity | DECIMAL(18,2) | Average loss per claim | total_incurred_loss / claim_count |
| pure_premium | DECIMAL(18,2) | Pure premium | claim_frequency × avg_severity |
| as_of_date | DATE | Snapshot date | Current date |

**Grain**: One row per state  
**Refresh**: Daily

---

### pc_insurance.gold.retention_by_agent

**Description**: Policy retention metrics by agent

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| agent_id | STRING | Agent ID | From agent_dim |
| agent_name | STRING | Agent name | From agent_dim |
| total_policies | BIGINT | Total policies | COUNT(policy_id) |
| renewed_policies | BIGINT | Renewed policies | COUNT WHERE policy_status = 'RENEWED' |
| retention_rate | DECIMAL(10,4) | Retention rate | renewed_policies / total_policies |
| total_premium | DECIMAL(18,2) | Total premium | SUM(premium_amount) |
| avg_premium_per_policy | DECIMAL(18,2) | Avg premium per policy | total_premium / total_policies |
| as_of_date | DATE | Snapshot date | Current date |

**Grain**: One row per agent  
**Refresh**: Monthly

---

### pc_insurance.gold.premium_growth

**Description**: Premium growth trends over time

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| year_month | STRING | Year-month (YYYY-MM) | From transaction_date |
| written_premium | DECIMAL(18,2) | Total written premium | SUM(written_premium) |
| earned_premium | DECIMAL(18,2) | Total earned premium | SUM(earned_premium) |
| policy_count | BIGINT | Number of policies | COUNT(DISTINCT policy_id) |
| avg_premium_per_policy | DECIMAL(18,2) | Avg premium per policy | written_premium / policy_count |
| mom_growth_rate | DECIMAL(10,4) | Month-over-month growth | (current - previous) / previous |
| yoy_growth_rate | DECIMAL(10,4) | Year-over-year growth | (current - prior_year) / prior_year |

**Grain**: One row per month  
**Refresh**: Monthly

---

### pc_insurance.gold.exposure_summary

**Description**: Exposure (coverage limits) summary by state and LOB

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| state | STRING | State | From policy_dim |
| line_of_business | STRING | Line of business | From policy_dim |
| policy_count | BIGINT | Number of policies | COUNT(policy_id) |
| total_coverage_limit | DECIMAL(18,2) | Total coverage | SUM(coverage_limit) |
| total_premium | DECIMAL(18,2) | Total premium | SUM(premium_amount) |
| avg_coverage_per_policy | DECIMAL(18,2) | Avg coverage per policy | total_coverage_limit / policy_count |
| avg_premium_per_policy | DECIMAL(18,2) | Avg premium per policy | total_premium / policy_count |
| as_of_date | DATE | Snapshot date | Current date |

**Grain**: One row per state × line of business  
**Refresh**: Daily

---

### pc_insurance.gold.uw_dashboard_summary

**Description**: Comprehensive underwriting dashboard metrics

| Column | Data Type | Description | Calculation |
|--------|-----------|-------------|-------------|
| line_of_business | STRING | Line of business | From policy_dim |
| state | STRING | State | From policy_dim |
| policy_count | BIGINT | Number of policies | COUNT(DISTINCT policy_id) |
| total_premium | DECIMAL(18,2) | Total premium | SUM(premium_amount) |
| total_incurred_loss | DECIMAL(18,2) | Total incurred loss | SUM(incurred_loss) |
| loss_ratio | DECIMAL(10,4) | Loss ratio | total_incurred_loss / total_premium |
| claim_count | BIGINT | Number of claims | COUNT(DISTINCT claim_id) |
| claim_frequency | DECIMAL(10,4) | Claim frequency | claim_count / policy_count |
| avg_severity | DECIMAL(18,2) | Average severity | total_incurred_loss / claim_count |
| as_of_date | DATE | Snapshot date | Current date |

**Grain**: One row per line of business × state  
**Refresh**: Daily

---

## Metadata Tables

### pc_insurance.reference.silver_transformation_config

**Description**: Configuration for metadata-driven Silver transformations

See `sql/03_silver_transformation_config.sql` for schema.

---

### pc_insurance.reference.gold_metric_config

**Description**: Configuration for metadata-driven Gold KPI generation

See `sql/05_gold_metric_config.sql` for schema.

---

## Key Business Metrics

### Loss Ratio
**Formula**: `Incurred Loss / Earned Premium`  
**Interpretation**: Percentage of premium paid out in claims. Lower is better.  
**Benchmark**: < 0.70 is good, > 1.00 indicates underwriting loss.

### Claim Frequency
**Formula**: `Number of Claims / Number of Policies`  
**Interpretation**: Average number of claims per policy.

### Claim Severity
**Formula**: `Total Incurred Loss / Number of Claims`  
**Interpretation**: Average loss amount per claim.

### Pure Premium
**Formula**: `Claim Frequency × Claim Severity`  
**Interpretation**: Expected loss cost per policy.

### Retention Rate
**Formula**: `Renewed Policies / Total Policies`  
**Interpretation**: Percentage of policies renewed. Higher is better.

### Combined Ratio
**Formula**: `(Incurred Loss + Expenses) / Earned Premium`  
**Interpretation**: Overall underwriting profitability. < 1.00 is profitable.

---

**Version**: 1.0  
**Last Updated**: 2026-09-24  
**Owner**: Data Engineering Team
