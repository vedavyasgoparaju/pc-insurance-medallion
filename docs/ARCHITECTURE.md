# P&C Insurance Medallion Architecture

## Overview

This document describes the Medallion architecture for the Property & Casualty (P&C) Insurance data platform built on Databricks. The architecture follows the Bronze-Silver-Gold pattern with metadata-driven transformations and multi-agent orchestration.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                             │
│  Policy Admin │ Claims System │ Billing │ CRM │ Agent Admin     │
└────────┬────────────┬───────────┬───────┬───────────┬───────────┘
         │            │           │       │           │
         ▼            ▼           ▼       ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ policies_raw │  │  claims_raw  │  │ premiums_raw │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────┐  ┌──────────────┐                            │
│  │customers_raw │  │  agents_raw  │                            │
│  └──────────────┘  └──────────────┘                            │
└────────┬────────────┬───────────┬───────┬───────────┬───────────┘
         │            │           │       │           │
         ▼            ▼           ▼       ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│              SILVER LAYER (Cleansed & Conformed)                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  policy_dim  │  │   claim_dim  │  │ customer_dim │          │
│  │    (SCD2)    │  │    (SCD2)    │  │    (SCD2)    │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  agent_dim   │  │ premium_fact │  │  claim_fact  │          │
│  │    (SCD2)    │  │              │  │              │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                    ┌──────────────┐                             │
│                    │   date_dim   │                             │
│                    └──────────────┘                             │
└────────┬────────────┬───────────┬───────┬───────────┬───────────┘
         │            │           │       │           │
         ▼            ▼           ▼       ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Business KPIs)                      │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │ loss_ratio_by_lob    │  │claim_frequency_      │            │
│  │                      │  │     severity         │            │
│  └──────────────────────┘  └──────────────────────┘            │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │retention_by_agent    │  │  premium_growth      │            │
│  └──────────────────────┘  └──────────────────────┘            │
│  ┌──────────────────────┐  ┌──────────────────────┐            │
│  │ exposure_summary     │  │uw_dashboard_summary  │            │
│  └──────────────────────┘  └──────────────────────┘            │
└─────────────────────────────────────────────────────────────────┘
```

## Unity Catalog Structure

### Catalog: `pc_insurance`

#### Schemas

1. **bronze**: Raw ingested data
2. **silver**: Cleansed, conformed, and historized data
3. **gold**: Business-level aggregations and KPIs
4. **reference**: Metadata and configuration tables
5. **dq**: Data quality validation results

## Bronze Layer

### Purpose
- Ingest raw data from source systems
- Minimal transformation (schema enforcement only)
- Preserve source data lineage

### Tables

| Table | Source System | Row Count | Key Columns |
|-------|---------------|-----------|-------------|
| policies_raw | Policy Admin | 1,000 | policy_id, customer_id, agent_id |
| claims_raw | Claims System | 300 | claim_id, policy_id |
| premiums_raw | Billing System | 1,200 | transaction_id, policy_id |
| customers_raw | CRM | 500 | customer_id |
| agents_raw | Agent Admin | 50 | agent_id |

### Implementation
- **Notebook**: `Bronze_Pipeline.py`
- **Format**: Delta Lake
- **Partitioning**: By ingestion date
- **Refresh**: Daily full load (demo) / CDC in production

## Silver Layer

### Purpose
- Data cleansing and standardization
- SCD Type 2 historization for dimensions
- PII masking
- Data quality validation
- Metadata-driven transformations

### Metadata-Driven Framework

The Silver layer uses metadata configuration tables to drive transformations:

#### Configuration Table: `pc_insurance.reference.silver_transformation_config`

Defines:
- Source and target tables
- Transformation type (FULL_LOAD, INCREMENTAL, SCD2)
- Business keys
- Column mappings
- Cleansing rules
- Deduplication strategy
- Execution order

#### Audit Table: `pc_insurance.reference.silver_load_audit`

Tracks:
- Transformation run status
- Row counts (source, target, inserted, updated)
- Execution time
- Error messages

#### Reconciliation Table: `pc_insurance.reference.silver_reconciliation`

Validates:
- Source vs target row counts
- Count match status
- Reconciliation notes

### Tables

#### Dimensions (SCD2)

| Table | Business Key | SCD2 Fields | Partitioning |
|-------|--------------|-------------|--------------|
| policy_dim | policy_id | is_current, effective_from, effective_to | state, line_of_business |
| claim_dim | claim_id | is_current, effective_from, effective_to | claim_type |
| customer_dim | customer_id | is_current, effective_from, effective_to | state |
| agent_dim | agent_id | is_current, effective_from, effective_to | license_state |
| date_dim | date_key | N/A (static) | year, month |

#### Facts

| Table | Grain | Key Columns | Partitioning |
|-------|-------|-------------|--------------|
| premium_fact | Transaction | transaction_id, policy_id | transaction_date |
| claim_fact | Claim snapshot | claim_id, report_date | loss_date |

### Implementation
- **Notebook**: `Silver_Pipeline_Metadata.py`
- **Format**: Delta Lake
- **SCD2**: Managed via metadata config
- **DQ Checks**: Inline expectations

## Gold Layer

### Purpose
- Business-level KPIs and metrics
- Pre-aggregated for performance
- Optimized for BI and analytics
- Metadata-driven metric generation

### Metadata-Driven Framework

The Gold layer uses metadata configuration tables to drive KPI generation:

#### Configuration Table: `pc_insurance.reference.gold_metric_config`

Defines:
- Metric name and category
- Target table
- Source tables
- Dimension and measure columns
- Calculation logic
- Aggregation grain
- Refresh frequency
- Execution order

#### Audit Table: `pc_insurance.reference.gold_refresh_audit`

Tracks:
- Metric refresh status
- Row counts
- Calculated metric values
- Data quality scores
- Execution time

### Tables

| Table | Grain | Key Metrics | Refresh |
|-------|-------|-------------|---------|
| loss_ratio_by_lob | Line of Business | Loss Ratio, Claim Count, Avg Loss | Daily |
| claim_frequency_severity | State | Frequency, Severity, Pure Premium | Daily |
| retention_by_agent | Agent | Retention Rate, Policy Count | Monthly |
| premium_growth | Month | Written/Earned Premium, Growth % | Monthly |
| exposure_summary | State × LOB | Coverage Limit, Premium | Daily |
| uw_dashboard_summary | LOB × State | Comprehensive UW Metrics | Daily |

### Implementation
- **Notebook**: `Gold_Pipeline.py`
- **Format**: Delta Lake
- **Optimization**: Z-ordering on key dimensions
- **Refresh**: Scheduled based on metadata config

## Multi-Agent Architecture

### Agent Roles

1. **Architect Agent**: Designs schemas and data flow
2. **Data Engineer Agent**: Implements pipelines
3. **Domain Expert Agent**: Provides P&C insurance knowledge
4. **Analyst Agent**: Queries Gold layer KPIs
5. **Supervisor Agent**: Orchestrates multi-agent workflows
6. **DevOps Agent**: Provides Git/CI-CD guidance

### Orchestration

- **Orchestrator**: `Orchestrator.py`
- **Execution Framework**: `execution/orchestrator.py`, `execution/plan_executor.py`
- **Job Definition**: `resources/multi_agent_execution_job.yml`

## Data Quality Framework

### Schema: `pc_insurance.dq`

#### Table: `dq_validation_results`

Tracks:
- Validation rule
- Table and column
- Pass/fail status
- Failed record count
- Validation timestamp

### DQ Checks

- **Completeness**: NOT NULL checks on required fields
- **Validity**: Status code validation, date range checks
- **Consistency**: Referential integrity between layers
- **Accuracy**: Business rule validation (e.g., loss_ratio <= 2.0)

## Deployment

### Databricks Asset Bundles (DAB)

- **Configuration**: `databricks.yml`
- **Environments**: dev, staging, prod
- **Deployment**: `databricks bundle deploy -e prod`

### CI/CD Pipeline

- **Git Repository**: `/Repos/vedavyas.goparaju/pc-insurance-medallion`
- **Workflow**: `resources/git_auto_push_workflow.yml`
- **Automation**: `Git_Automation.py`

## Security & Governance

### Unity Catalog

- **Catalog**: `pc_insurance` (managed)
- **Access Control**: Table-level grants
- **Lineage**: Automatic via Delta Lake
- **Audit**: System tables

### PII Masking

- **Customer Names**: Masked in `customer_dim`
- **Method**: First character + asterisks
- **Compliance**: GDPR, CCPA ready

## Performance Optimization

### Delta Lake Features

- **Liquid Clustering**: On key dimensions
- **Z-Ordering**: On frequently filtered columns
- **Auto-optimize**: Enabled on all tables
- **Vacuum**: Scheduled weekly

### Partitioning Strategy

- **Bronze**: By ingestion_date
- **Silver Dimensions**: By business-relevant columns (state, LOB)
- **Silver Facts**: By transaction/event date
- **Gold**: By time period (year_month)

## Monitoring & Observability

### Metrics

- Pipeline execution time
- Row counts per layer
- Data quality scores
- Reconciliation status

### Alerts

- Pipeline failures
- DQ score < 0.95
- Reconciliation mismatches
- SLA breaches

## Future Enhancements

1. **Real-time Streaming**: Kafka → Bronze
2. **ML Models**: Fraud detection, claim prediction
3. **Advanced Analytics**: Cohort analysis, churn prediction
4. **Data Mesh**: Domain-oriented ownership
5. **Lakehouse Federation**: Query external sources

## References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Delta Lake Documentation](https://docs.delta.io/)

---

**Version**: 1.0  
**Last Updated**: 2026-09-24  
**Owner**: Data Engineering Team
