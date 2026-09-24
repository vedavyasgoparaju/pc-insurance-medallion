# P&C Insurance Medallion Architecture

**Version:** 3.0  
**Last Updated:** 2026-09-25  
**Repository:** `vedavyasgoparaju/pc-insurance-medallion`  
**Workspace:** `https://dbc-ec4d2e3d-58c3.cloud.databricks.com`

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture Layers](#architecture-layers)
3. [Multi-Agent System](#multi-agent-system)
4. [Agent Roles & Responsibilities](#agent-roles--responsibilities)
5. [Metadata-Driven Framework](#metadata-driven-framework)
6. [Data Flow](#data-flow)
7. [Unity Catalog Structure](#unity-catalog-structure)
8. [Pipeline Orchestration](#pipeline-orchestration)
9. [Deployment Architecture](#deployment-architecture)
10. [Operations & Monitoring](#operations--monitoring)
11. [Performance Optimization](#performance-optimization)
12. [Future Enhancements](#future-enhancements)

---

## Overview

The P&C Insurance Medallion Architecture is a comprehensive data platform built on Databricks that implements:

- **Medallion Architecture**: Bronze → Silver → Gold layers
- **Multi-Agent System**: 8 specialized AI tools (7 agents + 1 MCP server) for different domains
- **Metadata-Driven Pipelines**: Configuration-based Silver and Gold transformations
- **Unity Catalog Governance**: Centralized data governance and security
- **Declarative Automation**: DAB-based deployment and CI/CD

### Key Features

✅ **Automated Data Pipeline**: End-to-end Bronze → Silver → Gold processing  
✅ **AI-Powered Agents**: Domain experts, analysts, architects, and engineers  
✅ **Metadata-Driven**: No hardcoded transformations, all config-based  
✅ **SCD Type 2**: Historical tracking for dimensions  
✅ **Data Quality**: Built-in validation and reconciliation  
✅ **PII Masking**: Automated sensitive data protection  
✅ **Git Integration**: Version control and CI/CD ready

---

## Architecture Layers

### Bronze Layer (Raw Ingestion)

**Purpose**: Ingest raw data from source systems with minimal transformation

**Tables**:
- `policies_raw` - Policy administration data (~1,000 records)
- `claims_raw` - Claims management data (~300 records)
- `premiums_raw` - Billing and premium transactions (~1,200 records)
- `customers_raw` - Customer master data (~500 records)
- `agents_raw` - Agent and agency data (~50 records)

**Characteristics**:
- Schema enforcement only
- Preserve source data lineage
- Append-only (with ingestion timestamp)
- Partitioned by ingestion date
- Delta Lake format

**Implementation**: `pipelines/Bronze_Pipeline.py`

### Silver Layer (Cleansed & Conformed)

**Purpose**: Cleanse, standardize, and historize data with SCD Type 2

**Dimensions** (SCD2):
- `policy_dim` - Policy dimension with history
- `claim_dim` - Claim dimension with history
- `customer_dim` - Customer dimension with PII masking
- `agent_dim` - Agent dimension with history
- `date_dim` - Date dimension (static)

**Facts**:
- `premium_fact` - Premium transactions
- `claim_fact` - Claim financial details

**Characteristics**:
- **Metadata-Driven**: Transformations driven by `silver_transformation_config`
- **SCD Type 2**: `is_current`, `effective_from`, `effective_to` tracking
- **Data Cleansing**: Standardization, null handling, type conversion
- **PII Masking**: Customer names masked
- **Deduplication**: Latest record based on ingestion timestamp
- **Audit Logging**: Every run logged in `silver_load_audit`
- **Reconciliation**: Source vs target counts in `silver_reconciliation`

**Implementation**: `pipelines/Silver_Pipeline_Metadata.py`

### Gold Layer (Business KPIs)

**Purpose**: Pre-aggregated business metrics for analytics and reporting

**Tables**:
- `loss_ratio_by_lob` - Loss ratio by line of business
- `claim_frequency_severity` - Frequency and severity by state
- `retention_by_agent` - Policy retention by agent
- `premium_growth` - Premium growth trends over time
- `exposure_summary` - Exposure by state and LOB
- `uw_dashboard_summary` - Comprehensive underwriting dashboard

**Characteristics**:
- **Metadata-Driven**: Metrics driven by `gold_metric_config`
- **Pre-Aggregated**: Optimized for BI tools
- **Business Logic**: Loss ratios, frequencies, retention rates
- **Audit Logging**: Every refresh logged in `gold_refresh_audit`
- **Data Quality Scores**: DQ metrics tracked per refresh

**Implementation**: `pipelines/Gold_Pipeline.py`

---

## Multi-Agent System

The platform uses a **multi-agent architecture** where specialized AI agents handle different aspects of the data platform.

### Agent Architecture

```
                    ┌─────────────────────┐
                    │  Supervisor Agent   │
                    │   (Orchestrator)    │
                    └──────────┬──────────┘
                               │
                ┌──────────────┼──────────────┐
                │              │              │
        ┌───────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
        │  Architect   │ │   Data   │ │   Domain   │
        │    Agent     │ │ Engineer │ │   Expert   │
        └──────────────┘ └──────────┘ └────────────┘
                │              │              │
        ┌───────▼──────┐ ┌────▼─────┐ ┌─────▼──────┐
        │   Analyst    │ │  DevOps  │ │     QA     │
        │    Agent     │ │   Agent  │ │ Validator  │
        └──────────────┘ └──────────┘ └────────────┘
                │              │              
        ┌───────▼──────────────▼──────┐
        │ Documentation Agent         │
        └────────────────────┬────────┘
                             │
                ┌────────────▼─────────────┐
                │ Workspace-Actions (MCP)  │
                │  (Git, File, SQL Exec)   │
                └──────────────────────────┘
```

### Agent Communication Flow

1. **User Request** → Supervisor Agent
2. **Supervisor** analyzes request and routes to appropriate agent(s)
3. **Specialist Agent(s)** process request and return results
4. **Supervisor** synthesizes responses and returns to user

---

## Agent Roles & Responsibilities

### 1. Supervisor Agent

**Role**: Orchestrates multi-agent workflows and routes requests

**Responsibilities**:
- ✅ Analyze user requests and determine which agents to invoke
- ✅ Route questions to appropriate specialist agents
- ✅ Synthesize responses from multiple agents
- ✅ Manage agent execution order and dependencies
- ✅ Handle errors and fallback logic

**Implementation**: `Supervisor_Agent.py`
**Endpoint**: `mas-56389669-endpoint` (READY)

### 2. Architect Agent

**Role**: Designs data architecture, schemas, and data flow

**Responsibilities**:
- Design Bronze/Silver/Gold layer schemas
- Define table structures and relationships
- Plan data flow topology
- Design Unity Catalog governance model
- Define partitioning and optimization strategies

**Implementation**: `Architect_Agent.py` (serving_endpoint `pc_architect_agent`)

### 3. Data Engineer Agent

**Role**: Implements pipelines, transformations, and data quality checks

**Responsibilities**:
- Write Spark/SQL transformation code
- Implement Bronze → Silver → Gold pipelines
- Create metadata-driven frameworks
- Implement SCD Type 2 logic
- Write data quality expectations

**Implementation**: `Data_Engineer_Agent.py` (serving_endpoint `pc_data_engineer_agent`)

### 4. Domain Expert Agent

**Role**: Provides P&C insurance domain knowledge and business context

**Responsibilities**:
- Answer questions about P&C insurance concepts
- Explain loss ratios, combined ratios, frequency/severity
- Provide context on policy lifecycle
- Explain claims processing and reserving
- Interpret NAIC requirements

**Implementation**: `Domain_Expert_Agent.py`
**Data Source**: UC Volume `pc_insurance.reference.pc_domain_docs`

### 5. Analyst Agent

**Role**: Answers business questions by querying Gold layer KPIs

**Responsibilities**:
- Query Gold layer tables for business metrics
- Calculate loss ratios, frequencies, retention rates
- Provide trend analysis
- Generate business reports

**Implementation**: `Analyst_Genie_Agent.py` (genie_space)
**Key Metrics**:
- Loss Ratio = Incurred Loss / Earned Premium
- Claim Frequency = Claims / Policies
- Claim Severity = Incurred Loss / Claims
- Retention Rate = Renewed / (Renewed + Cancelled)
- Premium Growth = (Current - Previous) / Previous

### 6. DevOps Agent

**Role**: Provides guidance on Git, CI/CD, and deployment (GUIDANCE ONLY)

**Responsibilities**:
- Advise on Git workflows and branching strategies
- Guide CI/CD pipeline setup
- Explain Databricks Asset Bundles (DAB)
- Provide deployment best practices
- Troubleshoot Git issues

**Implementation**: `DevOps_Agent.py` (genie_space)

### 7. QA Validator Agent

**Role**: Validates data quality and runs validation checks

**Responsibilities**:
- Run data quality checks on tables
- Validate data completeness, validity, consistency
- Check business rule compliance
- Generate DQ reports

**Implementation**: Via `pc_insurance.dq.calculate_dq_score` (uc_function)

### 8. Workspace-Actions (MCP App)

**Role**: EXECUTES workspace changes (git commits, file writes, SQL execution)

**Implementation**: `app/app.py` (MCP app `pc-insurance-workspace-actions`)

### Anti-Routing Rules

1. KPI questions → **Analyst only** (never DevOps or Documentation)
2. Git execution → **Workspace-Actions** (DevOps is guidance only)
3. Architecture design → **Architect** (not Data Engineer)
4. Code implementation → **Data Engineer** (not Architect)
5. Domain definitions → **Domain Expert** (not Analyst)
6. DevOps → **Guidance only** (cannot execute Git operations)

---

## Metadata-Driven Framework

### Silver Layer Metadata

**Configuration Table**: `pc_insurance.reference.silver_transformation_config`

Defines: source/target tables, transformation type (SCD2, FACT, DEDUP), business keys, column mappings, cleansing rules, deduplication strategy, SCD2 enablement, execution order.

**Audit Table**: `pc_insurance.reference.silver_load_audit` — tracks transformation ID, run timestamp, status, source/target row counts, execution time, error messages.

**Reconciliation Table**: `pc_insurance.reference.silver_reconciliation` — validates source vs target counts, count match status, reconciliation notes.

**Example Configuration**:
```sql
INSERT INTO silver_transformation_config VALUES (
  'SILVER_POLICY_DIM', 'Policy Dimension',
  'bronze', 'policies_raw', 'silver', 'policy_dim',
  'SCD2', ARRAY('policy_id'),
  MAP('policy_status', 'UPPER(TRIM(policy_status))'),
  'LATEST', 'ingestion_timestamp', TRUE, 1, TRUE
);
```

### Gold Layer Metadata

**Configuration Table**: `pc_insurance.reference.gold_metric_config`

Defines: metric name/category, target table, source tables, dimension columns, measure columns, calculation logic, aggregation grain, refresh frequency, execution order.

**Example Configuration**:
```sql
INSERT INTO gold_metric_config VALUES (
  'GOLD_LOSS_RATIO_LOB', 'Loss Ratio by Line of Business',
  'LOSS_RATIO', 'gold', 'loss_ratio_by_lob',
  ARRAY('silver.policy_dim', 'silver.claim_fact'),
  ARRAY('line_of_business'),
  ARRAY('total_incurred_loss', 'total_earned_premium', 'loss_ratio'),
  'SUM(incurred_loss) / SUM(earned_premium) AS loss_ratio',
  'LINE_OF_BUSINESS', 'DAILY', 1, TRUE
);
```

**Audit Table**: `pc_insurance.reference.gold_refresh_audit` — tracks metric ID, refresh timestamp, status, row counts, metric values, DQ scores, execution time.

---

## Data Flow

### End-to-End Pipeline Flow

```
SOURCE SYSTEMS → BRONZE LAYER (Bronze_Pipeline.py)
  → SILVER LAYER (Silver_Pipeline_Metadata.py)
  → GOLD LAYER (Gold_Pipeline.py)
  → CONSUMPTION LAYER (BI Dashboards, Analyst Agent, ML models)
```

---

## Unity Catalog Structure

### Catalog: `pc_insurance`

```
pc_insurance/
├── bronze/                    # Raw ingested data
├── silver/                    # Cleansed & conformed
├── gold/                      # Business KPIs
├── reference/                 # Metadata & configuration
└── dq/                        # Data quality
```

### Security Model

**Catalog-Level**: `USE CATALOG` granted to all users, `CREATE SCHEMA` restricted to admins.
**Schema-Level**: `USE SCHEMA` granted to all users, `CREATE TABLE` granted to service principals.
**Table-Level**: `SELECT` granted to analysts/BI tools, `MODIFY` granted to service principals (pipelines).
**Volume-Level**: `READ VOLUME` granted to Domain Expert Agent, `WRITE VOLUME` granted to admins.

### PII Masking

- **Customer Names**: Masked in `customer_dim`
- **Method**: First character + asterisks
- **Compliance**: GDPR, CCPA ready

---

## Pipeline Orchestration

### Job Configuration

The project uses 2 jobs with distinct purposes:

| Job | Name | ID | Purpose |
|---|---|---|---|
| 1 | `PC_Insurance_Agent_Setup` | `820361677269451` | Agent setup only (run once): MLflow models, serving endpoints, Genie Spaces, Knowledge Assistant, Supervisor Agent |
| 2 | `PC_Insurance_Data_Pipeline` | `894776717783668` | Data pipeline: Bronze -> Silver -> Gold with `load_type` parameter (INITIAL or INCREMENTAL) |

**Architecture**: Agents are set up FIRST (Job 1). Pipeline execution is triggered separately (Job 2) -- either on a schedule or on-demand via the Supervisor Agent + MCP app.

### Data Pipeline Job (Job 2)

**Job Name**: `PC_Insurance_Data_Pipeline`
**Job ID**: `894776717783668`
**Schedule**: Optional (configure for daily incremental loads at 2:00 AM UTC)

**Tasks**:
1. **Bronze_Pipeline** (15 min) — Ingest data, write to Bronze tables, no dependencies
2. **Silver_Pipeline_Metadata** (30 min) — Depends on Bronze. Read metadata config, execute transformations, apply SCD2, write audit logs, run reconciliation
3. **Gold_Pipeline** (15 min) — Depends on Silver. Read metric config, execute aggregations, calculate business metrics, write audit logs, track DQ scores

**Load Types**: `INITIAL` (first-time full load) or `INCREMENTAL` (default, for scheduled runs)

### On-Demand Execution via Supervisor Agent

The Supervisor Agent can trigger pipeline notebooks on-demand through the MCP app's `run_notebook` tool. No separate orchestrator job is needed -- the Supervisor Agent + MCP app provide direct workspace execution capabilities.

**Notebook**: `pipelines/Orchestrator.py` (optional helper for manual multi-layer runs)

---

## Data Quality Framework

### Schema: `pc_insurance.dq`

**Table**: `dq_validation_results` — tracks validation rule, table/column, pass/fail status, failed record count, validation timestamp.

### DQ Checks

- **Completeness**: NOT NULL checks on required fields
- **Validity**: Status code validation, date range checks
- **Consistency**: Referential integrity between layers
- **Accuracy**: Business rule validation (e.g., loss_ratio <= 2.0)
- **Timeliness**: Data < 24 hours old

---

## Deployment Architecture

### Environments

| Environment | Catalog | Git Branch | Purpose |
|---|---|---|---|
| Development | `pc_insurance_dev` | `feature/*` or `dev` | Development and testing |
| Staging | `pc_insurance_staging` | `staging` | Pre-production validation |
| Production | `pc_insurance` | `main` | Production workloads |

### Databricks Asset Bundles (DAB)

**Configuration**: `databricks.yml`

```bash
databricks bundle deploy -t dev
databricks bundle deploy -t prod
```

### CI/CD Pipeline

- **Git Repository**: `/Repos/vedavyas.goparaju/pc-insurance-medallion`
- **MCP App**: `app/app.py` (`pc-insurance-workspace-actions`) handles git commits via subprocess CLI
- **Bundle Config**: `databricks.yml`

---

## Operations & Monitoring

### Daily Operations

**Morning Checklist**:
1. Check pipeline status (last 24 hours)
2. Verify data freshness (Bronze ingestion)
3. Review DQ scores (> 95% target)
4. Check reconciliation status (all PASS)
5. Review error logs (if any failures)

### Alerts

| Alert | Condition | Severity |
|---|---|---|
| Pipeline Failure | status = 'FAILED' | Critical |
| DQ Score Low | dq_score < 0.90 | Critical |
| DQ Score Warning | dq_score < 0.95 | Warning |
| Reconciliation Fail | recon_status = 'FAIL' | High |
| Data Freshness | No data in 24 hours | High |

---

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

---

## Future Enhancements

1. **Real-time Streaming**: Kafka → Bronze
2. **ML Models**: Fraud detection, claim prediction
3. **Advanced Analytics**: Cohort analysis, churn prediction
4. **Data Mesh**: Domain-oriented ownership
5. **Lakehouse Federation**: Query external sources

---

## References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Delta Lake Documentation](https://docs.delta.io/)

---

## Summary

The P&C Insurance Medallion Architecture provides:

✅ **Complete Data Pipeline**: Bronze → Silver → Gold with metadata-driven transformations  
✅ **Multi-Agent System**: 8 specialized AI tools (7 subagents + 1 MCP server)  
✅ **Metadata-Driven**: No hardcoded logic, all configuration-based  
✅ **Data Quality**: Built-in validation, reconciliation, and audit logging  
✅ **Unity Catalog Governance**: Centralized security and data governance  
✅ **Production-Ready**: CI/CD, monitoring, alerting, and rollback procedures

---

**Document Version**: 3.0  
**Last Updated**: 2026-09-25  
**Maintained By**: Data Engineering Team
