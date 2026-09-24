# P&C Insurance Medallion Architecture
## Complete Architecture Guide with Multi-Agent System

**Version:** 2.0  
**Last Updated:** 2026-09-24  
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

---

## Overview

The P&C Insurance Medallion Architecture is a comprehensive data platform built on Databricks that implements:

- **Medallion Architecture**: Bronze → Silver → Gold layers
- **Multi-Agent System**: 7 specialized AI agents for different domains
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

**Implementation**: `Bronze_Pipeline.py`

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

**Implementation**: `Silver_Pipeline_Metadata.py`

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

**Implementation**: `Gold_Pipeline.py`

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

**Capabilities**:
- Natural language understanding
- Agent routing logic
- Response synthesis
- Error handling
- Logging and audit

**Example Interactions**:
```python
# User asks: "What is our loss ratio by line of business?"
# Supervisor routes to: Analyst Agent
# Analyst queries: pc_insurance.gold.loss_ratio_by_lob
# Supervisor returns: Formatted business metrics
```

---

### 2. Architect Agent

**Role**: Designs data architecture, schemas, and data flow

**Responsibilities**:
- ✅ Design Bronze/Silver/Gold layer schemas
- ✅ Define table structures and relationships
- ✅ Plan data flow topology
- ✅ Design Unity Catalog governance model
- ✅ Define partitioning and optimization strategies
- ✅ Create architecture documentation

**Implementation**: `Architect_Agent.py`

**Capabilities**:
- Schema design
- Data modeling (star schema, snowflake)
- SCD Type 2 design
- Partitioning strategy
- Performance optimization
- Documentation generation

**Example Interactions**:
```python
# User asks: "Design a schema for policy renewals"
# Architect responds with:
# - Table structure (columns, types, constraints)
# - Partitioning strategy (by effective_date)
# - SCD2 tracking fields
# - Relationships to other tables
# - Sample DDL
```

**Key Decisions**:
- Bronze: Raw data with minimal transformation
- Silver: SCD2 for dimensions, facts for transactions
- Gold: Pre-aggregated metrics by business grain
- Partitioning: By date for facts, by business key for dimensions

---

### 3. Data Engineer Agent

**Role**: Implements pipelines, transformations, and data quality checks

**Responsibilities**:
- ✅ Write Spark/SQL transformation code
- ✅ Implement Bronze → Silver → Gold pipelines
- ✅ Create metadata-driven frameworks
- ✅ Implement SCD Type 2 logic
- ✅ Write data quality expectations
- ✅ Optimize pipeline performance

**Implementation**: `Data_Engineer_Agent.py`

**Capabilities**:
- PySpark code generation
- SQL transformation logic
- Delta Lake operations (MERGE, OPTIMIZE, VACUUM)
- SCD2 implementation
- Data quality checks
- Performance tuning

**Example Interactions**:
```python
# User asks: "Implement Silver MERGE for policy_dim with SCD2"
# Data Engineer generates:
# - MERGE statement with SCD2 logic
# - Deduplication logic
# - Audit logging
# - Reconciliation checks
```

**Key Implementations**:
- Metadata-driven Silver pipeline
- Metadata-driven Gold pipeline
- SCD2 MERGE logic
- Data quality framework
- Reconciliation framework

---

### 4. Domain Expert Agent

**Role**: Provides P&C insurance domain knowledge and business context

**Responsibilities**:
- ✅ Answer questions about P&C insurance concepts
- ✅ Explain loss ratios, combined ratios, frequency/severity
- ✅ Provide context on policy lifecycle
- ✅ Explain claims processing and reserving
- ✅ Clarify underwriting principles
- ✅ Interpret NAIC requirements

**Implementation**: `Domain_Expert_Agent.py`

**Data Source**: Unity Catalog Volume `pc_insurance.reference.pc_domain_docs`

**Capabilities**:
- Knowledge base search (keyword-based)
- Topic-specific queries
- Concept explanations
- Business rule clarification
- Regulatory guidance

**Example Interactions**:
```python
# User asks: "What is a loss ratio and how is it calculated?"
# Domain Expert responds:
# - Definition: Loss Ratio = Incurred Loss / Earned Premium
# - Interpretation: Percentage of premium paid out in claims
# - Benchmark: < 0.70 is good, > 1.00 indicates underwriting loss
# - Sources: P&C domain documents
```

**Topics Covered**:
- Loss ratios and combined ratios
- Claim frequency and severity
- Policy lifecycle (new, renewal, endorsement, cancellation)
- Claims processing and reserving
- Underwriting and risk assessment
- Retention and renewal patterns
- NAIC requirements

---

### 5. Analyst Agent

**Role**: Answers business questions by querying Gold layer KPIs

**Responsibilities**:
- ✅ Query Gold layer tables for business metrics
- ✅ Calculate loss ratios, frequencies, retention rates
- ✅ Provide trend analysis
- ✅ Generate business reports
- ✅ Answer "what is our..." questions

**Implementation**: `Analyst_Genie_Agent.py`

**Data Sources**:
- `pc_insurance.gold.loss_ratio_by_lob`
- `pc_insurance.gold.claim_frequency_severity`
- `pc_insurance.gold.retention_by_agent`
- `pc_insurance.gold.premium_growth`
- `pc_insurance.gold.exposure_summary`
- `pc_insurance.gold.uw_dashboard_summary`

**Capabilities**:
- SQL query generation
- Natural language to SQL
- Metric calculation
- Trend analysis
- Data visualization support

**Example Interactions**:
```python
# User asks: "What is our loss ratio by line of business?"
# Analyst queries:
SELECT line_of_business, loss_ratio, claim_count, total_incurred_loss
FROM pc_insurance.gold.loss_ratio_by_lob
ORDER BY loss_ratio DESC;

# Returns formatted business metrics with interpretation
```

**Key Metrics**:
- Loss Ratio = Incurred Loss / Earned Premium
- Claim Frequency = Claims / Policies
- Claim Severity = Incurred Loss / Claims
- Retention Rate = Renewed Policies / Total Policies
- Premium Growth = (Current - Previous) / Previous

---

### 6. DevOps Agent

**Role**: Provides guidance on Git, CI/CD, and deployment

**Responsibilities**:
- ✅ Advise on Git workflows and branching strategies
- ✅ Guide CI/CD pipeline setup
- ✅ Explain Databricks Asset Bundles (DAB)
- ✅ Provide deployment best practices
- ✅ Troubleshoot Git issues

**Implementation**: `DevOps_Agent.py`

**Capabilities**:
- Git workflow guidance
- Branch management advice
- CI/CD pipeline design
- DAB configuration help
- Deployment troubleshooting

**Example Interactions**:
```python
# User asks: "How do I set up a feature branch?"
# DevOps responds:
# 1. Create branch: git checkout -b feature/new-metric
# 2. Make changes and commit
# 3. Push: git push origin feature/new-metric
# 4. Create pull request
# 5. Merge after review
```

**Key Topics**:
- Git workflows (feature branches, main, releases)
- CI/CD with GitHub Actions / Azure DevOps
- Databricks Asset Bundles (DAB)
- Environment promotion (dev → staging → prod)
- Rollback procedures

---

### 7. QA Validator Agent

**Role**: Validates data quality and runs validation checks

**Responsibilities**:
- ✅ Run data quality checks on tables
- ✅ Validate data completeness, validity, consistency
- ✅ Check business rule compliance
- ✅ Generate DQ reports
- ✅ Identify data issues

**Implementation**: Via `run_dq_checks()` function

**Capabilities**:
- Completeness checks (NOT NULL)
- Validity checks (status codes, date ranges)
- Consistency checks (referential integrity)
- Accuracy checks (business rules)
- Timeliness checks (data freshness)

**Example Interactions**:
```python
# User asks: "Validate the policy_dim table"
# QA runs checks:
# - NOT NULL on required fields: PASS (100%)
# - Valid policy_status codes: PASS (99.8%)
# - Valid date ranges: PASS (100%)
# - Referential integrity: PASS (99.5%)
# Overall DQ Score: 99.8%
```

**DQ Checks**:
- Completeness: Required fields not null
- Validity: Status codes in valid list
- Consistency: Foreign keys exist
- Accuracy: Loss ratio <= 2.0
- Timeliness: Data < 24 hours old

---

## Metadata-Driven Framework

### Silver Layer Metadata

**Configuration Table**: `pc_insurance.reference.silver_transformation_config`

**Defines**:
- Source and target tables
- Transformation type (FULL_LOAD, INCREMENTAL, SCD2)
- Business keys for deduplication
- Column mappings (source → target)
- Cleansing rules (UPPER, TRIM, COALESCE)
- Deduplication strategy (LATEST, FIRST)
- SCD2 enablement
- Execution order

**Example Configuration**:
```sql
INSERT INTO silver_transformation_config VALUES (
  'SILVER_POLICY_DIM',           -- transformation_id
  'Policy Dimension',            -- transformation_name
  'bronze',                      -- source_schema
  'policies_raw',                -- source_table
  'silver',                      -- target_schema
  'policy_dim',                  -- target_table
  'SCD2',                        -- transformation_type
  ARRAY('policy_id'),            -- business_keys
  MAP('policy_status', 'UPPER(TRIM(policy_status))'), -- cleansing_rules
  'LATEST',                      -- dedup_strategy
  'ingestion_timestamp',         -- dedup_order_column
  TRUE,                          -- scd2_enabled
  1,                             -- execution_order
  TRUE                           -- is_active
);
```

**Audit Table**: `pc_insurance.reference.silver_load_audit`

Tracks:
- Transformation ID
- Run timestamp
- Status (SUCCESS, FAILED)
- Source and target row counts
- Execution time
- Error messages

**Reconciliation Table**: `pc_insurance.reference.silver_reconciliation`

Validates:
- Source vs target counts
- Count match status
- Reconciliation notes

### Gold Layer Metadata

**Configuration Table**: `pc_insurance.reference.gold_metric_config`

**Defines**:
- Metric name and category
- Target table
- Source tables
- Dimension columns (GROUP BY)
- Measure columns (aggregations)
- Calculation logic (formulas)
- Aggregation grain
- Refresh frequency
- Execution order

**Example Configuration**:
```sql
INSERT INTO gold_metric_config VALUES (
  'GOLD_LOSS_RATIO_LOB',         -- metric_id
  'Loss Ratio by Line of Business', -- metric_name
  'LOSS_RATIO',                  -- metric_category
  'gold',                        -- target_schema
  'loss_ratio_by_lob',           -- target_table
  ARRAY('silver.policy_dim', 'silver.claim_fact'), -- source_tables
  ARRAY('line_of_business'),     -- dimension_columns
  ARRAY('total_incurred_loss', 'total_earned_premium', 'loss_ratio'), -- measures
  'SUM(incurred_loss) / SUM(earned_premium) AS loss_ratio', -- calculation_logic
  'LINE_OF_BUSINESS',            -- aggregation_grain
  'DAILY',                       -- refresh_frequency
  1,                             -- execution_order
  TRUE                           -- is_active
);
```

**Audit Table**: `pc_insurance.reference.gold_refresh_audit`

Tracks:
- Metric ID
- Refresh timestamp
- Status (SUCCESS, FAILED)
- Row counts
- Metric values
- Data quality scores
- Execution time

---

## Data Flow

### End-to-End Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      SOURCE SYSTEMS                              │
│  Policy Admin │ Claims System │ Billing │ CRM │ Agent Admin     │
└────────┬────────────┬───────────┬───────┬───────────┬───────────┘
         │            │           │       │           │
         ▼            ▼           ▼       ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (Raw)                             │
│  Bronze_Pipeline.py                                              │
│  • Schema enforcement                                            │
│  • Append with ingestion_timestamp                               │
│  • Delta Lake format                                             │
└────────┬────────────┬───────────┬───────┬───────────┬───────────┘
         │            │           │       │           │
         ▼            ▼           ▼       ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│              SILVER LAYER (Cleansed & Conformed)                 │
│  Silver_Pipeline_Metadata.py                                     │
│  • Metadata-driven transformations                               │
│  • SCD Type 2 historization                                      │
│  • Data cleansing & standardization                              │
│  • PII masking                                                   │
│  • Deduplication                                                 │
│  • Audit logging                                                 │
│  • Reconciliation                                                │
└────────┬────────────┬───────────┬───────┬───────────┬───────────┘
         │            │           │       │           │
         ▼            ▼           ▼       ▼           ▼
┌─────────────────────────────────────────────────────────────────┐
│                 GOLD LAYER (Business KPIs)                       │
│  Gold_Pipeline.py                                                │
│  • Metadata-driven aggregations                                  │
│  • Pre-calculated business metrics                               │
│  • Optimized for BI tools                                        │
│  • Audit logging                                                 │
│  • Data quality tracking                                         │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   CONSUMPTION LAYER                              │
│  • BI Dashboards (Power BI, Tableau)                            │
│  • Analyst Agent queries                                         │
│  • Ad-hoc analysis                                               │
│  • ML models                                                     │
└─────────────────────────────────────────────────────────────────┘
```

### Agent Interaction Flow

```
User Request
     │
     ▼
┌─────────────────┐
│  Supervisor     │ ◄─── Routes to appropriate agent(s)
│     Agent       │
└────────┬────────┘
         │
    ┌────┴────┬────────┬─────────┬──────────┬─────────┐
    ▼         ▼        ▼         ▼          ▼         ▼
┌────────┐ ┌──────┐ ┌────────┐ ┌────────┐ ┌──────┐ ┌────┐
│Architect│ │ Data │ │ Domain │ │Analyst │ │DevOps│ │ QA │
│        │ │Engr  │ │ Expert │ │        │ │      │ │    │
└────┬───┘ └───┬──┘ └───┬────┘ └───┬────┘ └──┬───┘ └─┬──┘
     │         │        │          │         │       │
     └─────────┴────────┴──────────┴─────────┴───────┘
                         │
                         ▼
                 ┌───────────────┐
                 │  Synthesized  │
                 │   Response    │
                 └───────────────┘
```

---

## Unity Catalog Structure

### Catalog: `pc_insurance`

```
pc_insurance/
├── bronze/                    # Raw ingested data
│   ├── policies_raw
│   ├── claims_raw
│   ├── premiums_raw
│   ├── customers_raw
│   └── agents_raw
│
├── silver/                    # Cleansed & conformed
│   ├── policy_dim (SCD2)
│   ├── claim_dim (SCD2)
│   ├── customer_dim (SCD2)
│   ├── agent_dim (SCD2)
│   ├── date_dim
│   ├── premium_fact
│   └── claim_fact
│
├── gold/                      # Business KPIs
│   ├── loss_ratio_by_lob
│   ├── claim_frequency_severity
│   ├── retention_by_agent
│   ├── premium_growth
│   ├── exposure_summary
│   └── uw_dashboard_summary
│
├── reference/                 # Metadata & configuration
│   ├── silver_transformation_config
│   ├── silver_load_audit
│   ├── silver_reconciliation
│   ├── gold_metric_config
│   ├── gold_refresh_audit
│   ├── project_documentation
│   ├── git_sync_audit
│   └── pc_domain_docs/ (volume)
│
└── dq/                        # Data quality
    └── dq_validation_results
```

### Security Model

**Catalog-Level**:
- `USE CATALOG` granted to all users
- `CREATE SCHEMA` restricted to admins

**Schema-Level**:
- `USE SCHEMA` granted to all users
- `CREATE TABLE` granted to service principals and engineers

**Table-Level**:
- `SELECT` granted to analysts and BI tools
- `MODIFY` granted to service principals (pipelines)
- Row-level security for sensitive data (future)

**Volume-Level**:
- `READ VOLUME` granted to Domain Expert Agent
- `WRITE VOLUME` granted to admins only

---

## Pipeline Orchestration

### Job Configuration

**Job Name**: `PC_Insurance_MultiAgent_Pipeline`  
**Job ID**: `820361677269451`  
**Schedule**: Daily at 2:00 AM UTC

**Tasks**:
1. **Bronze_Pipeline** (15 min)
   - Ingest data from source systems
   - Write to Bronze tables
   - No dependencies

2. **Silver_Pipeline_Metadata** (30 min)
   - Depends on: Bronze_Pipeline
   - Read metadata configuration
   - Execute transformations in order
   - Apply SCD2 logic
   - Write audit logs
   - Run reconciliation

3. **Gold_Pipeline** (15 min)
   - Depends on: Silver_Pipeline_Metadata
   - Read metric configuration
   - Execute aggregations in order
   - Calculate business metrics
   - Write audit logs
   - Track DQ scores

**Cluster Configuration**:
- Runtime: 13.3 LTS
- Workers: 4-8 nodes (autoscaling)
- Node Type: Standard_DS4_v2 (Azure) / m5.2xlarge (AWS)

**Notifications**:
- On Failure: Email to data-engineering-team@company.com
- On Success: (optional) Email to stakeholders

### Orchestrator

**Notebook**: `Orchestrator.py`

**Capabilities**:
- Execute pipelines in order
- Handle dependencies
- Retry logic on failure
- Parallel execution where possible
- Comprehensive logging

**Execution Framework**:
- `execution/orchestrator.py` - Main orchestration logic
- `execution/plan_executor.py` - Plan execution engine
- `execution/example_plan.json` - Sample execution plan

---

## Deployment Architecture

### Environments

**Development**:
- Catalog: `pc_insurance_dev`
- Cluster: Shared all-purpose cluster
- Git Branch: `feature/*` or `dev`
- Purpose: Development and testing

**Staging**:
- Catalog: `pc_insurance_staging`
- Cluster: Dedicated job cluster
- Git Branch: `staging`
- Purpose: Pre-production validation

**Production**:
- Catalog: `pc_insurance`
- Cluster: Dedicated job cluster
- Git Branch: `main`
- Purpose: Production workloads

### Databricks Asset Bundles (DAB)

**Configuration**: `databricks.yml`

```yaml
bundle:
  name: pc-insurance-medallion

resources:
  jobs:
    pc_insurance_pipeline:
      name: PC_Insurance_MultiAgent_Pipeline
      tasks:
        - task_key: bronze_pipeline
          notebook_task:
            notebook_path: ./Bronze_Pipeline
        - task_key: silver_pipeline
          depends_on:
            - task_key: bronze_pipeline
          notebook_task:
            notebook_path: ./Silver_Pipeline_Metadata
        - task_key: gold_pipeline
          depends_on:
            - task_key: silver_pipeline
          notebook_task:
            notebook_path: ./Gold_Pipeline

targets:
  dev:
    mode: development
    workspace:
      host: https://your-workspace.cloud.databricks.com
  prod:
    mode: production
    workspace:
      host: https://your-workspace.cloud.databricks.com
```

**Deployment Commands**:
```bash
# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### CI/CD Pipeline

**Workflow**: `resources/git_auto_push_workflow.yml`

**Stages**:
1. **Lint & Validate** - Check code quality
2. **Unit Tests** - Run unit tests
3. **DQ Checks** - Validate data quality
4. **Deploy to Dev** - Deploy to dev environment
5. **Integration Tests** - Run end-to-end tests
6. **Deploy to Staging** - Deploy to staging
7. **Approval Gate** - Manual approval required
8. **Deploy to Production** - Deploy to production

---

## Operations & Monitoring

### Daily Operations

**Morning Checklist**:
1. Check pipeline status (last 24 hours)
2. Verify data freshness (Bronze ingestion)
3. Review DQ scores (> 95% target)
4. Check reconciliation status (all PASS)
5. Review error logs (if any failures)

**Monitoring Queries**:
```sql
-- Pipeline status
SELECT transformation_id, status, execution_time_seconds
FROM pc_insurance.reference.silver_load_audit
WHERE DATE(run_timestamp) = CURRENT_DATE()
ORDER BY run_timestamp DESC;

-- DQ scores
SELECT table_name, 
       AVG(CASE WHEN validation_result = 'PASS' THEN 1.0 ELSE 0.0 END) AS dq_score
FROM pc_insurance.dq.dq_validation_results
WHERE DATE(validation_timestamp) = CURRENT_DATE()
GROUP BY table_name;

-- Reconciliation status
SELECT transformation_id, recon_status, count_diff
FROM pc_insurance.reference.silver_reconciliation
WHERE DATE(recon_timestamp) = CURRENT_DATE()
AND recon_status != 'PASS';
```

### Alerts

**SQL Alerts**:
1. **DQ Alert** - Triggers when DQ score < 95%
2. **Pipeline Failure Alert** - Triggers on any FAILED status
3. **Reconciliation Alert** - Triggers on count mismatches
4. **Data Freshness Alert** - Triggers if no data in 24 hours

**Notification Channels**:
- Email: data-engineering-team@company.com
- Slack: #data-alerts channel
- PagerDuty: For critical P1 incidents

### Dashboard

**Widgets**:
1. **Pipeline Health** - Success/failure rates
2. **Execution Time Trends** - Performance over time
3. **Data Quality Scores** - DQ metrics by table
4. **Row Count Trends** - Data volume trends
5. **Business Metrics** - Key KPIs from Gold layer

---

## Agent Usage Examples

### Example 1: Architecture Question

**User**: "How should I design a new claims adjustment table?"

**Supervisor** routes to **Architect Agent**

**Architect** responds:
```
Table: claim_adjustment_dim (SCD2)

Columns:
- adjustment_key BIGINT (surrogate key)
- adjustment_id STRING (business key)
- claim_id STRING (FK to claim_dim)
- adjustment_date DATE
- adjustment_type STRING (RESERVE_INCREASE, RESERVE_DECREASE, PAYMENT)
- adjustment_amount DECIMAL(12,2)
- adjuster_id STRING
- adjustment_reason STRING
- is_current BOOLEAN (SCD2)
- effective_from TIMESTAMP (SCD2)
- effective_to TIMESTAMP (SCD2)

Partitioning: BY adjustment_date
SCD2: Track changes to adjustment amounts and reasons
```

### Example 2: Implementation Question

**User**: "Write the Silver MERGE for claim_adjustment_dim"

**Supervisor** routes to **Data Engineer Agent**

**Data Engineer** generates:
```sql
MERGE INTO pc_insurance.silver.claim_adjustment_dim AS target
USING (
  SELECT 
    adjustment_id,
    claim_id,
    adjustment_date,
    UPPER(TRIM(adjustment_type)) AS adjustment_type,
    COALESCE(adjustment_amount, 0) AS adjustment_amount,
    adjuster_id,
    adjustment_reason,
    ROW_NUMBER() OVER (PARTITION BY adjustment_id ORDER BY ingestion_timestamp DESC) AS rn
  FROM pc_insurance.bronze.claim_adjustments_raw
  WHERE rn = 1
) AS source
ON target.adjustment_id = source.adjustment_id AND target.is_current = TRUE
WHEN MATCHED AND (
  target.adjustment_amount != source.adjustment_amount OR
  target.adjustment_reason != source.adjustment_reason
) THEN UPDATE SET
  is_current = FALSE,
  effective_to = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  adjustment_id, claim_id, adjustment_date, adjustment_type,
  adjustment_amount, adjuster_id, adjustment_reason,
  is_current, effective_from, effective_to
) VALUES (
  source.adjustment_id, source.claim_id, source.adjustment_date,
  source.adjustment_type, source.adjustment_amount, source.adjuster_id,
  source.adjustment_reason, TRUE, CURRENT_TIMESTAMP(), NULL
);
```

### Example 3: Domain Question

**User**: "What is IBNR and how is it calculated?"

**Supervisor** routes to **Domain Expert Agent**

**Domain Expert** responds:
```
IBNR (Incurred But Not Reported) is a reserve for claims that have occurred 
but have not yet been reported to the insurance company.

Calculation Methods:
1. Loss Development Method: Apply development factors to known losses
2. Expected Loss Ratio Method: Premium × Expected Loss Ratio - Reported Losses
3. Bornhuetter-Ferguson Method: Combination of the above two methods

Formula (Expected Loss Ratio):
IBNR = (Earned Premium × Expected Loss Ratio) - (Paid Losses + Case Reserves)

Example:
- Earned Premium: $10,000,000
- Expected Loss Ratio: 0.65
- Paid Losses: $4,000,000
- Case Reserves: $2,000,000

IBNR = ($10M × 0.65) - ($4M + $2M) = $6.5M - $6M = $500,000

Sources: P&C domain documents, actuarial standards
```

### Example 4: Business Question

**User**: "What is our loss ratio by line of business?"

**Supervisor** routes to **Analyst Agent**

**Analyst** queries and responds:
```
Loss Ratio by Line of Business (as of 2026-09-24):

Line of Business | Loss Ratio | Claim Count | Total Incurred Loss | Total Earned Premium
----------------|------------|-------------|---------------------|---------------------
Workers Comp    | 0.85       | 45          | $850,000           | $1,000,000
Auto            | 0.72       | 120         | $1,440,000         | $2,000,000
Property        | 0.68       | 80          | $1,020,000         | $1,500,000
Liability       | 0.55       | 55          | $550,000           | $1,000,000

Interpretation:
- Workers Comp has the highest loss ratio (0.85) - requires attention
- Liability has the best loss ratio (0.55) - profitable line
- Overall portfolio loss ratio: 0.70 (within acceptable range)

Source: pc_insurance.gold.loss_ratio_by_lob
```

### Example 5: DevOps Question

**User**: "How do I deploy changes to production?"

**Supervisor** routes to **DevOps Agent**

**DevOps** responds:
```
Production Deployment Process:

1. Create feature branch:
   git checkout -b feature/new-metric

2. Make changes and commit:
   git add .
   git commit -m "feat: Add new metric"

3. Push to remote:
   git push origin feature/new-metric

4. Create pull request to staging branch

5. After approval, merge to staging:
   git checkout staging
   git merge feature/new-metric

6. Deploy to staging:
   databricks bundle deploy -t staging

7. Run validation tests in staging

8. Create pull request to main

9. After approval, merge to main:
   git checkout main
   git merge staging

10. Deploy to production:
    databricks bundle deploy -t prod

11. Monitor production deployment

Best Practices:
- Always test in dev first
- Run full validation in staging
- Deploy during low-traffic windows
- Have rollback plan ready
- Monitor for 24 hours post-deployment
```

---

## Summary

The P&C Insurance Medallion Architecture provides:

✅ **Complete Data Pipeline**: Bronze → Silver → Gold with metadata-driven transformations  
✅ **Multi-Agent System**: 7 specialized AI agents for different domains  
✅ **Metadata-Driven**: No hardcoded logic, all configuration-based  
✅ **Data Quality**: Built-in validation, reconciliation, and audit logging  
✅ **Unity Catalog Governance**: Centralized security and data governance  
✅ **Production-Ready**: CI/CD, monitoring, alerting, and rollback procedures  

**Key Success Factors**:
- Metadata-driven design enables rapid changes without code modifications
- Multi-agent system provides specialized expertise for different tasks
- SCD Type 2 preserves historical data for compliance and analysis
- Comprehensive audit logging enables troubleshooting and compliance
- Delta Lake provides ACID transactions and time travel capabilities

**Next Steps**:
1. Follow `docs/DEPLOYMENT.md` for step-by-step deployment
2. Execute SQL scripts to create metadata tables
3. Test agent notebooks
4. Run end-to-end pipeline
5. Set up monitoring and alerts
6. Deploy to production

---

**Document Version**: 2.0  
**Last Updated**: 2026-09-24  
**Maintained By**: Data Engineering Team  
**Next Review**: 2026-12-24
