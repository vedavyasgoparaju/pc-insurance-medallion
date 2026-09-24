# P&C Insurance Medallion - Deployment & Getting Started Guide

## Overview

This document provides comprehensive setup, deployment, and getting-started instructions for the P&C Insurance Medallion data platform on Databricks. It covers prerequisites, environment setup, step-by-step deployment, MCP app configuration, post-deployment validation, and troubleshooting.

For architecture details, see [ARCHITECTURE.md](ARCHITECTURE.md). For ongoing operations, see [RUNBOOK.md](RUNBOOK.md). For column-level definitions, see [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Project Structure](#project-structure)
3. [Unity Catalog Setup](#unity-catalog-setup)
4. [Deployment Steps](#deployment-steps)
5. [Running the Pipelines](#running-the-pipelines)
6. [Data Quality Validation](#data-quality-validation)
7. [Phase 5: Multi-Agent System Deployment](#phase-5-multi-agent-system-deployment)
8. [Git Automation via MCP App](#git-automation-via-mcp-app)
9. [Post-Deployment Validation](#post-deployment-validation)
10. [Rollback Procedures](#rollback-procedures)
11. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Databricks Workspace
- Databricks workspace (AWS, Azure, or GCP) with serverless compute enabled
- Unity Catalog enabled
- Model Serving access (for multi-agent system)
- Databricks Runtime 13.3 LTS or higher

### Permissions Required
- Workspace Admin (for initial setup)
- Unity Catalog Admin (for catalog/schema creation)
- Cluster creation permissions
- Git repository access

### Tools
- **Databricks CLI** installed locally (for CI/CD)
- **GitHub account** with access to the project repo

**GitHub Repo**: https://github.com/vedavyasgoparaju/pc-insurance-medallion

---

## Project Structure

```
pc-insurance-medallion/
├── pipelines/
│   ├── Bronze_Pipeline.py          # Data generation & ingestion (5 Bronze tables)
│   ├── Silver_Pipeline_Metadata.py # SCD2, PII masking, DQ checks (7 Silver tables)
│   ├── Gold_Pipeline.py            # KPI aggregations (6 Gold tables)
│   └── Orchestrator.py             # Master pipeline orchestrator
├── agents/
│   ├── Architect_Agent.py           # MLflow agent for architecture design
│   ├── Data_Engineer_Agent.py       # MLflow agent for pipeline code
│   ├── DevOps_Agent.py             # DevOps agent (MLflow)
│   ├── Domain_Expert_Agent.py      # Domain expert agent setup
│   ├── Domain_Expert_Setup.py       # UC volume with P&C reference docs
│   ├── Analyst_Genie_Agent.py       # Analyst agent setup
│   ├── Analyst_Genie_Setup.py       # Genie Space over Gold tables
│   └── Supervisor_Agent_Setup.py    # Multi-agent orchestration setup
├── app/
│   ├── app.py                      # MCP server (pc-insurance-workspace-actions)
│   ├── app.yaml                    # Databricks App config
│   └── requirements.txt            # Python dependencies
├── execution/
│   ├── orchestrator.py             # MCP server action orchestration
│   └── plan_executor.py            # MCP server action execution
├── sql/
│   ├── 01_catalog_schemas.sql      # Catalog and schema creation
│   ├── 02_bronze_tables.sql        # Bronze table DDL
│   ├── 03_silver_transformation_config.sql  # Silver transformation metadata
│   ├── 04_gold_tables.sql          # Gold table DDL
│   └── 05_gold_metric_config.sql   # Gold metric configuration
├── utils/
│   └── common_utils.py             # Shared utility functions
├── docs/
│   ├── ARCHITECTURE.md             # Architecture documentation
│   ├── DATA_DICTIONARY.md          # Column-level data dictionary
│   ├── DEPLOYMENT.md               # This file
│   └── RUNBOOK.md                  # Operations runbook
├── README.md                        # Project overview
├── databricks.yml                   # DAB bundle config
├── pyproject.toml                  # Python project config
└── .gitignore
```

---

## Unity Catalog Setup

The project uses a single catalog with five schemas:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS pc_insurance
COMMENT 'P&C Insurance Medallion Architecture';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS pc_insurance.bronze;
CREATE SCHEMA IF NOT EXISTS pc_insurance.silver;
CREATE SCHEMA IF NOT EXISTS pc_insurance.gold;
CREATE SCHEMA IF NOT EXISTS pc_insurance.reference;
CREATE SCHEMA IF NOT EXISTS pc_insurance.dq;

-- Create volume for P&C domain documents
CREATE VOLUME IF NOT EXISTS pc_insurance.reference.pc_domain_docs;

-- Grant permissions (replace with your service principal)
GRANT USE CATALOG ON CATALOG pc_insurance TO `your-service-principal`;
GRANT CREATE TABLE ON SCHEMA pc_insurance.bronze TO `your-service-principal`;
-- Repeat for all schemas

-- Verify
SHOW SCHEMAS IN pc_insurance;
```

### Schema Purposes

| Schema | Purpose |
|---|---|
| `bronze` | Raw ingested data (Delta format, append mode) |
| `silver` | Conformed dimensions (SCD2) and fact tables |
| `gold` | Business KPI aggregations (quarterly grain) |
| `reference` | Documentation table, domain knowledge volume, metadata config |
| `dq` | Data quality validation functions and results |

---

## Deployment Steps

### Step 0: Clone Git Repository

**Using Databricks UI**:
1. Navigate to Workspace -> Repos
2. Click "Add Repo"
3. Enter Git URL: `https://github.com/vedavyasgoparaju/pc-insurance-medallion.git`
4. Select branch: `main`
5. Click "Create Repo"

### Phase 1: Database Schema Creation (15 minutes)

#### Step 1.1: Create Bronze Tables

```sql
-- Run SQL scripts in order
%run /Repos/your-username/pc-insurance-medallion/sql/01_catalog_schemas.sql
%run /Repos/your-username/pc-insurance-medallion/sql/02_bronze_tables.sql
```

**Validation**:
```sql
SHOW TABLES IN pc_insurance.bronze;
-- Expected: policies_raw, claims_raw, premiums_raw, customers_raw, agents_raw
```

#### Step 1.2: Create Silver Metadata Configuration

```sql
%run /Repos/your-username/pc-insurance-medallion/sql/03_silver_transformation_config.sql
```

**Validation**:
```sql
SELECT transformation_id, transformation_name, is_active
FROM pc_insurance.reference.silver_transformation_config
ORDER BY execution_order;
-- Expected: 6 active transformations
```

#### Step 1.3: Create Gold Tables and Metadata

```sql
%run /Repos/your-username/pc-insurance-medallion/sql/04_gold_tables.sql
%run /Repos/your-username/pc-insurance-medallion/sql/05_gold_metric_config.sql
```

**Validation**:
```sql
SELECT metric_id, metric_name, is_active
FROM pc_insurance.reference.gold_metric_config
ORDER BY execution_order;
-- Expected: 6 active metrics
```

**Note on Silver table DDL:** Silver layer tables are created dynamically by `pipelines/Silver_Pipeline_Metadata.py` based on the configuration in `silver_transformation_config`. The `sql/03_silver_transformation_config.sql` script populates the metadata config table; it does not contain static Silver table DDL.

### Phase 2: Agent Setup (20 minutes)

#### Step 2.1: Upload Domain Documents

```python
# Upload P&C domain documents to Unity Catalog volume
dbutils.fs.cp(
  "file:/path/to/local/domain_docs/",
  "/Volumes/pc_insurance/reference/pc_domain_docs/",
  recurse=True
)
```

#### Step 2.2: Test Agent Notebooks

```python
# Test Domain Expert Agent
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/agents/Domain_Expert_Agent",
  timeout_seconds=300
)
print(result)

# Test Analyst Agent
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/agents/Analyst_Genie_Agent",
  timeout_seconds=300
)
print(result)
```

### Phase 3: Pipeline Deployment (30 minutes)

#### Step 3.1: Run Bronze Pipeline

```python
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/pipelines/Bronze_Pipeline",
  timeout_seconds=1800
)
print(f"Bronze Pipeline Result: {result}")
```

**Validation**:
```sql
SELECT 'policies_raw' AS table_name, COUNT(*) AS row_count
FROM pc_insurance.bronze.policies_raw
UNION ALL
SELECT 'claims_raw', COUNT(*) FROM pc_insurance.bronze.claims_raw;
-- Expected: ~1000 policies, ~300 claims
```

#### Step 3.2: Run Silver Pipeline

```python
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/pipelines/Silver_Pipeline_Metadata",
  timeout_seconds=3600
)
print(f"Silver Pipeline Result: {result}")
```

**Validation**:
```sql
SELECT 'policy_dim' AS table_name, COUNT(*) AS row_count
FROM pc_insurance.silver.policy_dim WHERE is_current = TRUE;

SELECT transformation_id, status, execution_time_seconds
FROM pc_insurance.reference.silver_load_audit
ORDER BY run_timestamp DESC LIMIT 10;
-- All should have status = 'SUCCESS'
```

#### Step 3.3: Run Gold Pipeline

```python
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/pipelines/Gold_Pipeline",
  timeout_seconds=1800
)
print(f"Gold Pipeline Result: {result}")
```

**Validation**:
```sql
SELECT line_of_business, loss_ratio, claim_count
FROM pc_insurance.gold.loss_ratio_by_lob
ORDER BY loss_ratio DESC;
```

### Phase 4: Job Orchestration Setup (20 minutes)

#### Deploy Databricks Asset Bundle

```bash
cd /path/to/local/pc-insurance-medallion

# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

**Or Create Scheduled Job Manually**:
1. Navigate to Workflows -> Create Job
2. Add tasks: Bronze -> Silver -> Gold
3. Set schedule: Daily at 2:00 AM UTC
4. Configure notifications

**Job Name**: `PC_Insurance_MultiAgent_Pipeline`
**Job ID**: `820361677269451`

### Phase 4b: MCP App Deployment (10 minutes)

#### Deploy `pc-insurance-workspace-actions`

The MCP app (`app/app.py`) provides git commit, file write, and SQL execution capabilities to the Supervisor Agent.

```bash
# Deploy via Databricks CLI
databricks apps deploy pc-insurance-workspace-actions \
  --source-file app/app.py
```

Or via the Databricks UI:
1. Navigate to Apps -> Create App
2. Name: `pc-insurance-workspace-actions`
3. Source: Select `app/app.py` from repo
4. Click "Deploy"

```bash
# Check app status
databricks apps get pc-insurance-workspace-actions

# View app logs
databricks apps logs pc-insurance-workspace-actions
```

---

## Running the Pipelines

### Quick Start (All Pipelines)

The orchestration job runs all pipelines sequentially:

```python
# Run full orchestrated pipeline
dbutils.notebook.run(
  "/Repos/vedavyas.goparaju/pc-insurance-medallion/pipelines/Orchestrator",
  timeout_seconds=10800,
  arguments={
    "run_mode": "full",
    "layers": "bronze,silver,gold"
  }
)
```

### Bronze Pipeline Widget Parameters

- `load_type`: `INITIAL` or `INCREMENTAL` (default: `INCREMENTAL`)
- `source_filter`: comma-separated source names (empty = all sources)

### Silver Pipeline Widget Parameters

- `load_type`: `INITIAL` or `INCREMENTAL` (default: `INCREMENTAL`)
- `transformation_filter`: comma-separated transformation names (empty = all)

### Gold Pipeline

Outputs 6 KPI tables (all quarterly grain by line of business):

| Table | KPI |
|---|---|
| `loss_ratio_by_lob` | Loss Ratio, Combined Ratio |
| `claim_frequency_severity` | Claim Frequency, Claim Severity |
| `retention_by_agent` | Retention Rate, New Business Growth |
| `premium_growth` | Premium Growth Rate |
| `exposure_summary` | Total Exposure, Earned Premium |
| `uw_dashboard_summary` | Combined Underwriting Dashboard |

---

## Data Quality Validation

The project includes 7 DQ functions in `pc_insurance.dq`:

| Function | Purpose |
|---|---|
| `check_policy_exists` | Validates that claim references exist in policies |
| `check_claim_status` | Validates claim status values are in allowed set |
| `check_premium_positive` | Validates premium amounts are positive |
| `check_loss_ratio` | Validates loss ratio is within reasonable range |
| `check_not_null` | Validates required columns are not null |
| `check_date_order` | Validates date chronology (effective_from < effective_to) |
| `calculate_dq_score` | Runs all checks and returns overall score (0-100) |

### Running DQ Checks

```sql
-- Calculate overall DQ score for the policies table
SELECT pc_insurance.dq.calculate_dq_score(
  'pc_insurance.silver.policy_dim',
  array('policy_id', 'customer_id', 'agent_id', 'policy_status')
);
-- Score > 90 = excellent, 70-90 = acceptable, < 70 = needs attention
```

---

## Phase 5: Multi-Agent System Deployment (2-3 hours)

The project includes a **Supervisor Agent** that orchestrates 8 specialized tools (7 subagents + 1 MCP server). This section covers the complete setup from scratch in a new environment.

### Agent Overview

| Agent | Type | Role | Resource |
|---|---|---|---|
| **Architect** | Serving Endpoint | Designs Bronze/Silver/Gold schemas, data flow topology | MLflow model `pc_architect_agent` |
| **Data Engineer** | Serving Endpoint | Generates SDP code, SQL transformations, DQ expectations | MLflow model `pc_data_engineer_agent` |
| **P&C Domain Expert** | Knowledge Assistant | Answers insurance domain questions (policies, claims, underwriting) | UC Volume `pc_insurance.reference.pc_domain_docs` |
| **Analyst** | Genie Space | Queries Gold layer for KPIs (loss ratios, retention, frequency) | Gold tables in `pc_insurance.gold` |
| **QA Validator** | UC Function | Runs data quality validation checks | UC function `pc_insurance.dq.calculate_dq_score` |
| **Documentation** | Genie Space | Generates and updates technical documentation | Table `pc_insurance.reference.project_documentation` |
| **DevOps** | Genie Space | Provides Git/CI-CD GUIDANCE ONLY (no execution) | Same Genie Space as Documentation or separate |
| **Workspace-Actions** | MCP App | EXECUTES workspace changes (file writes, SQL, git commits) | Databricks App `pc-insurance-workspace-actions` |

### Execution Order

The setup must follow this exact order due to dependencies:

1. **DQ Functions** (no dependencies — only needs UC schema)
2. **Domain Expert** (needs UC volume + reference documents)
3. **Genie Spaces** (needs Gold tables + project_documentation table populated)
4. **MLflow Models + Serving Endpoints** (needs agent notebooks in repo)
5. **MCP App** (needs app code in repo + SQL warehouse)
6. **Supervisor Agent** (needs ALL of the above — registers them as tools)
7. **Validation** (needs Supervisor Agent endpoint)

---

### Step 1: Create DQ Functions as UC Functions (10 minutes)

The 7 DQ functions live in the `pc_insurance.dq` schema. They must be created **after** Phase 1 (UC setup) and **before** the Supervisor Agent (which registers `calculate_dq_score` as a tool).

Run the following SQL in a SQL warehouse or notebook:

```sql
USE CATALOG pc_insurance;
USE SCHEMA dq;

-- 1. check_not_null: Validates that required columns are not null
CREATE FUNCTION IF NOT EXISTS check_not_null(val STRING, column_name STRING)
RETURNS BOOLEAN
RETURN val IS NOT NULL
COMMENT 'DQ Check: Returns TRUE if the value is not null';

-- 2. check_claim_status: Validates claim status values are in allowed set
CREATE FUNCTION IF NOT EXISTS check_claim_status(claim_status_val STRING)
RETURNS BOOLEAN
RETURN claim_status_val IN ('Open', 'Closed', 'Reopened', 'Denied', 'Pending')
COMMENT 'DQ Check: Returns TRUE if the claim status is one of the valid values';

-- 3. check_premium_positive: Validates premium amounts are positive
CREATE FUNCTION IF NOT EXISTS check_premium_positive(premium_val DECIMAL)
RETURNS BOOLEAN
RETURN premium_val > 0
COMMENT 'DQ Check: Returns TRUE if premium amount is greater than zero';

-- 4. check_date_order: Validates date chronology (effective <= expiry)
CREATE FUNCTION IF NOT EXISTS check_date_order(effective_date_val DATE, expiry_date_val DATE)
RETURNS BOOLEAN
RETURN effective_date_val <= expiry_date_val
COMMENT 'DQ Check: Returns TRUE if effective_date is before or equal to expiry_date';

-- 5. check_policy_exists: Validates that claim references exist in policies
CREATE FUNCTION IF NOT EXISTS check_policy_exists(policy_id_val STRING)
RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM pc_insurance.bronze.policies_raw p
  WHERE p.policy_id = policy_id_val
)
COMMENT 'DQ Check: Returns TRUE if the given policy_id exists in the bronze.policies_raw table';

-- 6. check_loss_ratio: Validates loss ratio is within reasonable range
CREATE FUNCTION IF NOT EXISTS check_loss_ratio(
  earned_premium_val DECIMAL(15,2),
  incurred_losses_val DECIMAL(15,2)
)
RETURNS BOOLEAN
RETURN CASE
  WHEN earned_premium_val <= 0 THEN FALSE
  ELSE (incurred_losses_val / earned_premium_val) BETWEEN 0 AND 5.0
END
COMMENT 'DQ Check: Returns TRUE if loss ratio is between 0 and 5.0 (500%) - flags extreme outliers';

-- 7. calculate_dq_score: Runs all checks and returns overall score (0.0 to 1.0)
CREATE FUNCTION IF NOT EXISTS calculate_dq_score(total_records INT, failed_records INT)
RETURNS DECIMAL(10,4)
RETURN CASE
  WHEN total_records <= 0 THEN 0.0
  ELSE (total_records - failed_records) / CAST(total_records AS DECIMAL(10,4))
END
COMMENT 'DQ Score: Returns the percentage of records that passed validation (0.0 to 1.0)';
```

**Verify:**
```sql
USE CATALOG pc_insurance;
SHOW FUNCTIONS IN dq;
-- Expected: 7 functions listed
```

---

### Step 2: Domain Expert Setup — UC Volume + Knowledge Assistant (20 minutes)

Run the notebook `agents/Domain_Expert_Setup.py` or follow the steps below manually.

#### 2a. Create UC Volume for Domain Documents

```sql
CREATE VOLUME IF NOT EXISTS pc_insurance.reference.pc_domain_docs
  COMMENT 'Volume for P&C insurance domain reference documents for Knowledge Assistant';
```

#### 2b. Upload P&C Reference Document

Upload a P&C insurance reference document (PDF, TXT, or MD) to the volume. The document should cover:
- Policy lifecycle and underwriting guidelines
- Claims processing and reserving practices
- P&C insurance regulatory requirements (NAIC)
- Key insurance metrics and their definitions
- Lines of business terminology

Upload via UI (Catalog Explorer → Volumes → pc_domain_docs → Upload) or via SDK:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Upload the reference document
with open('/path/to/pc_insurance_reference.pdf', 'rb') as f:
    w.files.upload(
        "/Volumes/pc_insurance/reference/pc_domain_docs/pc_insurance_reference.pdf",
        f,
        overwrite=True
    )
print("✓ Reference document uploaded to UC Volume")
```

#### 2c. Create Knowledge Assistant

Create a **Knowledge Assistant** (Agent Bricks) that uses the volume as its knowledge source:

**Via UI:**
1. Go to **Agents** → **Create Agent** → **Knowledge Assistant**
2. Name: `pc_domain_expert`
3. Description: `P&C Insurance domain expert. Answers questions about policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements.`
4. Knowledge Source: UC Volume `pc_insurance.reference.pc_domain_docs`
5. Save and wait for indexing to complete (check status in the UI)

**Via SDK (if API available):**
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

try:
    ka = w.knowledge_assistants.create(
        name="pc_domain_expert",
        description="P&C Insurance domain expert. Answers questions about policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements.",
    )
    knowledge_assistant_id = ka.name
    print(f"✓ Knowledge Assistant created: {knowledge_assistant_id}")
except Exception as e:
    print(f"SDK not available: {e}")
    print("Create manually via UI (see above)")
    knowledge_assistant_id = "<replace-with-your-knowledge-assistant-id>"
```

**⚠️ Record the Knowledge Assistant ID — needed for Step 6 (Supervisor Agent setup).**

---

### Step 3: Create Genie Spaces (30 minutes)

Three Genie Spaces are needed: **Analyst** (required), **Documentation** (required), and **DevOps** (optional — can reuse Documentation space or create separate).

#### 3a. Add Rich Comments to Gold Tables (for Analyst Genie)

Run the notebook `agents/Analyst_Genie_Setup.py` or execute manually. This adds table and column comments that Genie uses for query routing:

```python
# Run in a Databricks notebook (Python cell)
gold_table_descriptions = {
    "loss_ratio_by_lob": "P&C insurance loss ratio, expense ratio, and combined ratio by line of business and reporting period. Contains earned premium, incurred losses, expense amounts, and calculated ratios. Use for questions about profitability, loss ratios, combined ratios by LOB.",
    "claim_frequency_severity": "P&C insurance claim frequency and severity metrics by line of business, state, and reporting period. Contains exposure units, claim count, frequency rate, average severity, and incurred losses. Use for questions about how often claims occur and average claim costs.",
    "retention_by_agent": "Policy retention and growth metrics by agent, agency, and reporting period. Contains total policies, renewed, cancelled, new business counts, retention rate, and new business growth. Use for agent performance and retention questions.",
    "premium_growth": "Premium growth by line of business and reporting period. Contains new business, renewal, endorsement, and cancelled premium amounts. Use for questions about premium trends and growth.",
    "exposure_summary": "Exposure summary by line of business, state, and reporting period. Contains total policies, active/cancelled counts, coverage limits, average premium, and earned exposure. Use for exposure and policy volume questions.",
    "uw_dashboard_summary": "Executive underwriting dashboard summary combining loss ratios, combined ratios, policy counts, and claim statistics by line of business and reporting period. Use for high-level executive KPI questions.",
}

for table_name, description in gold_table_descriptions.items():
    spark.sql(f"""
        COMMENT ON TABLE pc_insurance.gold.{table_name} IS '{description.replace("'", "''")}'
    """)
    print(f"✓ Commented: pc_insurance.gold.{table_name}")

# Key column comments
spark.sql("COMMENT ON COLUMN pc_insurance.gold.loss_ratio_by_lob.loss_ratio IS 'Loss Ratio = Incurred Losses / Earned Premium. Below 0.60 is good, above 0.70 is concerning.'")
spark.sql("COMMENT ON COLUMN pc_insurance.gold.loss_ratio_by_lob.combined_ratio IS 'Combined Ratio = (Incurred Losses + Expenses) / Earned Premium. Below 1.0 (100%) means underwriting profit.'")
spark.sql("COMMENT ON COLUMN pc_insurance.gold.claim_frequency_severity.claim_frequency IS 'Claim Frequency = Claim Count / Exposure Units.'")
spark.sql("COMMENT ON COLUMN pc_insurance.gold.claim_frequency_severity.claim_severity IS 'Claim Severity = Incurred Losses / Claim Count. Average cost per claim.'")
spark.sql("COMMENT ON COLUMN pc_insurance.gold.retention_by_agent.retention_rate IS 'Retention Rate = Renewed Policies / (Renewed + Cancelled). Target: >90% personal, >85% commercial.'")
print("✓ All Gold layer comments added for Genie")
```

#### 3b. Create Analyst Genie Space

**Via UI:**
1. Go to **Genie** → **Create Space**
2. Name: `PC_Insurance_Analyst`
3. Description: `P&C Insurance Analyst — Answers business questions about loss ratios, combined ratios, claim frequency/severity, retention rates, and premium growth from Gold layer tables.`
4. Add ALL tables from `pc_insurance.gold` schema:
   - `pc_insurance.gold.loss_ratio_by_lob`
   - `pc_insurance.gold.claim_frequency_severity`
   - `pc_insurance.gold.retention_by_agent`
   - `pc_insurance.gold.premium_growth`
   - `pc_insurance.gold.exposure_summary`
   - `pc_insurance.gold.uw_dashboard_summary`
5. Add example queries (optional but recommended):
   - Q: "What is our loss ratio by line of business?" → `SELECT line_of_business, reporting_period, loss_ratio, combined_ratio FROM pc_insurance.gold.loss_ratio_by_lob ORDER BY reporting_period DESC`
   - Q: "Show claim frequency and severity by state" → `SELECT state, line_of_business, reporting_period, claim_frequency, claim_severity FROM pc_insurance.gold.claim_frequency_severity ORDER BY reporting_period DESC`
   - Q: "Which agents have the best retention rates?" → `SELECT agent_name, agency_name, reporting_period, retention_rate, total_policies FROM pc_insurance.gold.retention_by_agent ORDER BY retention_rate DESC`
   - Q: "Give me the executive dashboard summary" → `SELECT * FROM pc_insurance.gold.uw_dashboard_summary ORDER BY reporting_period DESC, line_of_business`
6. Save and note the **Genie Space ID**

#### 3c. Create Documentation Genie Space

1. Go to **Genie** → **Create Space**
2. Name: `PC_Insurance_Documentation`
3. Description: `Technical documentation for the P&C Insurance Medallion project — tables, agents, DQ rules, pipelines.`
4. Add table: `pc_insurance.reference.project_documentation`
5. Save and note the **Genie Space ID**

#### 3d. Create DevOps Genie Space (or reuse Documentation)

**Option A (Recommended — reuse Documentation space):** Use the same Genie Space ID as Documentation. The Supervisor Agent will register it with a DevOps-specific description that limits it to Git/CI-CD guidance only.

**Option B (Separate space):**
1. Go to **Genie** → **Create Space**
2. Name: `PC_Insurance_DevOps`
3. Description: `DevOps guidance for Git, CI/CD, and deployment operations. GUIDANCE ONLY — cannot execute Git operations.`
4. Add tables: `pc_insurance.reference.project_documentation` (or no tables if guidance-only)
5. Save and note the **Genie Space ID**

**⚠️ Record all Genie Space IDs — needed for Step 6.**

---

### Step 4: Register MLflow Models + Create Serving Endpoints (30 minutes)

Two agents (Architect and Data Engineer) are deployed as MLflow models with serving endpoints.

#### 4a. Run Architect Agent Notebook

Run the notebook `agents/Architect_Agent.py`. This notebook:
1. Defines the Architect system prompt (Medallion architecture, P&C insurance domain, Databricks best practices)
2. Creates a `pyfunc` model class and logs it to MLflow experiment `/Users/<your-email>/pc_insurance_agents`
3. Registers the model as `pc_architect_agent` in the MLflow Model Registry
4. Creates a serving endpoint named `pc_architect_agent` (Small workload, scale-to-zero enabled)

**Via UI (if notebook execution fails):**
1. Go to **Models** → verify `pc_architect_agent` exists
2. Go to **Serving** → **Create Endpoint**
3. Name: `pc_architect_agent`
4. Source: `pc_architect_agent` (latest version)
5. Workload: Small, Scale to zero: enabled
6. Click Create

#### 4b. Run Data Engineer Agent Notebook

Run the notebook `agents/Data_Engineer_Agent.py`. This notebook:
1. Defines the Data Engineer system prompt (pipeline implementation, code patterns, P&C data sources)
2. Creates a `pyfunc` model class and logs it to MLflow experiment `/Users/<your-email>/pc_insurance_agents`
3. Registers the model as `pc_data_engineer_agent` in the MLflow Model Registry
4. Creates a serving endpoint named `pc_data_engineer_agent` (Small workload, scale-to-zero enabled)

**Via UI (if notebook execution fails):** Follow the same steps as above but use model name `pc_data_engineer_agent` and endpoint name `pc_data_engineer_agent`.

#### 4c. Verify Serving Endpoints

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

for ep_name in ["pc_architect_agent", "pc_data_engineer_agent"]:
    ep = w.serving_endpoints.get(ep_name)
    print(f"{ep_name}: {ep.state.ready}")
    # Expected: READY (may take a few minutes to provision)
```

**⚠️ Endpoint names (`pc_architect_agent`, `pc_data_engineer_agent`) are needed for Step 6.**

---

### Step 5: Deploy MCP App — pc-insurance-workspace-actions (15 minutes)

The MCP app (`app/app.py`) provides 10 tools that the Supervisor Agent calls to EXECUTE workspace changes: `execute_sql`, `create_table`, `write_notebook`, `run_notebook`, `git_commit`, `run_dq_checks`, `get_table_schema`, `query_table`, `update_file`, `insert_config_row`.

#### 5a. Deploy the App

```bash
# Deploy via Databricks CLI
databricks apps deploy pc-insurance-workspace-actions \
  --source-file app/app.py
```

Or via the Databricks UI:
1. Navigate to **Apps** → **Create App**
2. Name: `pc-insurance-workspace-actions`
3. Source: Select `app/app.py` from the repo
4. Click **Deploy**

#### 5b. Set Environment Variables

After deployment, set these environment variables in the app settings:

| Variable | Value | Description |
|---|---|---|
| `CATALOG_NAME` | `pc_insurance` | Default catalog for SQL execution |
| `GIT_REPO_PATH` | `/Repos/<your-user>/pc-insurance-medallion` | Databricks Git folder path |
| `SQL_WAREHOUSE_ID` | `<your-warehouse-id>` | SQL warehouse ID for statement execution (leave empty to auto-detect) |

#### 5c. Verify App Status

```bash
databricks apps get pc-insurance-workspace-actions
databricks apps logs pc-insurance-workspace-actions
```

The app should be in `RUNNING` state. If the app uses auto-detect for `SQL_WAREHOUSE_ID`, ensure at least one SQL warehouse exists.

---

### Step 6: Create Supervisor Agent with 8 Tools + Anti-Routing Rules (30 minutes)

The Supervisor Agent is the orchestrator that routes questions to the right specialist. It registers all 8 tools created in Steps 1-5.

#### 6a. Gather All IDs

Before creating the Supervisor Agent, collect all resource IDs from Steps 1-5:

| Variable | Source | Example |
|---|---|---|
| `KNOWLEDGE_ASSISTANT_ID` | Step 2c | Name of the Knowledge Assistant (e.g., `pc_domain_expert`) |
| `GENIE_SPACE_ANALYST_ID` | Step 3b | UUID from the Analyst Genie Space URL |
| `GENIE_SPACE_DOCUMENTATION_ID` | Step 3c | UUID from the Documentation Genie Space URL |
| `GENIE_SPACE_DEVOPS_ID` | Step 3d | Same as Documentation or separate UUID |
| `ARCHITECT_ENDPOINT` | Step 4a | `pc_architect_agent` (endpoint name) |
| `DATA_ENGINEER_ENDPOINT` | Step 4b | `pc_data_engineer_agent` (endpoint name) |
| `DQ_FUNCTION` | Step 1 | `pc_insurance.dq.calculate_dq_score` (fully qualified) |
| `MCP_APP_NAME` | Step 5 | `pc-insurance-workspace-actions` (app name) |

#### 6b. Create Supervisor Agent

**Via UI (Recommended):**
1. Go to **Agents** → **Create Agent** → **Supervisor Agent**
2. Display Name: `P&C Insurance Medallion Architecture Team`
3. Description: `A multi-agent team that designs, develops, and operates a Medallion architecture for Property & Casualty (P&C) Insurance. Routes questions to the right specialist: Architect for design, Data Engineer for code, Domain Expert for insurance knowledge, Analyst for KPIs, QA for validation, Documentation for docs, DevOps for guidance, and Workspace-Actions for execution.`
4. Add the 8 tools one by one (see 6c-6j below)
5. Set the instructions (see 6k)
6. Save and wait for the serving endpoint to be READY

**Via SDK (if Supervisor Agents API is available):**

Run the notebook `agents/Supervisor_Agent_Setup.py`. Update the configuration variables at the top of the notebook with your IDs, then run all cells.

#### 6c. Register Tool 1: Architect (Serving Endpoint)

- **Tool type:** Serving Endpoint
- **Endpoint:** `pc_architect_agent`
- **Description:** `Designs Medallion architecture for P&C insurance. Defines Bronze/Silver/Gold layer schemas, data flow topology, Unity Catalog structure, governance policies, SCD2 strategies, and scalability patterns. Answers questions about architecture design, table schemas, and pipeline topology. DO NOT use for code generation — use Data Engineer for that.`

#### 6d. Register Tool 2: Data Engineer (Serving Endpoint)

- **Tool type:** Serving Endpoint
- **Endpoint:** `pc_data_engineer_agent`
- **Description:** `Implements Bronze/Silver/Gold pipelines for P&C insurance data. Writes Spark Declarative Pipeline (SDP) code, SQL transformations, MERGE statements for SCD2, and data quality expectations. DO NOT use for architecture design — use Architect for that.`

#### 6e. Register Tool 3: P&C Domain Expert (Knowledge Assistant)

- **Tool type:** Knowledge Assistant
- **Knowledge Assistant ID:** `<KNOWLEDGE_ASSISTANT_ID from Step 2c>`
- **Description:** `Answers P&C insurance domain questions: policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements. DO NOT use for KPI queries — use Analyst for that.`

#### 6f. Register Tool 4: Analyst (Genie Space)

- **Tool type:** Genie Space
- **Genie Space ID:** `<GENIE_SPACE_ANALYST_ID from Step 3b>`
- **Description:** `Answers business questions by querying Gold layer tables: loss ratios, combined ratios, claim frequency/severity, retention rates, premium growth, exposure summaries. Route ALL KPI and business metric questions here. DO NOT use for domain definitions — use Domain Expert for that.`

#### 6g. Register Tool 5: QA Validator (UC Function)

- **Tool type:** UC Function
- **Function:** `pc_insurance.dq.calculate_dq_score`
- **Description:** `Runs data quality validation checks on the data. Route data quality, validation, and testing questions here. DO NOT use for business KPI questions.`

#### 6h. Register Tool 6: Documentation (Genie Space)

- **Tool type:** Genie Space
- **Genie Space ID:** `<GENIE_SPACE_DOCUMENTATION_ID from Step 3c>`
- **Description:** `Generates and updates technical documentation for the project. Queries the project_documentation table for table schemas, agent configs, DQ rules, and pipeline details. DO NOT use for KPI queries or architecture design.`

#### 6i. Register Tool 7: DevOps (Genie Space)

- **Tool type:** Genie Space
- **Genie Space ID:** `<GENIE_SPACE_DEVOPS_ID from Step 3d>` (or reuse Documentation ID)
- **Description:** `Provides GUIDANCE on Git, CI/CD, and deployment operations. GUIDANCE ONLY — cannot execute Git operations, file writes, or SQL. DO NOT route KPI/business metric questions here — those go to Analyst. DO NOT route architecture questions here — those go to Architect.`

#### 6j. Register Tool 8: Workspace-Actions (MCP App)

- **Tool type:** MCP Server
- **MCP App:** `pc-insurance-workspace-actions`
- **Description:** `EXECUTES workspace changes: file writes, SQL execution, git commits, notebook execution, DQ checks, config row inserts. This is the ONLY tool that can make changes to the workspace. DO NOT use for guidance or advisory questions.`

#### 6k. Set Supervisor Instructions + Anti-Routing Rules

Paste the following as the Supervisor Agent instructions:

```
You are the team lead for a virtual team building a Medallion architecture for a Property & Casualty (P&C) Insurance use case on Databricks.

## Your Team Members

1. ARCHITECT (Principal Data Architect) — Designs the overall Medallion architecture, defines Bronze/Silver/Gold layer schemas, data flow topology, and Unity Catalog governance. Route architecture and design questions here.

2. DATA ENGINEER (Senior Data Engineer) — Implements Bronze/Silver/Gold pipelines, writes SDP code, SQL transformations, and data quality expectations. Route code generation and pipeline implementation questions here.

3. P&C DOMAIN EXPERT (Insurance SME) — Answers questions about P&C insurance domain: policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements. Route insurance domain questions here.

4. ANALYST (Business Analyst) — Answers business questions by querying Gold layer tables: loss ratios, combined ratios, claim frequency/severity, retention rates, premium growth, exposure summaries. Route KPI and business metric questions here.

5. QA VALIDATOR (QA Engineer) — Runs data quality validation checks on the data. Route data quality, validation, and testing questions here.

6. DOCUMENTATION (Technical Writer) — Generates and updates technical documentation. Route documentation questions here.

7. DEVOPS (DevOps Engineer) — Provides GUIDANCE on Git, CI/CD, and deployment. GUIDANCE ONLY — cannot execute operations.

8. WORKSPACE-ACTIONS (MCP Server) — EXECUTES workspace changes: file writes, SQL, git commits. Route execution requests here.

## Routing Rules

- Architecture and design questions → ARCHITECT
- Code and pipeline questions → DATA ENGINEER
- Insurance domain questions → P&C DOMAIN EXPERT
- KPI and business questions → ANALYST
- Data quality questions → QA VALIDATOR
- Documentation questions → DOCUMENTATION
- Git/CI-CD guidance → DEVOPS (guidance only)
- Workspace execution (file writes, SQL, git commits) → WORKSPACE-ACTIONS

## Anti-Routing Rules (HARD BOUNDARIES — MUST NOT VIOLATE)

1. KPI questions → ANALYST only (never DevOps or Documentation)
2. Git execution → WORKSPACE-ACTIONS (DevOps is GUIDANCE ONLY — cannot execute)
3. Architecture design → ARCHITECT (not Data Engineer)
4. Code implementation → DATA ENGINEER (not Architect)
5. Domain definitions → P&C DOMAIN EXPERT (not Analyst)
6. DevOps → GUIDANCE ONLY (cannot execute Git operations, file writes, or SQL)

## Synthesis Rules

When multiple agents contribute:
1. Present the synthesized answer in a logical order (architecture first, then implementation, then domain context)
2. Cite which agent provided each part
3. Ensure consistency across agent responses
4. Add a summary at the end if the response is long
```

#### 6l. Wait for Endpoint to be READY

The Supervisor Agent creates a serving endpoint automatically. Check status:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# List supervisor agents to find your endpoint name
# The endpoint name will be displayed in the Agent UI
# Check endpoint status
endpoint_name = "<your-supervisor-endpoint-name>"  # from the Agent UI
ep = w.serving_endpoints.get(endpoint_name)
print(f"Endpoint: {endpoint_name}, Status: {ep.state.ready}")
# Expected: READY (may take 5-10 minutes)
```

---

### Step 7: Validate the Multi-Agent System (10 minutes)

#### 7a. Query the Supervisor Agent

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Replace with your Supervisor Agent endpoint name
endpoint_name = "<your-supervisor-endpoint-name>"

resp = w.api_client.do(
    "POST",
    f"/serving-endpoints/{endpoint_name}/invocations",
    body={
        "input": [
            {"role": "user", "content": "What is our loss ratio by line of business?"}
        ],
        "max_tokens": 2000,
        "stream": False
    }
)
print(resp)
# Expected: Response should route to Analyst agent and return loss ratio data
```

#### 7b. Test Each Routing Path

| Test Question | Expected Routing |
|---|---|
| "Design the Bronze layer for policy data" | → Architect |
| "Write the Silver MERGE for policy_dim SCD2" | → Data Engineer |
| "What is a loss ratio?" | → Domain Expert |
| "What is our loss ratio by LOB?" | → Analyst |
| "Check data quality on policies table" | → QA Validator |
| "What tables are in the Gold layer?" | → Documentation |
| "How do I set up CI/CD?" | → DevOps (guidance only) |
| "Commit my changes to git" | → Workspace-Actions (executes) |

#### 7c. Verify MCP App Integration

Test that the Workspace-Actions tool can execute SQL:

```python
# Via the Supervisor Agent, ask it to run a simple SQL query
# The MCP app should receive the request and return results
resp = w.api_client.do(
    "POST",
    f"/serving-endpoints/{endpoint_name}/invocations",
    body={
        "input": [
            {"role": "user", "content": "Run this SQL: SELECT COUNT(*) FROM pc_insurance.bronze.policies_raw"}
        ],
        "max_tokens": 2000,
        "stream": False
    }
)
print(resp)
# Expected: The supervisor routes to Workspace-Actions, which executes the SQL and returns the count
```

---

### Bundle Variables for Multi-Agent Deployment

When deploying to another environment via DAB, update these variables in `databricks.yml` or pass them as `--var` flags:

| Variable | Description | Example Value |
|---|---|---|
| `sql_warehouse_id` | SQL warehouse ID for MCP app statement execution | `670b9d31fd290bb2` |
| `supervisor_endpoint` | Supervisor Agent serving endpoint name | `mas-56389669-endpoint` |
| `workspace_root` | Workspace root path for the supervisor | `/Users/<your-email>/InsuranceModel` |
| `allowed_roots` | Comma-separated allowed root paths | `/Users/<your-email>/InsuranceModel,/Repos/<your-user>/pc-insurance-medallion` |
| `repo_path` | Databricks Git folder path | `/Repos/<your-user>/pc-insurance-medallion` |

**Deploy with variables:**
```bash
databricks bundle deploy -t staging \
  --var sql_warehouse_id=<staging-warehouse-id> \
  --var supervisor_endpoint=<staging-supervisor-endpoint> \
  --var workspace_root=/Users/<target-user>/InsuranceModel \
  --var allowed_roots=/Users/<target-user>/InsuranceModel,/Repos/<target-user>/pc-insurance-medallion \
  --var repo_path=/Repos/<target-user>/pc-insurance-medallion
```

**⚠️ Note:** The Supervisor Agent, Genie Spaces, Knowledge Assistant, and serving endpoints must be created MANUALLY in each environment (Steps 1-6 above). The DAB bundle deploys the **pipeline job** only. The multi-agent system is environment-specific because it references workspace-local resources (Genie Space IDs, endpoint names, volume paths).

---

## Git Automation via MCP App

### Overview

The `pc-insurance-workspace-actions` MCP app (`app/app.py`) automatically commits and pushes changes to the Git repository after successful pipeline execution. It replaces the former `Git_Automation.py` notebook.

### Workflow

```
Pipeline Execution -> Validation Success -> MCP App (app/app.py)
  -> Health Check (verify repo state, remote config, branch)
  -> Check Changes (git status --porcelain)
  -> Stage All (git add -A via subprocess)
  -> Commit (git commit -m "message" via subprocess)
  -> Push with Retry (git push origin main, up to 3 retries)
```

### Features

- **Subprocess Git CLI**: Uses `git add`, `git commit`, `git push` via subprocess (robust, no SDK dependency)
- **Automatic Commit**: Changes committed after validation success
- **Smart Push**: Retry logic handles network issues (3 attempts, 5-second delay)
- **Health Checks**: Validates repository state before operations
- **Change Detection**: Only commits when changes are detected (`git status --porcelain`)
- **MCP Integration**: Exposed as tool to Supervisor Agent (8th tool)

### Configuration

```python
# Environment variables in app/app.py
REPO_PATH = "/Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion"
MAX_PUSH_RETRIES = 3
RETRY_DELAY_SECONDS = 5
```

### MCP Tool Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `commit_message` | str | Auto-generated | Custom commit message |
| `push_enabled` | bool | True | Enable/disable push to remote |
| `repo_path` | str | Configured | Override repository path |
| `skip_if_no_changes` | bool | True | Skip commit when no changes detected |

### Return Value

```python
{
    'timestamp': '2026-09-24T10:30:00',
    'success': True,
    'repo_health': 'Repository healthy, on branch: main',
    'changes_detected': True,
    'staged': True,
    'committed': True,
    'commit_hash': 'a1b2c3d',
    'pushed': True,
    'messages': ['Health check: OK', 'All changes staged', 'Commit created: a1b2c3d', 'Pushed to origin/main']
}
```

### Compliance

Ensures compliance with the **Mandatory Change Completion Policy**:
- All changes committed after validation
- Documentation updates included in staged changes
- Audit trail maintained in Git history
- No manual intervention required

---

## Post-Deployment Validation

### Compute Resources

**Development Cluster**:
- Runtime: 13.3 LTS or higher
- Workers: 2-4 nodes (autoscaling 2-8)

**Production Cluster**:
- Runtime: 13.3 LTS or higher
- Workers: 4-8 nodes (autoscaling 4-16)

### Validation Checklist

- [ ] All Bronze tables populated with expected row counts
- [ ] Silver tables have SCD2 columns (is_current, effective_from, effective_to)
- [ ] Gold tables have KPI aggregations
- [ ] DQ score > 90% for all tables
- [ ] Reconciliation status = MATCH for all transformations
- [ ] Supervisor Agent endpoint is READY
- [ ] MCP app is running and healthy
- [ ] Scheduled job configured and tested

### Deploying to Another Environment

The same Git commit can be deployed to `dev`, `staging`, or `prod`:

```bash
databricks bundle deploy -t staging \
  --var sql_warehouse_id=<staging-warehouse-id> \
  --var supervisor_endpoint=<staging-supervisor-endpoint> \
  --var workspace_root=/Users/<target-user>/InsuranceModel \
  --var allowed_roots=/Users/<target-user>/InsuranceModel,/Repos/<target-user>/pc-insurance-medallion \
  --var repo_path=/Repos/<target-user>/pc-insurance-medallion
```

Use the same command with `-t prod` and production values for production.

---

## Rollback Procedures

### Pipeline Rollback
1. Identify the last known good state via audit tables
2. Re-run the pipeline with `load_type=INITIAL` to reload from source
3. Verify row counts and DQ scores match previous good state

### Git Rollback
```bash
cd /Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion
git log --oneline -5          # Find last good commit
git revert <commit-hash>       # Revert specific commit
git push origin main           # Push revert
```

### DAB Rollback
```bash
# Redeploy previous bundle version
databricks bundle deploy -t dev --previous-version
```

---

## Troubleshooting

### Issue: Pipeline Failure

**Diagnosis**:
```sql
SELECT transformation_id, run_timestamp, error_message
FROM pc_insurance.reference.silver_load_audit
WHERE status = 'FAILED'
ORDER BY run_timestamp DESC LIMIT 10;
```

**Resolution**: Check error message, review notebook logs, verify source data, check for schema changes, validate UC permissions, re-run after fix.

### Issue: DQ Score Below Threshold

**Diagnosis**:
```sql
SELECT table_name, column_name, validation_rule, failed_record_count
FROM pc_insurance.dq.dq_validation_results
WHERE validation_result = 'FAIL'
AND DATE(validation_timestamp) = CURRENT_DATE()
ORDER BY failed_record_count DESC;
```

**Resolution**: Identify failed rules, query source data for root cause, coordinate with source system owners if needed.

### Issue: Reconciliation Mismatch

**Diagnosis**:
```sql
SELECT transformation_id, source_count, target_count, count_diff, notes
FROM pc_insurance.reference.silver_reconciliation
WHERE recon_status = 'FAIL'
ORDER BY recon_timestamp DESC;
```

**Resolution**: Check for duplicates, verify deduplication logic, check SCD2 version proliferation.

### Issue: Git Push Fails ("Could not resolve host: github.com")

**Cause**: Network connectivity from execution environment to GitHub.

**Solutions**:
1. Use Databricks Repos UI to push manually
2. Configure Databricks to use a different network path
3. Use Databricks Git credentials for authentication

### Issue: "Not a git repository"

**Cause**: Repository not initialized or path incorrect.

**Solutions**:
1. Verify `REPO_PATH` in `app/app.py`
2. Ensure Databricks Repo is properly cloned
3. Check Git folder exists: `/Workspace/Repos/<user>/<repo>/.git`

### Issue: "No remote repository configured"

**Cause**: Git remote not set up.

**Solutions**:
```bash
cd /Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion
git remote add origin https://github.com/<user>/<repo>.git
```

### Issue: "Nothing to commit"

**Cause**: No changes detected (expected behavior). MCP app skips commit. No action needed.

### Issue: MCP App Not Responding

```bash
# Check app status
databricks apps get pc-insurance-workspace-actions

# Restart the app
databricks apps stop pc-insurance-workspace-actions
databricks apps start pc-insurance-workspace-actions

# View logs
databricks apps logs pc-insurance-workspace-actions
```

---

**Document Version**: 3.0  
**Last Updated**: 2026-09-25  
**Maintained By**: Data Engineering Team
