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
7. [Multi-Agent System Setup](#multi-agent-system-setup)
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

## Multi-Agent System Setup

The project includes a **Supervisor Agent** that orchestrates 8 specialized tools (7 subagents + 1 MCP server):

**Endpoint**: `mas-56389669-endpoint` (READY)

| Agent | Type | Role |
|---|---|---|
| **Architect** | Serving Endpoint | Designs Bronze/Silver/Gold schemas, data flow topology |
| **Data Engineer** | Serving Endpoint | Generates SDP code, SQL transformations, DQ expectations |
| **P&C Domain Expert** | UC Volume | Answers insurance domain questions (policies, claims, underwriting) |
| **Analyst** | Genie Space | Queries Gold layer for KPIs (loss ratios, retention, frequency) |
| **QA Validator** | UC Function | Runs data quality validation checks |
| **Documentation** | Genie Space | Generates and updates technical documentation |
| **DevOps** | Genie Space | Provides Git/CI-CD guidance (guidance only, no execution) |
| **Workspace-Actions** | MCP App | Executes workspace changes (file writes, SQL, git commits) |

### Querying the Supervisor Agent

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
resp = w.api_client.do(
    "POST",
    "/serving-endpoints/mas-56389669-endpoint/invocations",
    body={
        "input": [
            {"role": "user", "content": "What is our loss ratio by line of business?"}
        ],
        "max_tokens": 2000,
        "stream": False
    }
)
print(resp)
```

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
