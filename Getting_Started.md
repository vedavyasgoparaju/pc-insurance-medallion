# P&C Insurance Medallion Architecture — Getting Started

## Overview

This project implements a multi-layered data platform for Property & Casualty (P&C) Insurance built on Databricks. The architecture follows the **Medallion pattern** with three layers:

| Layer | Purpose | Tables |
|---|---|---|
| **Bronze** | Raw data ingestion | policies_raw (1,000), claims_raw (300), premiums_raw (1,200), customers_raw (500), agents_raw (50) |
| **Silver** | Conformed dimensions & facts with SCD2, PII masking | policy_dim, claim_dim, customer_dim, agent_dim, date_dim, premium_fact, claim_fact |
| **Gold** | KPI aggregations for analytics | loss_ratio_by_lob, claim_frequency_severity, retention_by_agent, premium_growth, exposure_summary, uw_dashboard_summary |

**Lines of Business**: Auto, Home, Property, Commercial  
**Grain**: Gold layer aggregations are quarterly by line of business (and state for frequency/severity and exposure)  
**GitHub Repo**: https://github.com/vedavyasgoparaju/pc-insurance-medallion

---

## Prerequisites

- **Databricks workspace** with serverless compute enabled
- **Unity Catalog** enabled
- **Model Serving** access (for multi-agent system)
- **Databricks CLI** installed locally (optional, for CI/CD)
- **GitHub account** with access to the project repo

---

## Project Structure

```
pc-insurance-medallion/
├── Bronze_Pipeline.py          # Data generation & ingestion (5 Bronze tables)
├── Silver_Pipeline_Metadata.py # SCD2, PII masking, DQ checks (7 Silver tables)
├── Gold_Pipeline.py            # KPI aggregations (6 Gold tables)
├── Orchestrator.py             # Multi-agent orchestration setup
├── Agent_Setup.py              # MLflow agent registration & serving endpoints
├── plan_executor.py            # MCP server action execution
├── README.md                   # Project documentation
├── Getting_Started.md          # This file
└── databricks.yml              # DAB bundle config (optional)
```

---

## Unity Catalog Setup

The project uses a single catalog with five schemas:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS pc_insurance;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS pc_insurance.bronze;
CREATE SCHEMA IF NOT EXISTS pc_insurance.silver;
CREATE SCHEMA IF NOT EXISTS pc_insurance.gold;
CREATE SCHEMA IF NOT EXISTS pc_insurance.reference;
CREATE SCHEMA IF NOT EXISTS pc_insurance.dq;
```

### Schema Purposes

| Schema | Purpose |
|---|---|
| `bronze` | Raw ingested data (Delta format, append mode) |
| `silver` | Conformed dimensions (SCD2) and fact tables |
| `gold` | Business KPI aggregations (quarterly grain) |
| `reference` | Documentation table, domain knowledge volume |
| `dq` | Data quality validation functions |

---

## Running the Pipelines

### Step 1: Bronze Layer — Data Generation & Ingestion

The Bronze pipeline generates sample P&C insurance data and ingests it into Delta tables.

**Notebook**: `Bronze_Pipeline.py`

```python
# Run the Bronze pipeline notebook
# Generates: policies_raw (1000), claims_raw (300), premiums_raw (1200),
#            customers_raw (500), agents_raw (50)
# Ingestion mode: append with source_system and ingestion_timestamp
```

### Step 2: Silver Layer — Cleansing, SCD2 & PII Masking

The Silver pipeline transforms Bronze data into conformed dimensions and facts.

**Notebook**: `Silver_Pipeline_Metadata.py`

Key transformations:
- **Deduplication**: Row number window by natural key, keeping latest
- **SCD2**: `is_current`, `effective_from`, `effective_to`, `__START_AT`, `__END_AT` columns
- **PII Masking**: Regex-based masking on customer phone and email fields
- **Referential joins**: All fact tables join to `date_dim` for time-based analysis

### Step 3: Gold Layer — KPI Aggregations

The Gold pipeline builds executive KPI aggregations from Silver fact tables.

**Notebook**: `Gold_Pipeline.py`

Output tables (all quarterly grain by line of business):

| Table | KPI |
|---|---|
| `loss_ratio_by_lob` | Loss Ratio, Combined Ratio |
| `claim_frequency_severity` | Claim Frequency, Claim Severity |
| `retention_by_agent` | Retention Rate, New Business Growth |
| `premium_growth` | Premium Growth Rate |
| `exposure_summary` | Total Exposure, Earned Premium |
| `uw_dashboard_summary` | Combined Underwriting Dashboard |

### Running All Pipelines

You can run all pipelines sequentially via the orchestration job:

**Job Name**: `PC_Insurance_MultiAgent_Pipeline`

The job runs: Bronze → Silver → Gold → Agent deployments → Supervisor Agent setup

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

## Multi-Agent System

The project includes a **Supervisor Agent** that orchestrates 8 specialized subagents:

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
```

### Anti-Routing Rules

The supervisor enforces hard boundaries to prevent role confusion:
1. KPI questions → **Analyst only** (never DevOps or Documentation)
2. Git execution → **Workspace-Actions** (DevOps is guidance only)
3. Architecture design → **Architect** (not Data Engineer)
4. Code implementation → **Data Engineer** (not Architect)
5. Domain definitions → **Domain Expert** (not Analyst)
6. DevOps → **Guidance only** (cannot execute Git operations)

---

## CI/CD Pipeline

The project uses GitHub for version control and Databricks Jobs for orchestration.

### CI/CD Flow

1. Developer commits code to a feature branch
2. CI runs unit tests + DQ validation checks
3. PR review and merge to `main`
4. CD deploys via Declarative Automation Bundles (DABs)
5. Jobs run automatically on schedule

### GitHub Setup

```bash
# Clone the repo
git clone https://github.com/vedavyasgoparaju/pc-insurance-medallion.git

# Or use Databricks Git folder (already configured):
# /Repos/vedavyas.goparaju/pc-insurance-medallion
```

### Credential Setup

- **Git credential**: `git_vedavyas` (GitHub OAuth, username: `vedavyasgoparaju`)
- **Secret scope**: `pc_insurance` with key `github_token` for GitHub PAT

---

## Quick Start Checklist

- [ ] Verify Unity Catalog is enabled in your workspace
- [ ] Create catalog `pc_insurance` and 5 schemas
- [ ] Run `Bronze_Pipeline.py` to generate and ingest data
- [ ] Run `Silver_Pipeline_Metadata.py` for SCD2 transformations
- [ ] Run `Gold_Pipeline.py` for KPI aggregations
- [ ] Verify data quality with `calculate_dq_score`
- [ ] Deploy serving endpoints for `pc_architect_agent` and `pc_data_engineer_agent`
- [ ] Configure the Supervisor Agent with all 8 tools
- [ ] Test the supervisor endpoint with a sample query
- [ ] Clone the GitHub repo to your Databricks Git folder

---

## Support

- **Project Documentation**: Query the `pc_insurance.reference.project_documentation` table (37 entries)
- **Domain Knowledge**: UC Volume `pc_insurance.reference.pc_domain_docs` (P&C insurance guide)
- **Supervisor Agent**: Query `mas-56389669-endpoint` for any project question
- **GitHub Issues**: https://github.com/vedavyasgoparaju/pc-insurance-medallion/issues