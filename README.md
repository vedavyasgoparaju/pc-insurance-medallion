# P&C Insurance Medallion Architecture on Databricks

A multi-layered data platform for Property & Casualty Insurance built on Databricks, featuring Bronze/Silver/Gold pipelines, data quality functions, and a 7-agent multi-agent system.

## What's New: Metadata-Driven Bronze, Silver + Gold Layers

Bronze, Silver, and Gold layers now use a **metadata-driven approach** with:
- **Config tables** defining all sources/transformations, schemas, and load order (no hardcoding)
- **Staging tables** as intermediate landing zone with `_load_id` traceability
- **Load audit tables** tracking every load execution (timing, counts, status)
- **Reconciliation tables** validating source-to-target row counts at every interval
- **Auto Loader** for scalable CSV ingestion from UC Volume (Bronze)
- **SCD2, FACT, and DEDUP transformation types** with PII masking (Silver)
- **Gold metric configuration** defining KPI sources, dimensions, formulas, grain, and refresh order
- **Gold load audit** tracking every configured KPI refresh
- **Both INITIAL and INCREMENTAL load** patterns supported

## Recent Changes (2026-09-25)

### Repository Restructuring
- 📁 **New folder hierarchy**: `pipelines/`, `agents/`, `app/`, `execution/`, `utils/`, `sql/`, `docs/`
- ❌ **Removed:** `Silver_Pipeline.py` (obsolete), `Git_Automation.py` (replaced by MCP app), `CLEANUP_SUMMARY.md`, `CLEANUP_FINAL_REPORT.txt`, `tools/`, `resources/`, `.vscode/`
- ✅ **Active:** `Silver_Pipeline_Metadata.py` (metadata-driven framework)
- 📝 **Updated:** All pipeline notebooks moved to `pipelines/`, agent scripts to `agents/`
- 🔧 **MCP App:** `app/app.py` — `pc-insurance-workspace-actions` handles git commits, file writes, and SQL execution via subprocess git CLI

**Reason:** Production-ready codebase with clean folder structure. All Silver transformations are config-driven via `silver_transformation_config`.

## Architecture

### Unity Catalog Structure
- **Catalog**: `pc_insurance`
- **Schemas**: `bronze`, `silver`, `gold`, `reference`, `dq`

### Metadata and Audit Tables (reference schema)

| Table | Layer | Description |
|-------|-------|-------------|
| bronze_ingestion_config | Bronze | Config-driven source definitions (path, schema, target, load order) |
| bronze_load_audit | Bronze | Every load execution tracked (load_id, timing, counts, status) |
| bronze_reconciliation | Bronze | Source-to-target row count validation (MATCH/MISMATCH) |
| silver_transformation_config | Silver | Config-driven transformation definitions (source, target, SCD2 cols, PII rules, load order) |
| silver_load_audit | Silver | Every Silver load tracked (SCD2 ops, row counts, status) |
| silver_reconciliation | Silver | Bronze-to-Silver row count validation (MATCH/MISMATCH) |
| gold_metric_config | Gold | Config-driven KPI definitions, formulas, sources, and refresh order |
| gold_load_audit | Gold | Every Gold metric refresh tracked with source and target counts |

### Staging Tables (bronze schema)

| Table | Description |
|-------|-------------|
| stg_policies | Intermediate landing zone for policies (with _load_id, _file_name) |
| stg_claims | Intermediate landing zone for claims |
| stg_premiums | Intermediate landing zone for premiums |
| stg_customers | Intermediate landing zone for customers |
| stg_agents | Intermediate landing zone for agents |

### Staging Tables (silver schema)

| Table | Description |
|-------|-------------|
| stg_policy_dim | Policy dimension staging (SCD2, with _load_id, _bronze_source) |
| stg_claim_dim | Claim dimension staging (SCD2) |
| stg_customer_dim | Customer dimension staging (SCD2 + PII masking applied) |
| stg_agent_dim | Agent dimension staging (DEDUP) |
| stg_premium_fact | Premium fact staging |
| stg_claim_fact | Claim fact staging |

### Bronze Layer (Raw Ingestion via Auto Loader)

| Table | Rows | Source System |
|-------|------|---------------|
| policies_raw | 100 | policy_admin_system |
| claims_raw | 300 | claims_system |
| premiums_raw | 1,200 | billing_system |
| customers_raw | 500 | crm |
| agents_raw | 50 | agent_admin |

All Bronze tables have `source_system` and `ingestion_timestamp` metadata columns.

### Silver Layer (Conformed Dimensions and Facts)

| Table | Rows | Transformation Type | Description |
|-------|------|---------------------|-------------|
| policy_dim | 1,000 | DIMENSION_SCD2 | Policy dimension (SCD2) |
| claim_dim | 300 | DIMENSION_SCD2 | Claim dimension (SCD2) |
| customer_dim | 500 | DIMENSION_SCD2 | Customer dimension (SCD2 + PII masked) |
| agent_dim | 50 | DEDUP | Agent dimension |
| date_dim | 2,557 | Generated | Date dimension (2020-2026, fiscal calendar) |
| premium_fact | 1,200 | FACT | Premium fact table |
| claim_fact | 300 | FACT | Claim fact table |

### Gold Layer (KPI Aggregations)

| Table | Rows | KPIs |
|-------|------|------|
| loss_ratio_by_lob | 24 | Loss ratio, expense ratio, combined ratio |
| claim_frequency_severity | 120 | Frequency, severity by LOB and state |
| retention_by_agent | 198 | Retention rate, new business growth |
| premium_growth | 24 | Written/earned premium, growth rate |
| exposure_summary | 82 | Active policies, coverage limits |
| uw_dashboard_summary | 24 | Executive dashboard (all KPIs) |

Gold outputs are selected from active rows in `gold_metric_config`; the pipeline
does not create a new hardcoded output path for each metric.

## Bronze Pipeline: Metadata-Driven Flow

```
Source CSV (UC Volume) -> Auto Loader -> Staging Table (with _load_id, _file_name)
                                              -> Promotion -> Bronze Target Table
                                                    |                |
                                              Config Table    Audit + Reconciliation
```

### Load Types

| Type | Behavior |
|------|----------|
| INITIAL | Truncate target + staging, load all files, full refresh |
| INCREMENTAL | Preserve target, load new files only, append to target |

### Running the Bronze Pipeline

The `Bronze_Pipeline` notebook accepts two widget parameters:

- `load_type`: `INITIAL` or `INCREMENTAL` (default: `INCREMENTAL`)
- `source_filter`: comma-separated source names (empty = all sources)

```python
# Full refresh for all sources
dbutils.notebook.run("Bronze_Pipeline", 600,
  {"load_type": "INITIAL", "source_filter": ""})

# Incremental load for policies only
dbutils.notebook.run("Bronze_Pipeline", 600,
  {"load_type": "INCREMENTAL", "source_filter": "policies"})
```

### Adding a New Bronze Source

1. Add CSV files to `/Volumes/pc_insurance/reference/raw_sources/{new_source}/`
2. Create staging + target Bronze tables
3. Insert a row into `bronze_ingestion_config`
4. Run the pipeline with `source_filter="{new_source}"`

## Silver Pipeline: Metadata-Driven Flow

```
silver_transformation_config (defines mappings)
    |
    v
Bronze Tables (policies_raw, claims_raw, etc.)
    | cleansing, dedup, PII masking
    v
Silver Staging (stg_policy_dim, etc.) + _load_id, _bronze_source
    | MERGE (SCD2) / INSERT (FACT) / OVERWRITE (DEDUP)
    v
Silver Target (policy_dim, claim_fact, etc.)
    |
    v
silver_load_audit + silver_reconciliation (audit trail)
```

### Transformation Types

| Type | Description |
|------|-------------|
| DIMENSION_SCD2 | SCD Type 2: close old version on change, insert new with is_current=true |
| FACT | Append new records (deduped by business key) |
| DEDUP | Overwrite with latest deduplicated records |

### PII Masking

Configured per transformation in `silver_transformation_config.pii_mask_rules` as JSON:
- `regex_mask`: Phone numbers masked (keep first 3 and last 4 digits)
- `hash`: Email addresses hashed with SHA-256
- `partial`: Keep first 2 chars + domain

### Running the Silver Pipeline

The `Silver_Pipeline_Metadata` notebook accepts two widget parameters:

- `load_type`: `INITIAL` or `INCREMENTAL` (default: `INCREMENTAL`)
- `transformation_filter`: comma-separated transformation names (empty = all)

```python
# Full refresh of all Silver transformations
dbutils.notebook.run("Silver_Pipeline_Metadata", 600,
  {"load_type": "INITIAL", "transformation_filter": ""})

# Incremental load for specific transformations
dbutils.notebook.run("Silver_Pipeline_Metadata", 600,
  {"load_type": "INCREMENTAL", "transformation_filter": "policy_dim,claim_dim"})
```

### Adding a New Silver Transformation

1. Create staging + target Silver tables
2. Insert a row into `silver_transformation_config` with transformation type, business key, SCD2 columns, PII rules
3. Run the pipeline with `transformation_filter="{new_transformation}"`

## Querying the Audit Trail

```sql
-- Recent Bronze loads by source
SELECT substring(load_id, 1, 13) as load_id, source_name, load_type, status,
       source_row_count, target_row_count_after, rows_inserted
FROM pc_insurance.reference.bronze_load_audit
ORDER BY load_start_time DESC LIMIT 10;

-- Recent Silver loads by transformation
SELECT substring(load_id, 1, 13) as load_id, transformation_name, load_type, status,
       source_row_count, staging_row_count, target_row_count_after, rows_inserted
FROM pc_insurance.reference.silver_load_audit
ORDER BY load_start_time DESC LIMIT 10;

-- Bronze reconciliation mismatches
SELECT source_name, match_status, source_row_count, target_row_count,
       expected_target_count, mismatch_details
FROM pc_insurance.reference.bronze_reconciliation
WHERE match_status != "MATCH";

-- Silver reconciliation mismatches
SELECT transformation_name, match_status, source_row_count, staging_row_count,
       target_row_count, mismatch_details
FROM pc_insurance.reference.silver_reconciliation
WHERE match_status != "MATCH";
```

## Multi-Agent System

Supervisor Agent: "P&C Insurance Medallion Architecture Team" with 8 tools (7 subagents + 1 MCP server):

1. **Architect** (serving_endpoint `pc_architect_agent`) - Architecture design
2. **Data Engineer** (serving_endpoint `pc_data_engineer_agent`) - Pipeline code generation
3. **P&C Domain Expert** (volume `pc_insurance.reference.pc_domain_docs`) - P&C insurance domain knowledge
4. **QA Validator** (uc_function `pc_insurance.dq.calculate_dq_score`) - Data quality validation
5. **Analyst** (genie_space) - Gold layer KPI queries
6. **Documentation** (genie_space) - Technical documentation
7. **DevOps** (genie_space) - Git/CI-CD GUIDANCE ONLY (no execution)
8. **Workspace-Actions** (MCP app `pc-insurance-workspace-actions`) - EXECUTES workspace changes (git commits, file writes, SQL)

**Supervisor Endpoint**: `mas-56389669-endpoint` (READY)

### Anti-Routing Rules

1. KPI questions → **Analyst only** (never DevOps or Documentation)
2. Git execution → **Workspace-Actions** (DevOps is guidance only)
3. Architecture design → **Architect** (not Data Engineer)
4. Code implementation → **Data Engineer** (not Architect)
5. Domain definitions → **Domain Expert** (not Analyst)
6. DevOps → **Guidance only** (cannot execute Git operations)

## Repository Structure

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
│   ├── Domain_Expert_Agent.py      # UC volume domain expert setup
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
│   ├── DEPLOYMENT.md               # Deployment guide
│   ├── Git_Automation_Guide.md     # Git automation via MCP app
│   ├── InsuranceModel_Architecture_Guide.md  # End-to-end architecture guide
│   └── RUNBOOK.md                  # Operations runbook
├── README.md                        # This file
├── Getting_Started.md               # Newcomer guide
├── databricks.yml                   # DAB bundle config
├── pyproject.toml                  # Python project config
└── .gitignore
```

### Active Pipelines
- `pipelines/Bronze_Pipeline.py` - Metadata-driven Auto Loader ingestion (INITIAL + INCREMENTAL)
- `pipelines/Silver_Pipeline_Metadata.py` - ⭐ **ACTIVE** Metadata-driven Silver pipeline (config-driven SCD2/FACT/DEDUP)
- `pipelines/Gold_Pipeline.py` - Metadata-driven KPI aggregations and audit
- `pipelines/Orchestrator.py` - Master pipeline orchestrator (updated to use metadata-driven Silver)

### Agent Setup
- `agents/Architect_Agent.py` - MLflow agent for architecture design
- `agents/Data_Engineer_Agent.py` - MLflow agent for pipeline code
- `agents/DevOps_Agent.py` - DevOps agent (MLflow)
- `agents/Domain_Expert_Setup.py` - UC volume with P&C reference docs
- `agents/Analyst_Genie_Setup.py` - Genie Space over Gold tables
- `agents/Supervisor_Agent_Setup.py` - Multi-agent orchestration

### MCP App
- `app/app.py` - `pc-insurance-workspace-actions` MCP server (git commits, file writes, SQL execution)

### Documentation
- `README.md` - This file
- `Getting_Started.md` - Newcomer guide
- `docs/ARCHITECTURE.md` - Architecture documentation
- `docs/DATA_DICTIONARY.md` - Column-level data dictionary
- `docs/DEPLOYMENT.md` - Deployment guide
- `docs/RUNBOOK.md` - Operations runbook

## SQL DDL

- `sql/01_catalog_schemas.sql` - Catalog and schema creation
- `sql/02_bronze_tables.sql` - Bronze table DDL
- `sql/03_silver_transformation_config.sql` - Silver transformation metadata configuration
- `sql/04_gold_tables.sql` - Gold table DDL
- `sql/05_gold_metric_config.sql` - Gold metric configuration

**Note on Silver table DDL:**
Silver layer tables are created dynamically by `pipelines/Silver_Pipeline_Metadata.py` based on the configuration in `silver_transformation_config`. The `sql/03_silver_transformation_config.sql` script populates the metadata config table; it does not contain static Silver table DDL. The metadata-driven approach allows Silver tables to be defined and modified through configuration rather than static DDL scripts.

## Deploying to Another Environment

The same Git commit can be deployed to `dev`, `staging`, or `prod`.

1. Configure Databricks CLI profiles named `staging` and `prod` for the target workspaces.
2. Bootstrap the target catalog, schemas, tables, agent resources, and permissions.
3. Deploy the bundle with environment-specific values:

```bash
databricks bundle deploy -t staging \
  --var sql_warehouse_id=<staging-warehouse-id> \
  --var supervisor_endpoint=<staging-supervisor-endpoint> \
  --var workspace_root=/Users/<target-user>/InsuranceModel \
  --var allowed_roots=/Users/<target-user>/InsuranceModel,/Repos/<target-user>/pc-insurance-medallion \
  --var repo_path=/Repos/<target-user>/pc-insurance-medallion
```

Use the same command with `-t prod` and production values for production. Hosts
come from the configured Databricks CLI profiles; code does not contain target
workspace credentials.

## Architecture Documentation

The newcomer-focused end-to-end architecture guide is available in Markdown:

- `docs/InsuranceModel_Architecture_Guide.md`
- `docs/ARCHITECTURE.md` — Technical architecture reference

## KPI Formulas

- **Loss Ratio** = Incurred Losses / Earned Premium
- **Combined Ratio** = (Losses + Expenses) / Earned Premium
- **Claim Frequency** = Claim Count / Exposure Units
- **Claim Severity** = Incurred Losses / Claim Count
- **Retention Rate** = Renewed / (Renewed + Cancelled)

## Repository

- **GitHub**: https://github.com/vedavyasgoparaju/pc-insurance-medallion
- **Databricks Git Folder**: /Repos/vedavyas.goparaju/pc-insurance-medallion

## Getting Started

1. **Setup Unity Catalog**: Run `sql/01_catalog_schemas.sql`
2. **Create Bronze Tables**: Run `sql/02_bronze_tables.sql`
3. **Configure Silver Transformations**: Run `sql/03_silver_transformation_config.sql`
4. **Create Gold Tables**: Run `sql/04_gold_tables.sql` and `sql/05_gold_metric_config.sql`
5. **Run Bronze Pipeline**: Execute `pipelines/Bronze_Pipeline.py` with `load_type=INITIAL`
6. **Run Silver Pipeline**: Execute `pipelines/Silver_Pipeline_Metadata.py` with `load_type=INITIAL`
7. **Run Gold Pipeline**: Execute `pipelines/Gold_Pipeline.py`
8. **Setup Agents**: Run agent setup notebooks in `agents/`
9. **Deploy MCP App**: Deploy `app/` as `pc-insurance-workspace-actions`
10. **Query KPIs**: Use Analyst Genie Space or query Gold tables directly


## Git Automation via MCP App

The `pc-insurance-workspace-actions` MCP app (`app/app.py`) handles git commits, file writes, and SQL execution. It replaces the former `Git_Automation.py` notebook.

### Features

- ✅ **Subprocess Git CLI**: Uses `git add`, `git commit`, `git push` via subprocess (robust, no SDK dependency)
- ✅ **Automatic Commit**: Changes committed after validation success
- ✅ **Smart Push**: Retry logic handles network issues
- ✅ **Health Checks**: Validates repository state before operations
- ✅ **Change Detection**: Only commits when changes are detected
- ✅ **MCP Integration**: Exposed as tool to Supervisor Agent

### Usage

The MCP app runs as a Databricks App and is registered as the 8th tool in the Supervisor Agent. The Supervisor routes git/file/SQL execution requests to it via MCP.

### Documentation

See [Git Automation Guide](docs/Git_Automation_Guide.md) for detailed documentation.

### Compliance

Ensures compliance with the **Mandatory Change Completion Policy**:
- All changes are committed after validation
- Documentation updates are included
- Audit trail maintained in Git history
- No manual intervention required
