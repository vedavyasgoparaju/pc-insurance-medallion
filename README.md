# P&C Insurance Medallion Architecture on Databricks

A multi-layered data platform for Property & Casualty Insurance built on Databricks, featuring Bronze/Silver/Gold pipelines, data quality functions, and a multi-agent system with 8 AI tools (7 subagents + 1 MCP server).

## What's New: Metadata-Driven Bronze, Silver + Gold Layers

Bronze, Silver, and Gold layers now use a **metadata-driven approach** with:
- **Config tables** defining all sources/transformations, schemas, and load order (no hardcoding)
- **Staging tables** as intermediate landing zone with `_load_id` traceability
- **Load audit tables** tracking every load execution (timing, counts, status)
- **Reconciliation tables** validating source-to-target row counts at every interval
- **Auto Loader** for scalable CSV ingestion from UC Volume (Bronze)
- **SCD2, FACT, and DEDUP transformation types** with PII masking (Silver)
- **Gold metric configuration** defining KPI sources, dimensions, formulas, grain, and refresh order
- **Both INITIAL and INCREMENTAL load** patterns supported

## Recent Changes (2026-09-25)

### Repository Restructuring
- **New folder hierarchy**: `pipelines/`, `agents/`, `app/`, `execution/`, `utils/`, `sql/`, `docs/`
- **Removed:** `Silver_Pipeline.py` (obsolete), `Git_Automation.py` (replaced by MCP app), `CLEANUP_SUMMARY.md`, `CLEANUP_FINAL_REPORT.txt`, `tools/`, `resources/`, `.vscode/`, `InsuranceModel_Architecture_Guide.pdf`
- **Active:** `Silver_Pipeline_Metadata.py` (metadata-driven framework)
- **MCP App:** `app/app.py` — `pc-insurance-workspace-actions` handles git commits, file writes, and SQL execution via subprocess git CLI

### Documentation Consolidation (2026-09-25)
- Merged `ARCHITECTURE.md` + `InsuranceModel_Architecture_Guide.md` → single `docs/ARCHITECTURE.md`
- Merged `Getting_Started.md` + `Git_Automation_Guide.md` + `DEPLOYMENT.md` → single `docs/DEPLOYMENT.md`
- Deleted 3 redundant files; docs reduced from 7 to 4 files

## Architecture

### Unity Catalog Structure
- **Catalog**: `pc_insurance`
- **Schemas**: `bronze`, `silver`, `gold`, `reference`, `dq`

### Bronze Layer (Raw Ingestion)

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

> **See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for full architecture details** including metadata-driven framework, data flow diagrams, agent roles, UC security model, pipeline orchestration, and performance optimization.

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

**Supervisor Endpoint**: `mas-05a49b97-endpoint` (READY)

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
│   ├── DEPLOYMENT.md               # Deployment & getting started guide
│   └── RUNBOOK.md                  # Operations runbook
├── README.md                        # This file
├── databricks.yml                   # DAB bundle config
├── pyproject.toml                  # Python project config
└── .gitignore
```

### SQL DDL

- `sql/01_catalog_schemas.sql` - Catalog and schema creation
- `sql/02_bronze_tables.sql` - Bronze table DDL
- `sql/03_silver_transformation_config.sql` - Silver transformation metadata configuration
- `sql/04_gold_tables.sql` - Gold table DDL
- `sql/05_gold_metric_config.sql` - Gold metric configuration

**Note on Silver table DDL:** Silver layer tables are created dynamically by `pipelines/Silver_Pipeline_Metadata.py` based on the configuration in `silver_transformation_config`. The `sql/03_silver_transformation_config.sql` script populates the metadata config table; it does not contain static Silver table DDL.

## KPI Formulas

- **Loss Ratio** = Incurred Losses / Earned Premium
- **Combined Ratio** = (Losses + Expenses) / Earned Premium
- **Claim Frequency** = Claim Count / Exposure Units
- **Claim Severity** = Incurred Losses / Claim Count
- **Retention Rate** = Renewed / (Renewed + Cancelled)

## Getting Started

1. **Setup Unity Catalog**: Run `sql/01_catalog_schemas.sql`
2. **Create Bronze Tables**: Run `sql/02_bronze_tables.sql`
3. **Configure Silver Transformations**: Run `sql/03_silver_transformation_config.sql`
4. **Create Gold Tables**: Run `sql/04_gold_tables.sql` and `sql/05_gold_metric_config.sql`
5. **Run Data Pipeline Job** (initial load):
   ```bash
   databricks jobs run-now 894776717783668 --json '{"job_parameters":{"load_type":"INITIAL"}}'
   ```
   Or run notebooks individually: Bronze (`load_type=INITIAL`) -> Silver (`load_type=INITIAL`) -> Gold
6. **For incremental loads** (default): `databricks jobs run-now 894776717783668`
8. **Setup Agents**: Run agent setup notebooks in `agents/`
9. **Deploy MCP App**: Deploy `app/` as `pc-insurance-workspace-actions`
10. **Query KPIs**: Use Analyst Genie Space or query Gold tables directly

> **See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for detailed step-by-step deployment instructions**, including environment setup, agent deployment, MCP app configuration, post-deployment validation, rollback procedures, and troubleshooting.

## Deploying to Another Environment

The same Git commit can be deployed to `dev`, `staging`, or `prod`.

```bash
databricks bundle deploy -t staging \
  --var sql_warehouse_id=<staging-warehouse-id> \
  --var supervisor_endpoint=<staging-supervisor-endpoint> \
  --var workspace_root=/Users/<target-user>/InsuranceModel \
  --var allowed_roots=/Users/<target-user>/InsuranceModel,/Repos/<target-user>/pc-insurance-medallion \
  --var repo_path=/Repos/<target-user>/pc-insurance-medallion
```

Use the same command with `-t prod` and production values for production.

## Repository

- **GitHub**: https://github.com/vedavyasgoparaju/pc-insurance-medallion
- **Databricks Git Folder**: /Repos/vedavyas.goparaju/pc-insurance-medallion

## Documentation

| Document | Purpose |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Full architecture reference: layers, agents, metadata framework, data flow, UC structure, performance |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Setup & deployment guide: prerequisites, step-by-step deployment, MCP app, git automation, rollback |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operations runbook: daily checklists, monitoring queries, alerts, troubleshooting, incident response |
| [docs/DATA_DICTIONARY.md](docs/DATA_DICTIONARY.md) | Column-level definitions for all tables across Bronze, Silver, and Gold layers |
