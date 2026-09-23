# P&C Insurance Medallion Architecture on Databricks

A multi-layered data platform for Property & Casualty Insurance built on Databricks, featuring Bronze/Silver/Gold pipelines, data quality functions, and a 7-agent multi-agent system.

## What's New: Metadata-Driven Bronze Layer

The Bronze layer now uses a **metadata-driven approach** with:
- **Config table** defining all sources, schemas, and load order (no hardcoding)
- **Staging tables** as intermediate landing zone with `_load_id` and `_file_name` traceability
- **Load audit table** tracking every load execution (timing, counts, status)
- **Reconciliation table** validating source-to-target row counts at every interval
- **Auto Loader** for scalable CSV ingestion from UC Volume
- **Both INITIAL and INCREMENTAL load** patterns supported

## Architecture

### Unity Catalog Structure
- **Catalog**: `pc_insurance`
- **Schemas**: `bronze`, `silver`, `gold`, `reference`, `dq`

### Metadata and Audit Tables (reference schema)

| Table | Description |
|-------|-------------|
| bronze_ingestion_config | Config-driven source definitions (path, schema, target, load order) |
| bronze_load_audit | Every load execution tracked (load_id, timing, counts, status) |
| bronze_reconciliation | Source-to-target row count validation (MATCH/MISMATCH) |

### Staging Tables (bronze schema)

| Table | Description |
|-------|-------------|
| stg_policies | Intermediate landing zone for policies (with _load_id, _file_name) |
| stg_claims | Intermediate landing zone for claims |
| stg_premiums | Intermediate landing zone for premiums |
| stg_customers | Intermediate landing zone for customers |
| stg_agents | Intermediate landing zone for agents |

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

| Table | Rows | Description |
|-------|------|-------------|
| policy_dim | 1,000 | Policy dimension (SCD2) |
| claim_dim | 300 | Claim dimension (SCD2) |
| customer_dim | 500 | Customer dimension (SCD2 + PII masked) |
| agent_dim | 50 | Agent dimension |
| date_dim | 2,557 | Date dimension (2020-2026, fiscal calendar) |
| premium_fact | 1,200 | Premium fact table |
| claim_fact | 300 | Claim fact table |

### Gold Layer (KPI Aggregations)

| Table | Rows | KPIs |
|-------|------|------|
| loss_ratio_by_lob | 24 | Loss ratio, expense ratio, combined ratio |
| claim_frequency_severity | 120 | Frequency, severity by LOB and state |
| retention_by_agent | 198 | Retention rate, new business growth |
| premium_growth | 24 | Written/earned premium, growth rate |
| exposure_summary | 82 | Active policies, coverage limits |
| uw_dashboard_summary | 24 | Executive dashboard (all KPIs) |

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

### Running the Pipeline

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

### Querying the Audit Trail

```sql
-- Recent loads by source
SELECT substring(load_id, 1, 13) as load_id, source_name, load_type, status,
       source_row_count, target_row_count_after, rows_inserted
FROM pc_insurance.reference.bronze_load_audit
ORDER BY load_start_time DESC LIMIT 10;

-- Reconciliation mismatches
SELECT source_name, match_status, source_row_count, target_row_count,
       expected_target_count, mismatch_details
FROM pc_insurance.reference.bronze_reconciliation
WHERE match_status != "MATCH";
```

### Adding a New Source

1. Add CSV files to `/Volumes/pc_insurance/reference/raw_sources/{new_source}/`
2. Create staging + target Bronze tables
3. Insert a row into `bronze_ingestion_config`
4. Run the pipeline with `source_filter="{new_source}"`

## Multi-Agent System

Supervisor Agent: "P&C Insurance Medallion Architecture Team" with 7 subagents:

1. **Architect** (serving_endpoint) - Architecture design
2. **Data Engineer** (serving_endpoint) - Pipeline code generation
3. **Domain Expert** (volume) - P&C insurance domain knowledge
4. **QA Validator** (uc_function) - Data quality validation
5. **Analyst** (genie_space) - Gold layer KPI queries
6. **Documentation** (genie_space) - Technical documentation
7. **DevOps** (genie_space) - Git operations and CI/CD

## Notebooks

- `Bronze_Pipeline` - Metadata-driven Auto Loader ingestion (INITIAL + INCREMENTAL)
- `Silver_Pipeline` - Cleansing, SCD2, PII masking, DQ
- `Gold_Pipeline` - KPI aggregations
- `Architect_Agent` - MLflow agent for architecture design
- `Data_Engineer_Agent` - MLflow agent for pipeline code
- `Domain_Expert_Setup` - UC volume with P&C reference docs
- `Analyst_Genie_Setup` - Genie Space over Gold tables
- `Supervisor_Agent_Setup` - Multi-agent orchestration
- `Orchestrator` - Top-level walkthrough and demo script

## SQL DDL

- `sql/01_catalog_schemas.sql` - Catalog and schema creation
- `sql/02_bronze_tables.sql` - Bronze table DDL
- `sql/04_gold_tables.sql` - Gold table DDL

## KPI Formulas

- **Loss Ratio** = Incurred Losses / Earned Premium
- **Combined Ratio** = (Losses + Expenses) / Earned Premium
- **Claim Frequency** = Claim Count / Exposure Units
- **Claim Severity** = Incurred Losses / Claim Count
- **Retention Rate** = Renewed / (Renewed + Cancelled)

## Repository

- **GitHub**: https://github.com/vedavyasgoparaju/pc-insurance-medallion
- **Databricks Git Folder**: /Repos/vedavyas.goparaju/pc-insurance-medallion
