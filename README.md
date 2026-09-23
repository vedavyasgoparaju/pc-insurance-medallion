# P&C Insurance Medallion Architecture on Databricks

A multi-layered data platform for Property & Casualty Insurance built on Databricks, featuring Bronze/Silver/Gold pipelines, data quality functions, and a 7-agent multi-agent system.

## Architecture

### Unity Catalog Structure
- **Catalog**: `pc_insurance`
- **Schemas**: `bronze`, `silver`, `gold`, `reference`, `dq`

### Bronze Layer (Raw Ingestion)
| Table | Rows | Description |
|-------|------|-------------|
| policies_raw | 1,000 | Raw policy data from Policy Admin System |
| claims_raw | 300 | Raw claims from Claims Management System |
| premiums_raw | 1,200 | Premium transactions from Billing System |
| customers_raw | 500 | Customer data from CRM (PII fields) |
| agents_raw | 50 | Insurance agent data |

### Silver Layer (Conformed Dimensions & Facts)
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

## Multi-Agent System
Supervisor Agent: "P&C Insurance Medallion Architecture Team" with 7 subagents:
1. **Architect** (serving_endpoint) — Architecture design
2. **Data Engineer** (serving_endpoint) — Pipeline code generation
3. **Domain Expert** (volume) — P&C insurance domain knowledge
4. **QA Validator** (uc_function) — Data quality validation
5. **Analyst** (genie_space) — Gold layer KPI queries
6. **Documentation** (genie_space) — Technical documentation
7. **DevOps** (genie_space) — Git operations & CI/CD

## Notebooks
- `Bronze_Pipeline` — Data generation and ingestion
- `Silver_Pipeline` — Cleansing, SCD2, PII masking, DQ
- `Gold_Pipeline` — KPI aggregations
- `Architect_Agent` — MLflow agent for architecture design
- `Data_Engineer_Agent` — MLflow agent for pipeline code
- `Domain_Expert_Setup` — UC volume with P&C reference docs
- `Analyst_Genie_Setup` — Genie Space over Gold tables
- `Supervisor_Agent_Setup` — Multi-agent orchestration
- `Orchestrator` — Top-level walkthrough and demo script

## KPI Formulas
- **Loss Ratio** = Incurred Losses / Earned Premium
- **Combined Ratio** = (Losses + Expenses) / Earned Premium
- **Claim Frequency** = Claim Count / Exposure Units
- **Claim Severity** = Incurred Losses / Claim Count
- **Retention Rate** = Renewed / (Renewed + Cancelled)
