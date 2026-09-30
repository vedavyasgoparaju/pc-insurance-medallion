# P&C Insurance Medallion — Databricks Services Reference

**Version:** 1.0  
**Last Updated:** 2026-09-29  
**Purpose:** Comprehensive reference for every Databricks service used in this project, what it does, and how it's applied.

---

## Table of Contents

1. [Data Platform Services](#1-data-platform-services)
   - [Unity Catalog](#unity-catalog)
   - [Delta Lake](#delta-lake)
   - [UC SQL Functions (Persistent)](#uc-sql-functions-persistent)
   - [UC Volumes](#uc-volumes)
2. [AI & Agent Services](#2-ai--agent-services)
   - [Supervisor Agent (Agent Bricks)](#supervisor-agent-agent-bricks)
   - [MLflow Model Registry](#mlflow-model-registry)
   - [Model Serving Endpoints](#model-serving-endpoints)
   - [Knowledge Assistant (Agent Bricks)](#knowledge-assistant-agent-bricks)
   - [AI/BI Genie Spaces](#aibi-genie-spaces)
   - [Databricks Apps (MCP)](#databricks-apps-mcp)
   - [LangGraph (Autonomous Swarm)](#langgraph-autonomous-swarm)
3. [Governance & Security](#3-governance--security)
   - [PII Masking (Column Masks)](#pii-masking-column-masks)
   - [UC Network Rules](#uc-network-rules)
   - [Access Control Model](#access-control-model)
4. [Orchestration & Deployment](#4-orchestration--deployment)
   - [Lakeflow Jobs](#lakeflow-jobs)
   - [Declarative Automation Bundles (DAB)](#declarative-automation-bundles-dab)
   - [Git Integration](#git-integration)
5. [Data Engineering Features](#5-data-engineering-features)
   - [SCD Type 2](#scd-type-2)
   - [Delta Optimization](#delta-optimization)
   - [Metadata-Driven Pipelines](#metadata-driven-pipelines)
6. [Service Summary Table](#service-summary-table)

---

## 1. Data Platform Services

### Unity Catalog

**What it is:** Databricks' unified governance layer for all data assets — catalogs, schemas, tables, volumes, and functions. Provides centralized access control, auditing, and lineage.

**How it's used here:**

A single catalog `pc_insurance` holds all project objects across 6 schemas:

| Schema | Purpose | Key Objects |
|---|---|---|
| `bronze` | Raw ingestion | 5 tables (policies_raw, claims_raw, premiums_raw, customers_raw, agents_raw) |
| `silver` | Cleansed & conformed | 4 dims (SCD2), 2 facts, 1 date_dim |
| `gold` | Business KPIs | 6 pre-aggregated metric tables |
| `reference` | Config & audit | silver_transformation_config, gold_metric_config, audit & reconciliation tables |
| `dq` | Data quality | 8 persistent DQ SQL functions (incl. `pipeline_health_score`), 1 results table |
| `metadata` | Swarm infrastructure | 7 toolkit functions, 3 tables (mapping_documents, swarm_fix_history, health_monitor_log), 1 volume |

**Key artifacts:**
- Catalog: `pc_insurance`
- Created by: `vedavyas.goparaju@gmail.com`
- Security: Catalog → Schema → Table → Volume level grants

**Where configured:** Provisioned by `swarm/PC_Insurance_Swarm_Setup.py` (metadata schema) and pipeline notebooks (bronze/silver/gold). Governance model documented in `docs/ARCHITECTURE.md` → Unity Catalog Structure.

---

### Delta Lake

**What it is:** The open-source storage layer that brings ACID transactions, schema enforcement, and time travel to data lakes. The default table format for all Databricks tables.

**How it's used here:**

All tables in `pc_insurance` (bronze, silver, gold, reference) are Delta Lake managed tables. Key Delta features applied:

- **ACID Transactions**: Bronze append-only, Silver/Gold upserts via `MERGE INTO`
- **Time Travel**: `DESCRIBE HISTORY` and `RESTORE` available on all tables
- **Schema Enforcement**: `ALTER TABLE ADD COLUMNS` for evolution
- **OPTIMIZE**: Compaction via `spark.sql("OPTIMIZE table_name")` in `utils/common_utils.py`
- **ZORDER**: Co-locate frequently filtered columns via `OPTIMIZE ... ZORDER BY (col)`

**Where configured:** All pipeline notebooks (`pipelines/Bronze_Pipeline.py`, `pipelines/Silver_Pipeline_Metadata.py`, `pipelines/Gold_Pipeline.py`). Optimization utilities in `utils/common_utils.py`.

---

### UC SQL Functions (Persistent)

**What it is:** SQL functions stored in Unity Catalog that persist across sessions. Callable from any Spark SQL query, any notebook, or any job — like database stored functions but serverless.

**How it's used here:**

Two sets of persistent UC functions power the platform:

**Toolkit Functions** (7 functions in `pc_insurance.metadata`):

| Function | Purpose |
|---|---|
| `read_mapping_document(x_center, layer)` | Retrieves active mapping JSON from mapping_documents |
| `get_pipeline_error_log(run_id)` | Fetches lineage from system.access.table_lineage |
| `update_mapping_document(x_center, layer, payload)` | Returns SQL plan to update mapping metadata |
| `execute_sandbox_metadata_run(x_center, sandbox_catalog)` | Returns SQL plan for sandbox catalog creation + mapping clone |
| `update_uc_catalog_comments(table_name, comments)` | Returns SQL plan for updating UC column comments |
| `write_technical_markdown_doc(file_path, content)` | Returns execution plan for writing docs to UC volume or workspace |
| `trigger_pipeline_repair(run_id)` | Returns REST API plan for triggering job repair-run |

All use `CREATE OR REPLACE FUNCTION ... RETURN to_json(named_struct(...))` syntax — they return JSON execution plans (not direct execution) for safety.

**DQ Functions** (7 functions in `pc_insurance.dq`):

| Function | Purpose |
|---|---|
| `check_policy_exists(policy_id)` | Validates policy ID exists in dimension |
| `check_claim_status(status)` | Validates claim status code |
| `check_premium_positive(amount)` | Validates premium > 0 |
| `check_loss_ratio(ratio)` | Validates loss ratio <= 2.0 |
| `check_not_null(column, value)` | Generic NOT NULL check |
| `check_date_order(start, end)` | Validates date chronological order |
| `calculate_dq_score(total, failed)` | Returns 0.0–1.0 DQ score |
| `pipeline_health_score()` | Composite health score: DQ (40%) + Freshness (25%) + Reconciliation (20%) + Error Rate (15%) |

**Where registered:**
- Toolkit: `swarm/PC_Insurance_Toolkit_Functions_Registration.py` (Job 1 task: `toolkit_functions_setup`)
- DQ: `swarm/PC_Insurance_DQ_Functions_Setup.py` (Job 1 task: `dq_functions_setup`)

---

### UC Volumes

**What it is:** Unity Catalog-managed storage for files (non-tabular data). Provides governed access to documents, binaries, and arbitrary file content alongside tabular data.

**How it's used here:**

| Volume | Path | Purpose |
|---|---|---|
| `technical_docs` | `/Volumes/pc_insurance/metadata/technical_docs/` | Swarm-generated documentation: `post_mortems/`, `schema_docs/`, `escalations/` |
| `pc_domain_docs` | `/Volumes/pc_insurance/reference/pc_domain_docs/` | P&C insurance reference documents for the Domain Expert Knowledge Assistant (RAG source) |

**Where configured:** `technical_docs` created by `swarm/PC_Insurance_Swarm_Setup.py`. `pc_domain_docs` created by `agents/Domain_Expert_Setup.py`.

### Autonomy Infrastructure Tables & Functions

**What they are:** UC tables and functions that support the self-healing swarm's circuit breaker, fix knowledge base, and health monitoring.

| Artifact | Schema | Purpose |
|---|---|---|
| `dq_validation_results` | `pc_insurance.dq` | Individual DQ rule validation outcomes (partitioned by table_name) |
| `swarm_fix_history` | `pc_insurance.metadata` | Tracks every autonomous fix attempt with `circuit_breaker_triggered` column |
| `health_monitor_log` | `pc_insurance.metadata` | Pipeline health metrics over time (health_score, stale tables, DQ pass rate, swarm success rate) |
| `pipeline_health_score()` | `pc_insurance.dq` | Composite health score function: DQ (40%) + Freshness (25%) + Reconciliation (20%) + Error Rate (15%) |

**Where configured:** `swarm/PC_Insurance_Autonomy_Infrastructure_Setup.py` (Job 1 task: `autonomy_infrastructure_setup`). Uses `CREATE TABLE IF NOT EXISTS` for idempotent deployment.

---

## 2. AI & Agent Services

### Supervisor Agent (Agent Bricks)

**What it is:** Agent Bricks Supervisor Agent — a multi-tool AI agent that orchestrates specialized subagents and tools. Routes user requests to the right tool, synthesizes responses, and enforces anti-routing rules.

**How it's used here:**

The Supervisor Agent is the central orchestrator of the multi-agent system. It has 7 registered tools:

1. Unified Agent (subagent: architecture design + pipeline code, merged)
2. Domain Expert Agent (subagent)
3. Analyst Agent (subagent)
4. DevOps Agent (subagent)
5. QA Validator (UC function)
6. Documentation Agent (subagent)
7. Workspace-Actions MCP App (tool)

**Anti-routing rules enforced:**
- KPI questions → Analyst only (never DevOps)
- Git execution → Workspace-Actions (DevOps is guidance only)
- Architecture design AND code implementation → Unified Agent (not DevOps, not Analyst)

**Key artifacts:**
- Endpoint: `mas-fc596f26-endpoint` (status: READY)
- Agent ID: `fc596f26-066a-464d-94d9-9fc472b027dc`
- Display name: "P&C Insurance Medallion Architecture Team"

**Where configured:** `agents/Supervisor_Agent_Setup.py` (Job 1 task 9: `supervisor_agent_setup`, depends on tasks 1-7).

---

### MLflow Model Registry

**What it is:** Databricks' managed model registry for tracking, versioning, and deploying ML models. Models are registered as Python functions (`pyfunc`) and can be served via endpoints.

**How it's used here:**

A unified AI agent is registered as an MLflow `ChatAgent` model:

| Model | Experiment | Purpose |
|---|---|---|
| `pc_unified_agent` | `/Users/.../pc_insurance_agents` | Unified Architect & Data Engineer agent (Llama 3.3 70B, streaming supported) |

The agent:
1. Extends `mlflow.pyfunc.ChatAgent` (supports both `predict` and `predict_stream`)
2. Registered via `mlflow.start_run()` + `mlflow.pyfunc.log_model()` with `ChatAgent` signature
3. Calls `databricks-meta-llama-3-3-70b-instruct` via `mlflow.deployments` client
4. Deployed as serving endpoint `pc_insurance_agent` (scale-to-zero, streaming enabled)

**Where configured:** `agents/Unified_Insurance_Agent.py` (Job 1 task: `architect_agent`).

---

### Model Serving Endpoints

**What it is:** Serverless model serving infrastructure that hosts MLflow models behind REST API endpoints. Supports scale-to-zero for cost efficiency.

**How it's used here:**

| Endpoint | Model | Workload | Scale |
|---|---|---|---|
| `pc_insurance_agent` | Unified Agent (Architect + Data Engineer merged) | Small | Scale-to-zero, streaming enabled |

Created via `w.serving_endpoints.create()` with `EndpointCoreConfigInput` and `ServedModelInput`. The Supervisor Agent calls these endpoints to route architecture and engineering questions.

**Where configured:** `agents/Unified_Insurance_Agent.py`. DAB variable `supervisor_endpoint` in `databricks.yml`.

---

### Knowledge Assistant (Agent Bricks)

**What it is:** Agent Bricks Knowledge Assistant — a document-grounded RAG (Retrieval-Augmented Generation) chatbot that answers questions from your documents with citations. Uses Instructed Retriever for semantic search.

**How it's used here:**

The **Domain Expert Agent** is implemented as a Knowledge Assistant:
- **Knowledge source**: UC Volume `pc_insurance.reference.pc_domain_docs` containing P&C insurance reference documents
- **Coverage**: Loss ratios, combined ratios, frequency/severity, policy lifecycle, claims processing, reserving, NAIC requirements
- **Role**: Answers domain-specific questions routed by the Supervisor Agent
- **Agent type**: Senior-level P&C insurance SME

Created programmatically via the Databricks SDK (`w.agents.create_knowledge_assistant()` or equivalent API).

**Where configured:** `agents/Domain_Expert_Setup.py` (Job 1 task: `domain_expert_setup`).

---

### AI/BI Genie Spaces

**What it is:** AI/BI Genie Spaces provide natural-language querying of data — users ask questions in plain English and Genie translates them to SQL against registered tables. Uses table/column comments for query routing.

**How it's used here:**

Two Genie Spaces serve as AI agents:

| Genie Space | Agent Role | Tables | Purpose |
|---|---|---|---|
| `PC_Insurance_Analyst` | Analyst Agent | 6 Gold layer tables | Business KPI queries (loss ratio, retention, growth) |
| (DevOps Genie) | DevOps Agent | N/A | Git/CI/CD guidance (read-only, cannot execute) |

The Analyst Genie Space:
- All 6 Gold tables registered with rich column-level comments
- Example queries seeded for Genie training
- Answers: "What's the loss ratio by LOB?", "Show retention by agent", etc.

**Where configured:** `agents/Analyst_Genie_Setup.py` (Job 1 task: `analyst_genie_setup`).

---

### Databricks Apps (MCP)

**What it is:** Databricks Apps — serverless containerized applications that run on Databricks. Can expose MCP (Model Context Protocol) servers that AI agents call as tools.

**How it's used here:**

The **Workspace-Actions** MCP app (`pc-insurance-workspace-actions`) is the execution engine for the Supervisor Agent:

| Capability | Description |
|---|---|
| Git operations | Commit, push, pull via subprocess CLI |
| File writes | Write/update notebooks and workspace files |
| SQL execution | Run SQL statements against UC |
| Job triggering | Trigger Job 1 (agent setup) or Job 2 (data pipeline) |
| Job monitoring | Check run status, get run output |

**Key distinction:** The DevOps Agent provides *guidance* on Git/CI/CD. The MCP app *executes* the actual operations. This separation is enforced by anti-routing rules.

**Where configured:** `agents/MCP_App_Deploy.py` (Job 1 task: `mcp_app_deploy`). App source: `app/app.py`. DAB resource: `databricks.yml` references the app.

---

### LangGraph (Autonomous Swarm)

**What it is:** LangGraph is a framework for building stateful, multi-agent workflows with directed graphs. It enables complex agent orchestration with state management, conditional routing, and checkpointing.

**How it's used here:**

The **Autonomous Agent Swarm** is a LangGraph-based self-healing system with 6 agents in a Plan-Execute-Verify-Deploy loop:

| Swarm Agent | Role | Key API |
|---|---|---|
| Supervisor | Orchestrates the swarm (Llama 3.3 70B) | LangGraph StateGraph |
| Triage | Fetches real error logs | `jobs.get_run()`, `jobs.get_run_output()` |
| Business Analyst | Updates mapping metadata | `pc_insurance.metadata.mapping_documents` |
| Data Engineer | Applies fixes using toolkit functions | 7 UC toolkit SQL functions |
| QA | Validates fixes | `pc_insurance.dq.calculate_dq_score` |
| Deployment | Triggers pipeline repair | `jobs.repair_run()` |

**Guardrails:**
- Max 5 ReAct iterations per agent
- $25 token budget per swarm run
- UC Network Rules for HTTPS egress
- Sandbox isolation (prefix `dev_sandbox_`)
- **Circuit Breaker**: Halts swarm after 3+ failed fix attempts on same `error_signature` within 6 hours
- **Fix Knowledge Base**: Queries `swarm_fix_history` for similar past successful fixes before attempting new ones
- **RollbackManager**: Automated Delta `RESTORE` if DQ score drops >10% post-fix
- **DependencyChecker**: Verifies downstream table freshness after upstream fixes via UC lineage

**Trigger paths:**
1. **Automatic**: Job 2 task fails → `autonomous_swarm` triggers (`run_if=AT_LEAST_ONE_FAILED`)
2. **Manual**: Standalone job `PC_Insurance_Autonomous_Swarm` (ID: `774564996988013`)
3. **Interactive**: Supervisor Agent chat routes to `workspace-actions` MCP tool

**Where configured:** `swarm/PC_Insurance_Autonomous_Agent_Swarm.py` (24 cells, 3000+ lines). Uses `langgraph.graph.StateGraph`, `MemorySaver` for checkpointing.

---

### Health Monitor (Lakeflow Job)

**What it is:** A scheduled Lakeflow Job that runs every 6 hours to assess pipeline health, DQ validation rates, swarm fix success rates, and alert on anomalies.

**How it's used here:**

The Health Monitor job computes a composite health score using the `pipeline_health_score()` UC function and logs results to `pc_insurance.metadata.health_monitor_log`:

| Check | Description |
|---|---|
| Table freshness | Identifies stale tables (no updates in expected window) |
| DQ pass rate | Validates DQ validation results trend |
| Swarm success rate | Tracks fix success vs. failure ratio from `swarm_fix_history` |
| Circuit breaker status | Checks if any circuit breakers were triggered |
| Rollback events | Counts rollback operations in recent window |

**Key artifacts:**
- Job ID: `88172905444926`
- Job name: `PC_Insurance_Health_Monitor`
- Schedule: Every 6 hours
- Notebook: `swarm/PC_Insurance_Health_Monitor`

**Where configured:** `swarm/PC_Insurance_Health_Monitor` notebook. Job created via Lakeflow Jobs API.

---

## 3. Governance & Security

### PII Masking (Column Masks)

**What it is:** Unity Catalog column masks are row-level functions that transform sensitive data at query time. Users see masked values without the underlying data ever being exposed.

**How it's used here:**

- **Customer names** are masked in `pc_insurance.silver.customer_dim`
- **Method**: First character + asterisks (e.g., "John Smith" → "J********")
- **Implementation**: `regex_replace` in Silver Pipeline transformation
- **Compliance**: GDPR, CCPA ready

**Where configured:** `pipelines/Silver_Pipeline_Metadata.py` (PII masking transformation step). Documented in `docs/ARCHITECTURE.md` → PII Masking section.

---

### UC Network Rules

**What it is:** Unity Catalog Network Rules control outbound network access (egress) from UC-managed compute. Restrict which external endpoints agents and functions can call.

**How it's used here:**

The autonomous swarm operates under UC Network Rules for HTTPS egress:
- Swarm agents can only call approved LLM endpoints (Llama 3.3 70B)
- External API calls are restricted to Databricks internal APIs
- Prevents unauthorized data exfiltration during self-healing operations

**Where configured:** `swarm/PC_Insurance_Autonomous_Agent_Swarm.py` (Cell 17: Security, Network & Budget Guardrails). Documented in `docs/ARCHITECTURE.md` → Guardrails.

---

### Access Control Model

**What it is:** Unity Catalog's layered permission model: Catalog → Schema → Table → Volume, with `USE`, `SELECT`, `MODIFY`, `READ VOLUME`, `WRITE VOLUME` privileges.

**How it's used here:**

| Level | Grant | Recipient |
|---|---|---|
| Catalog | `USE CATALOG` | All users |
| Catalog | `CREATE SCHEMA` | Admins only |
| Schema | `USE SCHEMA` | All users |
| Schema | `CREATE TABLE` | Service principals (pipelines) |
| Table | `SELECT` | Analysts, BI tools |
| Table | `MODIFY` | Service principals (pipelines) |
| Volume | `READ VOLUME` | Domain Expert Agent |
| Volume | `WRITE VOLUME` | Admins only |

**Where configured:** Documented in `docs/ARCHITECTURE.md` → Security Model.

---

## 4. Orchestration & Deployment

### Lakeflow Jobs

**What it is:** Databricks Lakeflow Jobs orchestrate notebooks, Python scripts, and pipelines as tasks with dependencies, retries, schedules, and alerting.

**How it's used here:**

Two jobs with distinct purposes:

| Job | Name | ID | Purpose |
|---|---|---|---|
| 1 | `PC_Insurance_Agent_Setup` | `820361677269451` | Agent setup (run once): 8 parallel tasks + 1 dependent |
| 2 | `PC_Insurance_Data_Pipeline` | `894776717783668` | Data pipeline: Bronze → Silver → Gold → Swarm (on failure) |

**Job 1 tasks (8 parallel + 1 dependent):**

| Task | Description |
|---|---|
| `architect_agent` | Register unified MLflow ChatAgent model + create serving endpoint `pc_insurance_agent` |
| `domain_expert_setup` | Create UC volume + upload docs + create Knowledge Assistant |
| `analyst_genie_setup` | Add Gold table comments + create Genie Space |
| `swarm_setup` | Provision swarm infrastructure (catalog, schema, tables, volume, seed data) |
| `dq_functions_setup` | Register 7 DQ SQL functions |
| `toolkit_functions_setup` | Register 7 toolkit SQL functions |
| `mcp_app_deploy` | Deploy MCP app `pc-insurance-workspace-actions` |
| `supervisor_agent_setup` | Create Supervisor Agent with all 7 tools (depends on 1-7) |

**Job 2 flow:**
```
Bronze_Pipeline → Silver_Pipeline → Gold_Pipeline → autonomous_swarm (if any fails)
```
- Parameter: `load_type` (INITIAL or INCREMENTAL)
- All tasks idempotent (safe to re-run)

**Where configured:** `databricks.yml` (DAB), Job definitions in Databricks UI. Job 1 task notebooks in `agents/` and `swarm/`. Job 2 notebooks in `pipelines/`.

---

### Declarative Automation Bundles (DAB)

**What it is:** Declarative Automation Bundles (formerly Databricks Asset Bundles) define infrastructure-as-code for Databricks projects — jobs, pipelines, apps, and resources in YAML, deployed via CLI.

**How it's used here:**

- **Config file**: `databricks.yml`
- **Bundle name**: `InsuranceModel`
- **Active target**: `dev` (single environment)
- **Variables**: `sql_warehouse_id`, `supervisor_endpoint`, `workspace_root`, `allowed_roots`, `repo_path`
- **Deploy command**: `databricks bundle deploy -t dev`

**Where configured:** `databricks.yml` at repo root. Resource definitions in `resources/*.yml`.

---

### Git Integration

**What it is:** Databricks Git Integration (Repos) connects workspace folders to Git repositories for version control, branching, and CI/CD.

**How it's used here:**

- **Repo path**: `/Repos/vedavyas.goparaju/pc-insurance-medallion`
- **Git provider**: GitHub
- **Branch**: `main`
- **Commit/push**: Via MCP app (`app/app.py`) which runs `git` subprocess CLI
- **CI/CD**: DAB deploy triggered after code changes are committed

**Where configured:** Repo folder in Databricks workspace. Git credentials in user Linked Accounts.

---

## 5. Data Engineering Features

### SCD Type 2

**What it is:** Slowly Changing Dimension Type 2 tracks historical changes to dimension records by maintaining multiple versions with effective dates and current flags.

**How it's used here:**

Applied to 4 Silver layer dimensions:

| Dimension | Business Key | SCD2 Columns |
|---|---|---|
| `policy_dim` | `policy_id` | `is_current`, `effective_from`, `effective_to` |
| `claim_dim` | `claim_id` | `is_current`, `effective_from`, `effective_to` |
| `customer_dim` | `customer_id` | `is_current`, `effective_from`, `effective_to` |
| `agent_dim` | `agent_id` | `is_current`, `effective_from`, `effective_to` |

Implemented via `MERGE INTO` with deduplication (latest record by `ingestion_timestamp`) and `Window.partitionBy(business_key).orderBy(ingestion_timestamp.desc())`.

**Where configured:** `pipelines/Silver_Pipeline_Metadata.py`. Config in `pc_insurance.reference.silver_transformation_config` (column: `scd2_enabled`).

---

### Delta Optimization

**What it is:** Delta Lake optimization features for query performance: Liquid Clustering, Z-Ordering, OPTIMIZE compaction, and partitioning.

**How it's used here:**

| Feature | Where Applied | Implementation |
|---|---|---|
| Partitioning | Bronze: by `ingestion_date` | `spark.sql("CREATE TABLE ... PARTITIONED BY (ingestion_date)")` |
| Partitioning | Silver: by `state`, `line_of_business` | Config in `silver_transformation_config.partition_columns` |
| Partitioning | Gold: by `year_month` | Config in `gold_metric_config` |
| OPTIMIZE | All tables | `spark.sql("OPTIMIZE table_name")` in `utils/common_utils.py` |
| ZORDER | Frequently filtered columns | `spark.sql("OPTIMIZE table_name ZORDER BY (col1, col2)")` |
| Liquid Clustering | Key dimensions | Mentioned in ARCHITECTURE.md (planned) |

**Where configured:** `utils/common_utils.py` (lines 254-259), `pipelines/Silver_Pipeline_Metadata.py`, `sql/03_silver_transformation_config.sql`.

---

### Metadata-Driven Pipelines

**What it is:** A design pattern where transformation logic is stored as configuration in metadata tables rather than hardcoded in pipeline code. Pipelines read config at runtime to determine what to transform and how.

**How it's used here:**

| Config Table | Schema | Drives |
|---|---|---|
| `silver_transformation_config` | `pc_insurance.reference` | Silver layer: source/target, SCD2/fact/dedup, business keys, cleansing rules, execution order |
| `gold_metric_config` | `pc_insurance.reference` | Gold layer: metric name, target table, source tables, dimension/measure columns, calculation logic, refresh frequency |

**Audit tables** track every run:
- `silver_load_audit`: run status, row counts, execution time, errors
- `silver_reconciliation`: source vs target count validation
- `gold_load_audit`: refresh status, metric values, DQ scores

**Where configured:** `pipelines/Silver_Pipeline_Metadata.py` (reads `silver_transformation_config`), `pipelines/Gold_Pipeline.py` (reads `gold_metric_config`). Config DDL in `sql/` directory.

---

## Service Summary Table

| # | Service | Category | Key Artifact | Job 1 Task |
|---|---|---|---|---|
| 1 | Unity Catalog | Data Platform | Catalog `pc_insurance` (6 schemas) | `swarm_setup` |
| 2 | Delta Lake | Data Platform | All managed tables | N/A (pipelines) |
| 3 | UC SQL Functions | Data Platform | 7 toolkit + 7 DQ functions | `toolkit_functions_setup`, `dq_functions_setup` |
| 4 | UC Volumes | Data Platform | `technical_docs`, `pc_domain_docs` | `swarm_setup`, `domain_expert_setup` |
| 5 | Supervisor Agent | AI & Agents | Endpoint `fc596f26-066a-464d-94d9-9fc472b027dc` | `supervisor_agent_setup` (task 9) |
| 6 | MLflow | AI & Agents | Model `pc_unified_agent` (ChatAgent, v2) | `architect_agent` |
| 7 | Model Serving | AI & Agents | Endpoint `pc_insurance_agent` (scale-to-zero, streaming) | `architect_agent` |
| 8 | Knowledge Assistant | AI & Agents | Domain Expert Agent (RAG) | `domain_expert_setup` |
| 9 | Genie Spaces | AI & Agents | `PC_Insurance_Analyst`, DevOps Genie | `analyst_genie_setup` |
| 10 | Databricks Apps (MCP) | AI & Agents | `pc-insurance-workspace-actions` | `mcp_app_deploy` |
| 11 | LangGraph | AI & Agents | Autonomous swarm (6 agents) | `swarm_setup` (provisions infra) |
| 12 | PII Masking | Governance | `customer_dim` name masking | N/A (Silver pipeline) |
| 13 | UC Network Rules | Governance | Swarm HTTPS egress control | N/A (swarm runtime) |
| 14 | Lakeflow Jobs | Orchestration | Job 1 (`820361677269451`), Job 2 (`894776717783668`) | N/A (the jobs themselves) |
| 15 | DAB | Deployment | `databricks.yml` (dev target) | N/A (deploy infrastructure) |
| 16 | Git Integration | Deployment | Repo `/Repos/.../pc-insurance-medallion` | N/A (MCP app handles commits) |
| 17 | SCD Type 2 | Data Engineering | 4 Silver dimensions | N/A (Silver pipeline) |
| 18 | Delta Optimization | Data Engineering | OPTIMIZE, ZORDER, partitioning | N/A (utils + pipelines) |
| 19 | Metadata-Driven | Data Engineering | `silver_transformation_config`, `gold_metric_config` | N/A (pipelines read config) |

---

## References

- [Databricks Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks Delta Lake](https://docs.databricks.com/delta/index.html)
- [Databricks MLflow](https://docs.databricks.com/mlflow/index.html)
- [Databricks Model Serving](https://docs.databricks.com/machine-learning/model-serving/index.html)
- [Agent Bricks Supervisor Agent](https://docs.databricks.com/en/agent-bricks/supervisor-agent/index.html)
- [Agent Bricks Knowledge Assistant](https://docs.databricks.com/en/agent-bricks/knowledge-assistant/index.html)
- [AI/BI Genie](https://docs.databricks.com/genie/index.html)
- [Databricks Apps](https://docs.databricks.com/en/databricks-apps/index.html)
- [Lakeflow Jobs](https://docs.databricks.com/jobs/index.html)
- [Declarative Automation Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [LangGraph](https://langchain-ai.github.io/langgraph/)

---

**Document Version:** 1.0  
**Last Updated:** 2026-09-28  
**Maintained By:** Data Engineering Team
