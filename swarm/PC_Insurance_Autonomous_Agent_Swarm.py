# Databricks notebook source
# DBTITLE 1,Architecture Overview
# MAGIC %md
# MAGIC # Autonomous P&C Insurance Data Engineering Agent Swarm
# MAGIC
# MAGIC ## Architecture Overview
# MAGIC
# MAGIC This notebook implements a **100% autonomous, metadata-driven multi-agent swarm** for managing a P&C Insurance Medallion Architecture (Bronze → Silver → Gold) on Databricks.
# MAGIC
# MAGIC ### Agent Swarm Topology (LangGraph State Graph)
# MAGIC
# MAGIC ```
# MAGIC                           ┌──────────────┐
# MAGIC                           │  Supervisor  │
# MAGIC                           │    Agent      │
# MAGIC                           └──────┬───────┘
# MAGIC                                  │
# MAGIC                  ┌───────────────┼───────────────┐
# MAGIC                  ▼               ▼               ▼
# MAGIC           ┌──────────┐  ┌──────────┐  ┌──────────────┐
# MAGIC           │  Triage   │  │  BA Agent │  │  Deployment  │
# MAGIC           │  Agent    │  │ (Mapping  │  │    Agent      │
# MAGIC           └────┬─────┘  │  Owner)   │  └──────┬───────┘
# MAGIC                │        └─────┬────┘         │
# MAGIC                ▼              ▼              │
# MAGIC           ┌──────────┐  ┌──────────┐         │
# MAGIC           │  Data    │  │   QA &   │◄────────┘
# MAGIC           │ Engineer │  │Validation│
# MAGIC           │  Agent   │  │  Agent   │
# MAGIC           └──────────┘  └──────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Plan-Execute-Verify-Deploy Loop
# MAGIC
# MAGIC 1. **Plan** — Supervisor reads alerts, Triage fetches error context
# MAGIC 2. **Execute** — BA updates mapping metadata, Data Engineer applies changes to UC
# MAGIC 3. **Verify** — QA validates DQ via DLT expectations / Great Expectations in sandbox
# MAGIC 4. **Deploy** — Deployment Agent triggers repair, commits to Git, writes docs
# MAGIC
# MAGIC ### Key Design Principles
# MAGIC
# MAGIC - **Metadata-driven**: Agents never modify raw pipeline code — only mapping documents and metadata configs
# MAGIC - **Sandbox isolation**: All changes tested in `dev_staging_<run_id>` before production
# MAGIC - **Loop protection**: Max 5 ReAct iterations or token-dollar cap before human escalation
# MAGIC - **Auto-documentation**: Every change triggers UC comment updates + Markdown runbook generation
# MAGIC
# MAGIC ### Infrastructure Endpoints
# MAGIC
# MAGIC | Endpoint | Purpose |
# MAGIC |---------|--------|
# MAGIC | Mosaic AI Model Serving | LLM reasoning for Supervisor + Sub-agents |
# MAGIC | Serverless SQL Warehouse | Diagnostic SQL + metadata queries |
# MAGIC | Databricks Workspace REST API | Job run state, repair triggers |
# MAGIC | Unity Catalog Functions | Secure Python tool runtime |

# COMMAND ----------

# DBTITLE 1,Install LangGraph
# MAGIC %pip install langgraph langchain-core

# COMMAND ----------

# DBTITLE 1,Imports & Configuration
# ═══════════════════════════════════════════════════════════════
# Cell 2: Imports, Configuration & Constants
# ═══════════════════════════════════════════════════════════════

import os
import json
import time
import uuid
import logging
import hashlib
import datetime
from typing import TypedDict, Literal, Optional, Any, Dict, List, Annotated
from dataclasses import dataclass, field, asdict
from enum import Enum

# Databricks SDK for REST API + Unity Catalog interactions
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as jobs_service
from databricks.sdk.service.catalog import FunctionInfo, FunctionParameterInfo, FunctionParameterType

# LangGraph for multi-agent state graph orchestration
# (Install in cluster: %pip install langgraph langchain-core)
try:
    from langgraph.graph import StateGraph, END, START
    from langgraph.graph.state import CompiledStateGraph
    from langgraph.checkpoint.memory import MemorySaver
    HAS_LANGGRAPH = True
except ImportError:
    HAS_LANGGRAPH = False
    logging.warning("LangGraph not installed. Install with: %pip install langgraph")

# ── Global Configuration ──────────────────────────────────────────

WORKSPACE_CLIENT = WorkspaceClient()

# Model Serving endpoint names (provisioned in Databricks workspace)
SUPERVISOR_LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
SUBAGENT_LLM_ENDPOINT   = "databricks-meta-llama-3-1-8b-instruct"

# Serverless SQL Warehouse for diagnostic queries
SERVERLESS_WAREHOUSE_ID = os.environ.get("PC_INSURANCE_WAREHOUSE_ID", "")

# Unity Catalog metadata store locations
METADATA_CATALOG    = "pc_insurance"
METADATA_SCHEMA     = "metadata"
PROD_CATALOG        = "pc_insurance"
STAGING_CATALOG_PREFIX = "dev_staging_"

# Documentation volume path (UC Volume)
DOCS_VOLUME_PATH = "/Volumes/pc_insurance/metadata/technical_docs"

# Git configuration for metadata + docs sync
GIT_REPO_URL = os.environ.get("PC_GIT_REPO_URL", "")
GIT_BRANCH_PREFIX = "auto-metadata-fix"

# ── Guardrail Constants ──────────────────────────────────────────

MAX_REACT_ITERATIONS = 5
MAX_TOKEN_BUDGET_USD = 25.0  # Dollar cap per autonomous cycle
TOKEN_COST_PER_1K_INPUT = 0.005   # Adjust to your model pricing
TOKEN_COST_PER_1K_OUTPUT = 0.015

# Insurance X-Center source systems
X_CENTERS = ["PolicyCenter", "ClaimCenter", "BillingCenter", "MGA_Feed"]
MEDALLION_LAYERS = ["bronze", "silver", "gold"]

# ── Logging ──────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("PC_AgentSwarm")

# COMMAND ----------

# DBTITLE 1,Infrastructure Setup & Seeding
# Cell 2b: Infrastructure Setup — Catalog, Tables, Volume, Seeding
# Creates the full pc_insurance_dev infrastructure.

import json
import datetime

print("Creating pc_insurance_dev infrastructure...")

# 1. Catalog + Schema
spark.sql("CREATE CATALOG IF NOT EXISTS pc_insurance_dev")
spark.sql("CREATE SCHEMA IF NOT EXISTS pc_insurance.metadata")
print("Catalog + schema created")

# 2. mapping_documents table
spark.sql("""
    CREATE TABLE IF NOT EXISTS pc_insurance.metadata.mapping_documents (
        x_center STRING NOT NULL,
        layer STRING NOT NULL,
        version INT NOT NULL,
        is_active BOOLEAN NOT NULL,
        mapping_json STRING,
        business_description STRING,
        updated_by STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    PARTITIONED BY (x_center, layer)
""")
print("mapping_documents table created")

# 3. threshold_controls table
spark.sql("""
    CREATE TABLE IF NOT EXISTS pc_insurance.metadata.threshold_controls (
        metric_name STRING NOT NULL,
        region STRING,
        event_type STRING,
        max_threshold DOUBLE,
        min_threshold DOUBLE,
        effective_start_date DATE,
        effective_end_date DATE,
        is_active BOOLEAN,
        updated_by STRING,
        updated_at TIMESTAMP
    ) USING DELTA
""")
print("threshold_controls table created")

# 4. UC Volume for technical docs
spark.sql("CREATE VOLUME IF NOT EXISTS pc_insurance.metadata.technical_docs")
try:
    dbutils.fs.mkdirs("/Volumes/pc_insurance/metadata/technical_docs/post_mortems")
    dbutils.fs.mkdirs("/Volumes/pc_insurance/metadata/technical_docs/schema_docs")
    dbutils.fs.mkdirs("/Volumes/pc_insurance/metadata/technical_docs/escalations")
except Exception as e:
    print("  (subdir creation note: " + str(e) + ")")
print("UC Volume + subdirectories created")

# 5. Seed initial mapping documents
baseline_mappings = {
    ("PolicyCenter", "bronze"): {
        "column_mappings": {
            "policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_number": {"target_col": "policy_number", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_type": {"target_col": "policy_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "effective_date": {"target_col": "effective_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "expiration_date": {"target_col": "expiration_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "line_of_business": {"target_col": "line_of_business", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "coverage_type": {"target_col": "coverage_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "deductible_amount": {"target_col": "deductible_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "premium_amount": {"target_col": "premium_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "policy_status": {"target_col": "policy_status", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "source_system": {"target_col": "source_system", "data_type": "string", "nullable": False, "default_value": "PolicyCenter", "transformation": None},
            "ingestion_timestamp": {"target_col": "ingestion_timestamp", "data_type": "timestamp", "nullable": False, "default_value": None, "transformation": "current_timestamp()"}
        },
        "transformation_rules": [],
        "business_description": "PolicyCenter Bronze layer - raw ingestion of policy data from Guidewire PolicyCenter. All fields are ACORD-standard."
    },
    ("PolicyCenter", "silver"): {
        "column_mappings": {
            "policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_number": {"target_col": "policy_number", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_type": {"target_col": "policy_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "effective_date": {"target_col": "effective_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "expiration_date": {"target_col": "expiration_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "line_of_business": {"target_col": "line_of_business", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "coverage_type": {"target_col": "coverage_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "deductible_amount": {"target_col": "deductible_amount", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": "COALESCE(deductible_amount, 0)"},
            "premium_amount": {"target_col": "premium_amount", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": "COALESCE(premium_amount, 0)"},
            "net_premium": {"target_col": "net_premium", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": "premium_amount - COALESCE(deductible_amount, 0)"},
            "policy_status": {"target_col": "policy_status", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "source_system": {"target_col": "source_system", "data_type": "string", "nullable": False, "default_value": "PolicyCenter", "transformation": None}
        },
        "transformation_rules": [
            {"rule_name": "deductible_not_null", "rule_type": "not_null", "rule_sql": "COALESCE(deductible_amount, 0)", "applies_to": "deductible_amount", "condition": None},
            {"rule_name": "premium_not_null", "rule_type": "not_null", "rule_sql": "COALESCE(premium_amount, 0)", "applies_to": "premium_amount", "condition": None}
        ],
        "business_description": "PolicyCenter Silver layer - cleansed and conformed policy data. Deductible and premium amounts have NOT NULL constraints with COALESCE fallback to 0."
    },
    ("ClaimCenter", "bronze"): {
        "column_mappings": {
            "claim_id": {"target_col": "claim_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "claim_number": {"target_col": "claim_number", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "claim_status": {"target_col": "claim_status", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "loss_date": {"target_col": "loss_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "reported_date": {"target_col": "reported_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "loss_amount": {"target_col": "loss_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "paid_amount": {"target_col": "paid_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "reserved_amount": {"target_col": "reserved_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "loss_type": {"target_col": "loss_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "region": {"target_col": "region", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "source_system": {"target_col": "source_system", "data_type": "string", "nullable": False, "default_value": "ClaimCenter", "transformation": None},
            "ingestion_timestamp": {"target_col": "ingestion_timestamp", "data_type": "timestamp", "nullable": False, "default_value": None, "transformation": "current_timestamp()"}
        },
        "transformation_rules": [],
        "business_description": "ClaimCenter Bronze layer - raw ingestion of claims data from Guidewire ClaimCenter. Includes loss dates, amounts, and regional attributes."
    },
    ("ClaimCenter", "gold"): {
        "column_mappings": {
            "line_of_business": {"target_col": "line_of_business", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "region": {"target_col": "region", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "quarter": {"target_col": "quarter", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "total_premium": {"target_col": "total_premium", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": "SUM(premium_amount)"},
            "total_losses": {"target_col": "total_losses", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": "SUM(loss_amount)"},
            "loss_ratio": {"target_col": "loss_ratio", "data_type": "decimal(10,4)", "nullable": True, "default_value": None, "transformation": "SUM(loss_amount) / NULLIF(SUM(premium_amount), 0) * 100"},
            "claim_count": {"target_col": "claim_count", "data_type": "int", "nullable": False, "default_value": "0", "transformation": "COUNT(DISTINCT claim_id)"},
            "avg_claim_severity": {"target_col": "avg_claim_severity", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": "AVG(loss_amount)"}
        },
        "transformation_rules": [
            {"rule_name": "loss_ratio_threshold_check", "rule_type": "conditional", "rule_sql": "loss_ratio <= 200", "applies_to": "loss_ratio", "condition": "Default: max 200 pct loss ratio. Override via threshold_controls for catastrophic events."}
        ],
        "business_description": "ClaimCenter Gold layer - quarterly KPIs by LOB including Loss Ratio (max 200 pct threshold, overridable for catastrophic events), claim count, and average severity."
    },
    ("BillingCenter", "bronze"): {
        "column_mappings": {
            "billing_id": {"target_col": "billing_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "billing_type": {"target_col": "billing_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "invoice_amount": {"target_col": "invoice_amount", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": None},
            "commission_amount": {"target_col": "commission_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "payment_status": {"target_col": "payment_status", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "billing_date": {"target_col": "billing_date", "data_type": "date", "nullable": False, "default_value": None, "transformation": None},
            "source_system": {"target_col": "source_system", "data_type": "string", "nullable": False, "default_value": "BillingCenter", "transformation": None},
            "ingestion_timestamp": {"target_col": "ingestion_timestamp", "data_type": "timestamp", "nullable": False, "default_value": None, "transformation": "current_timestamp()"}
        },
        "transformation_rules": [],
        "business_description": "BillingCenter Bronze layer - raw ingestion of billing data from Guidewire BillingCenter. Includes invoices, commissions, and payment status."
    },
    ("MGA_Feed", "bronze"): {
        "column_mappings": {
            "mga_policy_id": {"target_col": "mga_policy_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "group_code": {"target_col": "source_group_code", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "policy_type": {"target_col": "policy_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "deductible_amount": {"target_col": "deductible_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "premium_amount": {"target_col": "premium_amount", "data_type": "decimal(15,2)", "nullable": True, "default_value": None, "transformation": None},
            "coverage_type": {"target_col": "coverage_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "source_system": {"target_col": "source_system", "data_type": "string", "nullable": False, "default_value": "MGA_Feed", "transformation": None},
            "ingestion_timestamp": {"target_col": "ingestion_timestamp", "data_type": "timestamp", "nullable": False, "default_value": None, "transformation": "current_timestamp()"}
        },
        "transformation_rules": [],
        "business_description": "MGA Feed Bronze layer - raw ingestion of third-party Managing General Agent policy data. Includes group_code for distinguishing commercial vs personal lines."
    },
    ("MGA_Feed", "silver"): {
        "column_mappings": {
            "mga_policy_id": {"target_col": "mga_policy_id", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "source_group_code": {"target_col": "source_group_code", "data_type": "string", "nullable": False, "default_value": None, "transformation": None},
            "policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "policy_type": {"target_col": "policy_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None},
            "deductible_amount": {"target_col": "deductible_amount", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": "COALESCE(deductible_amount, 0)"},
            "premium_amount": {"target_col": "premium_amount", "data_type": "decimal(15,2)", "nullable": False, "default_value": "0", "transformation": "COALESCE(premium_amount, 0)"},
            "coverage_type": {"target_col": "coverage_type", "data_type": "string", "nullable": True, "default_value": None, "transformation": None}
        },
        "transformation_rules": [
            {"rule_name": "deductible_not_null", "rule_type": "not_null", "rule_sql": "COALESCE(deductible_amount, 0)", "applies_to": "deductible_amount", "condition": None}
        ],
        "business_description": "MGA Feed Silver layer - cleansed third-party policy data. Deductible_amount has NOT NULL constraint with COALESCE fallback to 0."
    }
}

# Insert baseline mapping documents (version 1)
now_ts = datetime.datetime.now().isoformat()
for (x_center, layer), mapping_content in baseline_mappings.items():
    existing = spark.sql(
        "SELECT COUNT(*) as cnt FROM pc_insurance.metadata.mapping_documents " +
        "WHERE x_center = '" + x_center + "' AND layer = '" + layer + "'"
    ).collect()[0]["cnt"]
    
    if existing == 0:
        mapping_json = json.dumps(mapping_content, indent=2, sort_keys=True).replace("'", "''")
        biz_desc = mapping_content["business_description"].replace("'", "''")
        insert_sql = (
            "INSERT INTO pc_insurance.metadata.mapping_documents " +
            "(x_center, layer, version, is_active, mapping_json, business_description, updated_by, updated_at) " +
            "VALUES ('" + x_center + "', '" + layer + "', 1, true, '" +
            mapping_json + "', '" + biz_desc + "', 'system_init', '" + now_ts + "')"
        )
        spark.sql(insert_sql)
        print("  Seeded " + x_center + "/" + layer + " v1")
    else:
        print("  " + x_center + "/" + layer + " already has data, skipping")

# 6. Seed threshold controls
threshold_seeds = [
    ("loss_ratio", None, None, 200.0, 0.0, None, None, "system_init"),
    ("loss_ratio", "FLORIDA", "CATASTROPHIC", 600.0, 0.0, "2026-09-20", "2026-09-25", "system_init"),
    ("claim_count_variance_pct", None, None, 5000.0, 0.0, None, None, "system_init"),
    ("premium_growth_pct", None, None, 50.0, -50.0, None, None, "system_init"),
]

for metric, region, event, max_t, min_t, start_d, end_d, updated_by in threshold_seeds:
    region_clause = "AND region = '" + region + "'" if region else "AND region IS NULL"
    event_clause = "AND event_type = '" + event + "'" if event else "AND event_type IS NULL"
    existing = spark.sql(
        "SELECT COUNT(*) as cnt FROM pc_insurance.metadata.threshold_controls " +
        "WHERE metric_name = '" + metric + "' " + region_clause + " " + event_clause
    ).collect()[0]["cnt"]
    
    if existing == 0:
        region_val = "'" + region + "'" if region else "NULL"
        event_val = "'" + event + "'" if event else "NULL"
        start_val = "DATE('" + start_d + "')" if start_d else "NULL"
        end_val = "DATE('" + end_d + "')" if end_d else "NULL"
        insert_sql = (
            "INSERT INTO pc_insurance.metadata.threshold_controls " +
            "(metric_name, region, event_type, max_threshold, min_threshold, " +
            "effective_start_date, effective_end_date, is_active, updated_by, updated_at) " +
            "VALUES ('" + metric + "', " + region_val + ", " + event_val + ", " +
            str(max_t) + ", " + str(min_t) + ", " + start_val + ", " + end_val + ", " +
            "true, '" + updated_by + "', '" + now_ts + "')"
        )
        spark.sql(insert_sql)
        print("  Seeded threshold: " + metric + " (" + (region or "ALL") + ") (" + (event or "DEFAULT") + ")")
    else:
        print("  Threshold " + metric + " (" + (region or "ALL") + ") already exists, skipping")

# Verify
print("\n-- Infrastructure Verification --")
mapping_count = spark.sql("SELECT COUNT(*) as cnt FROM pc_insurance.metadata.mapping_documents").collect()[0]["cnt"]
threshold_count = spark.sql("SELECT COUNT(*) as cnt FROM pc_insurance.metadata.threshold_controls").collect()[0]["cnt"]
print("  mapping_documents: " + str(mapping_count) + " rows")
print("  threshold_controls: " + str(threshold_count) + " rows")

print("\n-- Mapping Documents Summary --")
display(spark.sql("""
    SELECT x_center, layer, version, is_active, updated_by, updated_at
    FROM pc_insurance.metadata.mapping_documents
    ORDER BY x_center, layer, version
"""))

print("\n-- Threshold Controls --")
display(spark.sql("""
    SELECT metric_name, region, event_type, max_threshold, min_threshold,
           effective_start_date, effective_end_date, is_active
    FROM pc_insurance.metadata.threshold_controls
    ORDER BY metric_name, region
"""))

print("\npc_insurance_dev infrastructure setup complete!")

# COMMAND ----------

# DBTITLE 1,Central State Schema
# ═══════════════════════════════════════════════════════════════
# Cell 3: Central State Schema (LangGraph TypedDict)
# ═══════════════════════════════════════════════════════════════

from typing import TypedDict, Literal, Optional, Any, Dict, List, Annotated
from operator import add


class AgentRole(str, Enum):
    """Enumerates all agents in the swarm."""
    SUPERVISOR   = "supervisor"
    TRIAGE       = "triage"
    BA           = "business_analyst"
    DATA_ENGINEER = "data_engineer"
    QA           = "qa_validation"
    DEPLOYMENT   = "deployment"
    HUMAN_ESCALATION = "human_escalation"


class SwarmPhase(str, Enum):
    """Plan-Execute-Verify-Deploy loop phases."""
    PLAN     = "plan"
    EXECUTE  = "execute"
    VERIFY   = "verify"
    DEPLOY   = "deploy"
    HALTED   = "halted"
    COMPLETE = "complete"


class SwarmState(TypedDict, total=False):
    """
    Central state object passed through the LangGraph state graph.

    Every agent reads from and writes to this shared state.
    The `messages` list uses the Annotated[...] reducer pattern from LangGraph
    so that messages from multiple agents accumulate without overwriting.
    """
    # ── Run Identity ──
    run_id: str                          # The Databricks job run ID that triggered the swarm
    session_id: str                      # Unique swarm session UUID
    trigger_source: str                  # "alert", "schedule", "manual"

    # ── Error Context (Populated by Triage) ──
    error_log: str                       # Raw error trace / log text
    error_class: str                     # Classified error type
    error_signature: str                 # Hash of error for deduplication
    affected_x_center: str              # PolicyCenter / ClaimCenter / BillingCenter / MGA_Feed
    affected_layer: str                  # bronze / silver / gold
    affected_table: str                  # Fully qualified table name
    downstream_impact: List[str]         # List of downstream tables/metrics affected

    # ── Metadata Delta (Populated by BA + Data Engineer) ──
    target_x_center: str                 # Which X-Center mapping to modify
    target_layer: str                     # Which medallion layer mapping
    metadata_delta: Dict[str, Any]       # The proposed metadata changes
    mapping_document_before: Dict[str, Any]   # Snapshot before changes
    mapping_document_after: Dict[str, Any]     # Snapshot after changes
    metadata_update_applied: bool        # Flag: has the metadata table been updated?

    # ── QA / Validation ──
    validation_results: Dict[str, Any]   # DLT expectation results
    validation_passed: bool              # True if sandbox QA passed
    sandbox_catalog: str                 # Name of the staging catalog
    sandbox_run_id: str                   # Databricks run ID in sandbox

    # ── Documentation ──
    technical_docs_payload: Dict[str, Any]  # Generated markdown docs / runbooks
    uc_comments_updated: bool
    git_pr_url: str                       # URL of the auto-created PR

    # ── Swarm Control ──
    attempt_counter: int                  # Current ReAct iteration (0-5)
    phase: str                            # Current phase (SwarmPhase value)
    next_agent: str                       # Next agent to route to
    token_usage: int                      # Cumulative token count
    token_cost_usd: float                # Cumulative dollar cost
    messages: Annotated[List[Dict[str, str]], add]  # Accumulated agent messages
    human_escalation_reason: str          # If halted, why
    final_status: str                     # "resolved", "halted", "escalated"

    # ── Autonomy: Circuit Breaker & Rollback ──
    pre_fix_timestamp: str               # ISO timestamp before fix (for RESTORE)
    dq_score_before: float               # DQ score before fix applied
    dq_score_after: float                # DQ score after fix applied
    circuit_breaker_triggered: bool      # True if circuit breaker halted
    rollback_performed: bool             # True if automated rollback executed
    similar_past_fixes: List[Dict[str, Any]]  # Past fixes for same error class
    fix_history_recorded: bool           # Whether fix was logged to swarm_fix_history


# ── Helper: Initialize a fresh SwarmState ──────────────────────────

def init_swarm_state(run_id: str, trigger_source: str = "alert") -> SwarmState:
    """Create a new SwarmState with defaults."""
    return SwarmState(
        run_id=run_id,
        session_id=str(uuid.uuid4()),
        trigger_source=trigger_source,
        error_log="",
        error_class="",
        error_signature="",
        affected_x_center="",
        affected_layer="",
        affected_table="",
        downstream_impact=[],
        target_x_center="",
        target_layer="",
        metadata_delta={},
        mapping_document_before={},
        mapping_document_after={},
        metadata_update_applied=False,
        validation_results={},
        validation_passed=False,
        sandbox_catalog="",
        sandbox_run_id="",
        technical_docs_payload={},
        uc_comments_updated=False,
        git_pr_url="",
        attempt_counter=0,
        phase=SwarmPhase.PLAN.value,
        next_agent=AgentRole.SUPERVISOR.value,
        token_usage=0,
        token_cost_usd=0.0,
        messages=[],
        human_escalation_reason="",
        final_status="",
        pre_fix_timestamp="",
        dq_score_before=0.0,
        dq_score_after=0.0,
        circuit_breaker_triggered=False,
        rollback_performed=False,
        similar_past_fixes=[],
        fix_history_recorded=False,
    )

# COMMAND ----------

# DBTITLE 1,UC Toolkit — Part 1
# ═══════════════════════════════════════════════════════════════
# Cell 4: UC Toolkit Functions — Part 1
#   get_pipeline_error_log(run_id)
#   read_mapping_document(x_center, layer)
#   update_mapping_document(x_center, layer, update_payload)
# ═══════════════════════════════════════════════════════════════
#
# These functions are designed to be deployed as Unity Catalog AI Functions
# (Python functions registered via `CREATE FUNCTION ... AS PYTHON`).
# They can also be called directly within the notebook for development.
# ─────────────────────────────────────────────────────────────────

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs as jobs_service
import json, hashlib, datetime

_w = WorkspaceClient()


def get_pipeline_error_log(run_id: str) -> dict:
    """
    Fetches the error trace and output logs for a Databricks job run.

    For multi-task jobs, iterates over all task run IDs and collects the first
    failed task's error output. Also fetches DLT event logs if the task is
    a pipeline (DLT/SDP) task.

    Returns:
        dict with keys: run_id, state, error_message, error_trace,
                        failed_task, task_run_id, dlt_events (optional)
    """
    # 1. Get the parent run details
    run_info = _w.jobs.get_run(run_id=int(run_id))
    state = run_info.state.result_state.value if run_info.state else "UNKNOWN"

    result = {
        "run_id": run_id,
        "state": state,
        "error_message": "",
        "error_trace": "",
        "failed_task": "",
        "task_run_id": "",
        "dlt_events": []
    }

    # 2. For multi-task jobs, find the failed task
    tasks = run_info.tasks or []
    failed_task = None
    for task in tasks:
        task_state = task.state.result_state.value if task.state else None
        if task_state == "FAILED":
            failed_task = task
            break

    if failed_task:
        task_run_id = str(failed_task.run_id)
        result["failed_task"] = failed_task.task_key
        result["task_run_id"] = task_run_id

        # 3. Fetch the run output for the failed task
        try:
            run_output = _w.jobs.get_run_output(run_id=int(task_run_id))
            if run_output.error:
                result["error_message"] = run_output.error
            if run_output.logs:
                result["error_trace"] = "\n".join(
                    log.get("log", "") for log in run_output.logs if isinstance(log, dict)
                )
        except Exception as e:
            result["error_message"] = f"Failed to fetch run output: {e}"

        # 4. If this is a DLT/SDP pipeline task, fetch update event logs
        if hasattr(failed_task, "pipeline_task") and failed_task.pipeline_task:
            pipeline_id = failed_task.pipeline_task.pipeline_id
            try:
                updates = _w.pipelines.list_updates(pipeline_id=pipeline_id)
                for update in updates.updates or []:
                    if update.state and "FAILED" in str(update.state):
                        events = _w.pipelines.list_pipeline_events(
                            pipeline_id=pipeline_id,
                            max_results=50
                        )
                        result["dlt_events"] = [
                            {"origin": e.origin, "timestamp": str(e.timestamp), "message": e.message}
                            for e in (events.events or [])
                            if e.message and "ERROR" in str(e.message).upper()
                        ][:20]
            except Exception as e:
                result["dlt_events"] = [{"error": f"Failed to fetch DLT events: {e}"}]

    else:
        # Single-task job — fetch output directly
        try:
            run_output = _w.jobs.get_run_output(run_id=int(run_id))
            if run_output.error:
                result["error_message"] = run_output.error
        except Exception as e:
            result["error_message"] = f"Failed to fetch run output: {e}"

    return result


def read_mapping_document(x_center: str, layer: str) -> dict:
    """
    Retrieves the active mapping/metadata configuration for a specific
    X-Center source system and Medallion layer from the Unity Catalog
    metadata store.

    The mapping document is stored as a Delta table:
        pc_insurance_dev.metadata.mapping_documents

    Schema:
        x_center STRING, layer STRING, version INT, is_active BOOLEAN,
        mapping_json STRING, business_description STRING,
        updated_by STRING, updated_at TIMESTAMP

    Returns:
        dict: The mapping JSON parsed as a Python dict.
    """
    query = f"""
        SELECT mapping_json, business_description, version
        FROM {METADATA_CATALOG}.{METADATA_SCHEMA}.mapping_documents
        WHERE x_center = '{x_center}'
          AND layer = '{layer}'
          AND is_active = true
        ORDER BY version DESC
        LIMIT 1
    """

    df = spark.sql(query)
    rows = df.collect()

    if not rows:
        return {
            "x_center": x_center,
            "layer": layer,
            "mapping": {},
            "business_description": "",
            "version": 0,
            "exists": False
        }

    row = rows[0]
    mapping = json.loads(row["mapping_json"]) if row["mapping_json"] else {}

    return {
        "x_center": x_center,
        "layer": layer,
        "mapping": mapping,
        "business_description": row["business_description"],
        "version": int(row["version"]),
        "exists": True
    }


def update_mapping_document(x_center: str, layer: str, update_payload: dict) -> dict:
    """
    Allows the BA Agent to programmatically insert, modify, or deprecate
    transformation rules, column mappings, data types, or default fallbacks
    in the metadata store.

    This function does NOT mutate the existing row — it inserts a NEW version
    with is_active = true and sets all prior versions to is_active = false.
    This preserves full audit history for governance.

    update_payload structure:
    {
        "column_mappings": {"source_col": {"target_col": "...", "data_type": "...", ...}},
        "transformation_rules": [{"rule_name": "...", "rule_sql": "...", ...}],
        "business_description": "Updated description...",
        "updated_by": "ba_agent"
    }

    Returns:
        dict: Confirmation with new version number and updated mapping.
    """
    # 1. Read the current active version (snapshot for audit)
    current = read_mapping_document(x_center, layer)
    current_mapping = current.get("mapping", {})
    new_version = current.get("version", 0) + 1

    # 2. Deep-merge the update payload into the current mapping
    updated_mapping = _deep_merge_mapping(current_mapping, update_payload.get("column_mappings", {}))

    # 3. Apply transformation rule updates
    if "transformation_rules" in update_payload:
        existing_rules = current_mapping.get("transformation_rules", [])
        new_rules = update_payload["transformation_rules"]
        # Merge rules by rule_name — new rules override existing ones
        rule_map = {r["rule_name"]: r for r in existing_rules}
        for nr in new_rules:
            rule_map[nr["rule_name"]] = nr
        updated_mapping["transformation_rules"] = list(rule_map.values())

    # 4. Write new version to UC table
    mapping_json = json.dumps(updated_mapping, indent=2, sort_keys=True)
    business_desc = update_payload.get("business_description", current.get("business_description", ""))
    updated_by = update_payload.get("updated_by", "ba_agent")
    now_ts = datetime.datetime.now().isoformat()

    # Deactivate old versions
    spark.sql(f"""
        UPDATE {METADATA_CATALOG}.{METADATA_SCHEMA}.mapping_documents
        SET is_active = false
        WHERE x_center = '{x_center}' AND layer = '{layer}' AND is_active = true
    """)

    # Insert new active version
    spark.sql(f"""
        INSERT INTO {METADATA_CATALOG}.{METADATA_SCHEMA}.mapping_documents
        (x_center, layer, version, is_active, mapping_json, business_description, updated_by, updated_at)
        VALUES ('{x_center}', '{layer}', {new_version}, true,
                '{mapping_json.replace("'", "''")}',
                '{business_desc.replace("'", "''")}',
                '{updated_by}', '{now_ts}')
    """)

    logger.info(f"Mapping updated: {x_center}/{layer} -> v{new_version}")

    return {
        "x_center": x_center,
        "layer": layer,
        "new_version": new_version,
        "mapping": updated_mapping,
        "business_description": business_desc,
        "updated_by": updated_by,
        "updated_at": now_ts,
        "status": "success"
    }


def _deep_merge_mapping(base: dict, overlay: dict) -> dict:
    """Recursively merges overlay into base. Lists are replaced, dicts are merged."""
    result = base.copy()
    for k, v in overlay.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge_mapping(result[k], v)
        else:
            result[k] = v
    return result

# COMMAND ----------

# DBTITLE 1,UC Toolkit — Part 2
# ═══════════════════════════════════════════════════════════════
# Cell 5: UC Toolkit Functions — Part 2
#   execute_sandbox_metadata_run(x_center, staging_catalog)
#   update_uc_catalog_comments(table_name, column_comments)
#   write_technical_markdown_doc(file_path, content)
#   trigger_pipeline_repair(run_id)
# ═══════════════════════════════════════════════════════════════

def execute_sandbox_metadata_run(x_center: str, staging_catalog: str) -> dict:
    """
    Signals the metadata framework to compile and execute a test run
    using the newly updated mappings in a safe sandbox environment.

    This function:
      1. Creates a staging catalog `dev_staging_<run_id>` if not exists
      2. Copies the active mapping into the staging catalog
      3. Triggers the ingestion pipeline in dry-run / validation mode
      4. Returns the sandbox run ID for QA verification

    Returns:
        dict with keys: staging_catalog, sandbox_run_id, status
    """
    # 1. Create staging catalog + schema
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {staging_catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {staging_catalog}.metadata")

    # 2. Clone the updated mapping into staging
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {staging_catalog}.metadata.mapping_documents
        AS SELECT * FROM {METADATA_CATALOG}.{METADATA_SCHEMA}.mapping_documents
        WHERE x_center = '{x_center}' AND is_active = true
    """)

    # 3. Find the job ID for the ingestion pipeline for this x_center
    job_name = f"PC_Ingestion_{x_center}_Bronze"
    jobs = _w.jobs.list()
    target_job_id = None
    for job in jobs:
        if job_name.lower() in (job.settings.name or "").lower():
            target_job_id = job.job_id
            break

    if not target_job_id:
        return {
            "staging_catalog": staging_catalog,
            "sandbox_run_id": "",
            "status": "error",
            "message": f"Job '{job_name}' not found"
        }

    # 4. Trigger the job with a staging catalog parameter override
    try:
        run_response = _w.jobs.run_now(
            job_id=target_job_id,
            notebook_params={
                "target_catalog": staging_catalog,
                "x_center": x_center,
                "execution_mode": "sandbox_validation"
            }
        )
        sandbox_run_id = str(run_response.run_id)
        logger.info(f"Sandbox run triggered: {sandbox_run_id} in {staging_catalog}")
        return {
            "staging_catalog": staging_catalog,
            "sandbox_run_id": sandbox_run_id,
            "status": "triggered",
            "job_id": target_job_id
        }
    except Exception as e:
        return {
            "staging_catalog": staging_catalog,
            "sandbox_run_id": "",
            "status": "error",
            "message": str(e)
        }


def update_uc_catalog_comments(table_name: str, column_comments: dict) -> dict:
    """
    Executes COMMENT ON SQL commands to refresh technical metadata
    descriptions directly in Unity Catalog.

    Args:
        table_name: Fully qualified table name (catalog.schema.table)
        column_comments: {"column_name": "markdown description", ...}

    Returns:
        dict with keys: table_name, columns_updated, status
    """
    updated = []
    errors = []

    # 1. Update table-level comment
    if "__table_comment__" in column_comments:
        table_comment = column_comments["__table_comment__"].replace("'", "''")
        try:
            spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES ('comment' = '{table_comment}')
            """)
            updated.append("__table_comment__")
        except Exception as e:
            errors.append({"__table_comment__": str(e)})

    # 2. Update each column comment
    for col_name, comment in column_comments.items():
        if col_name == "__table_comment__":
            continue
        safe_comment = comment.replace("'", "''")
        try:
            spark.sql(f"""
                ALTER TABLE {table_name}
                ALTER COLUMN {col_name}
                SET COMMENT '{safe_comment}'
            """)
            updated.append(col_name)
        except Exception as e:
            errors.append({col_name: str(e)})

    logger.info(f"UC comments updated for {table_name}: {len(updated)} columns")
    return {
        "table_name": table_name,
        "columns_updated": updated,
        "errors": errors,
        "status": "success" if not errors else "partial"
    }


def write_technical_markdown_doc(file_path: str, content: str) -> dict:
    """
    Saves generated post-mortems, lineage diagrams, and runbooks as .md files
    to a Unity Catalog Volume or Git workspace path.

    For UC Volumes: file_path = /Volumes/catalog/schema/volume/filename.md
    For Workspace:  file_path = /Workspace/path/to/filename.md

    Returns:
        dict with keys: file_path, bytes_written, status
    """
    try:
        if file_path.startswith("/Volumes/") or file_path.startswith("/Workspace/"):
            dbutils.fs.put(file_path, content, overwrite=True)
        else:
            return {"file_path": file_path, "bytes_written": 0, "status": "error",
                    "message": "Unsupported path prefix. Use /Volumes/ or /Workspace/"}

        logger.info(f"Technical doc written: {file_path} ({len(content)} bytes)")
        return {
            "file_path": file_path,
            "bytes_written": len(content),
            "status": "success"
        }
    except Exception as e:
        return {"file_path": file_path, "bytes_written": 0, "status": "error",
                "message": str(e)}


def trigger_pipeline_repair(run_id: str) -> dict:
    """
    Fires the Databricks Jobs repair endpoint to resume the pipeline once
    metadata and documentation dependencies are validated.

    Uses repair-run with rerun_all_failed_tasks to re-execute only
    the failed tasks with the updated metadata.

    Returns:
        dict with keys: run_id, repair_run_id, status
    """
    try:
        repair_response = _w.jobs.repair_run(
            run_id=int(run_id),
            rerun_all_failed_tasks=True,
            rerun_dependent_tasks=True
        )
        logger.info(f"Pipeline repair triggered for run {run_id}")
        return {
            "run_id": run_id,
            "repair_run_id": str(repair_response.run_id) if hasattr(repair_response, 'run_id') else run_id,
            "status": "repair_triggered"
        }
    except Exception as e:
        return {"run_id": run_id, "repair_run_id": "", "status": "error",
                "message": str(e)}

# COMMAND ----------

# DBTITLE 1,LLM Integration & Agent Base Class
# ═══════════════════════════════════════════════════════════════
# Cell 6: LLM Integration & Agent Base Class
# ═══════════════════════════════════════════════════════════════

import requests
import json
from abc import ABC, abstractmethod


class LLMServingClient:
    """
    Wraps the Mosaic AI Model Serving endpoint for LLM calls.
    Routes Supervisor to SUPERVISOR_LLM_ENDPOINT, sub-agents to SUBAGENT_LLM_ENDPOINT.
    """

    def __init__(self, workspace_client: WorkspaceClient):
        self._w = workspace_client
        self._host = workspace_client.config.host

    def query(self, endpoint_name: str, messages: list, temperature: float = 0.1,
              max_tokens: int = 2000) -> dict:
        """
        Calls the Mosaic AI Model Serving endpoint via the REST API.
        Uses the ChatCompletions format (OpenAI-compatible).

        Returns:
            dict: {"content": str, "usage": {"prompt_tokens": int, "completion_tokens": int}}
        """
        url = f"{self._host}/serving-endpoints/{endpoint_name}/invocations"
        token = self._w.config.authenticate()[1]
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }
        payload = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()

            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            usage = data.get("usage", {})

            return {
                "content": content,
                "usage": {
                    "prompt_tokens": usage.get("prompt_tokens", 0),
                    "completion_tokens": usage.get("completion_tokens", 0),
                    "total_tokens": usage.get("total_tokens", 0)
                }
            }
        except Exception as e:
            logger.error(f"LLM query failed: {e}")
            return {
                "content": f"LLM_ERROR: {e}",
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            }


class BaseAgent(ABC):
    """
    Abstract base class for all agents in the swarm.
    Each agent has:
      - A name (role)
      - An LLM endpoint (supervisor vs sub-agent)
      - A system prompt defining its responsibilities
      - A process(state) -> state method that reads + updates SwarmState
    """

    def __init__(self, name: str, llm_client: LLMServingClient,
                 system_prompt: str, endpoint: str = SUBAGENT_LLM_ENDPOINT):
        self.name = name
        self.llm_client = llm_client
        self.system_prompt = system_prompt
        self.endpoint = endpoint
        self.logger = logging.getLogger(f"Agent:{name}")

    @abstractmethod
    def process(self, state: SwarmState) -> SwarmState:
        """Process the current state and return updated state."""
        pass

    def _llm_call(self, user_message: str, temperature: float = 0.1,
                 max_tokens: int = 2000) -> tuple:
        """Call the LLM with the system prompt + user message. Returns (content, usage)."""
        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_message}
        ]
        result = self.llm_client.query(self.endpoint, messages, temperature, max_tokens)
        return result["content"], result["usage"]

    def _update_token_budget(self, state: SwarmState, usage: dict) -> SwarmState:
        """Track token usage and dollar cost. Halt if budget exceeded."""
        total_tokens = usage.get("total_tokens", 0)
        state["token_usage"] = state.get("token_usage", 0) + total_tokens

        input_cost = (usage.get("prompt_tokens", 0) / 1000) * TOKEN_COST_PER_1K_INPUT
        output_cost = (usage.get("completion_tokens", 0) / 1000) * TOKEN_COST_PER_1K_OUTPUT
        state["token_cost_usd"] = state.get("token_cost_usd", 0.0) + input_cost + output_cost

        if state["token_cost_usd"] >= MAX_TOKEN_BUDGET_USD:
            state["phase"] = SwarmPhase.HALTED.value
            state["human_escalation_reason"] = (
                f"Token budget exceeded: ${state['token_cost_usd']:.2f} >= ${MAX_TOKEN_BUDGET_USD}"
            )
            state["final_status"] = "halted"
            self.logger.warning(f"Token budget exceeded! Halting. Cost: ${state['token_cost_usd']:.2f}")

        return state

    def _check_loop_guard(self, state: SwarmState) -> bool:
        """Check if we've exceeded max ReAct iterations. Returns True if OK to proceed."""
        if state.get("attempt_counter", 0) >= MAX_REACT_ITERATIONS:
            state["phase"] = SwarmPhase.HALTED.value
            state["human_escalation_reason"] = (
                f"Max ReAct iterations exceeded: {state['attempt_counter']} >= {MAX_REACT_ITERATIONS}"
            )
            state["final_status"] = "halted"
            self.logger.warning(f"Max iterations exceeded! Halting at {state['attempt_counter']} iterations.")
            return False
        return True

    def _log_action(self, state: SwarmState, action: str, details: str = "") -> SwarmState:
        """Append an action message to the state's message log."""
        msg = {
            "agent": self.name,
            "timestamp": datetime.datetime.now().isoformat(),
            "action": action,
            "details": details,
            "attempt": state.get("attempt_counter", 0)
        }
        state["messages"] = state.get("messages", []) + [msg]
        self.logger.info(f"[{action}] {details[:200] if details else ''}")
        return state


# Initialize the shared LLM client
llm_client = LLMServingClient(WORKSPACE_CLIENT)

# COMMAND ----------

# DBTITLE 1,Supervisor Agent
# ═══════════════════════════════════════════════════════════════
# Cell 7: Supervisor Agent
# ═══════════════════════════════════════════════════════════════

class SupervisorAgent(BaseAgent):
    """
    The Supervisor Agent orchestrates the entire swarm.

    Responsibilities:
      - Read system alerts / triggers
      - Determine the failure domain (which X-Center, which layer)
      - Route tasks to specialized sub-agents
      - Enforce the Plan-Execute-Verify-Deploy loop
      - Enforce loop protection (max 5 iterations, token budget)
      - Decide when to escalate to human engineering
    """

    SUPERVISOR_PROMPT = f"""You are the Supervisor Agent of an autonomous P&C Insurance Data Engineering Agent Swarm.
Your job is to:
1. Analyze the error log and classify the failure domain.
2. Determine which X-Center (PolicyCenter, ClaimCenter, BillingCenter, MGA_Feed) and Medallion layer (bronze, silver, gold) is affected.
3. Route the task to the correct sub-agent: triage, business_analyst, data_engineer, qa_validation, or deployment.
4. Enforce the Plan-Execute-Verify-Deploy loop strictly.
5. If the attempt counter exceeds 5 or token budget exceeds ${MAX_TOKEN_BUDGET_USD}, halt and escalate to humans.

Return your routing decision as JSON with keys: next_agent, reasoning, failure_domain.
"""

    def __init__(self, llm_client: LLMServingClient):
        super().__init__(
            name="Supervisor",
            llm_client=llm_client,
            system_prompt=self.SUPERVISOR_PROMPT,
            endpoint=SUPERVISOR_LLM_ENDPOINT
        )

    def process(self, state: SwarmState) -> SwarmState:
        self.logger.info(f"Supervisor activated. Attempt {state.get('attempt_counter', 0) + 1}")

        # ── Guardrail: Loop protection ──
        if not self._check_loop_guard(state):
            return state

        state["attempt_counter"] = state.get("attempt_counter", 0) + 1

        # ── Determine routing based on current phase ──
        phase = state.get("phase", SwarmPhase.PLAN.value)

        if phase == SwarmPhase.PLAN.value:
            state["next_agent"] = AgentRole.TRIAGE.value
            state = self._log_action(state, "route_to_triage",
                "Routing to Triage Agent for error context gathering")

        elif phase == SwarmPhase.EXECUTE.value:
            state["next_agent"] = AgentRole.BA.value
            state = self._log_action(state, "route_to_ba",
                f"Routing to BA Agent for {state.get('affected_x_center')}/{state.get('affected_layer')} mapping update")

        elif phase == SwarmPhase.VERIFY.value:
            state["next_agent"] = AgentRole.QA.value
            state = self._log_action(state, "route_to_qa",
                "Routing to QA Agent for sandbox validation")

        elif phase == SwarmPhase.DEPLOY.value:
            state["next_agent"] = AgentRole.DEPLOYMENT.value
            state = self._log_action(state, "route_to_deployment",
                "Routing to Deployment Agent for production repair + Git sync")

        elif phase == SwarmPhase.COMPLETE.value:
            state["next_agent"] = "__end__"
            state = self._log_action(state, "swarm_complete", "All phases complete")

        elif phase == SwarmPhase.HALTED.value:
            state["next_agent"] = AgentRole.HUMAN_ESCALATION.value
            state = self._log_action(state, "human_escalation",
                f"Escalating to humans: {state.get('human_escalation_reason', 'Unknown reason')}")

        # ── LLM-based routing refinement ──
        if state.get("error_log") and phase == SwarmPhase.PLAN.value:
            routing_prompt = self._build_routing_prompt(state)
            llm_response, usage = self._llm_call(routing_prompt, temperature=0.0, max_tokens=500)
            state = self._update_token_budget(state, usage)

            try:
                routing = json.loads(llm_response)
                fd = routing.get("failure_domain", {})
                state["affected_x_center"] = fd.get("x_center", state.get("affected_x_center", ""))
                state["affected_layer"] = fd.get("layer", state.get("affected_layer", ""))
                state["next_agent"] = routing.get("next_agent", state.get("next_agent", AgentRole.TRIAGE.value))
                state = self._log_action(state, "llm_routing",
                    f"LLM classified: x_center={state['affected_x_center']}, layer={state['affected_layer']}")
            except json.JSONDecodeError:
                self.logger.warning(f"LLM routing response not valid JSON: {llm_response[:200]}")

        return state

    def _build_routing_prompt(self, state: SwarmState) -> str:
        return f"""Analyze this error and determine the failure domain.

Error Log:
{state.get('error_log', 'No error log available')[:2000]}

Known X-Centers: {X_CENTERS}
Known Layers: {MEDALLION_LAYERS}

Return JSON:
{{
  "next_agent": "triage",
  "reasoning": "...",
  "failure_domain": {{"x_center": "...", "layer": "..."}}
}}"""

# COMMAND ----------

# DBTITLE 1,Triage Agent
# ═══════════════════════════════════════════════════════════════
# Cell 8: Triage Agent
# ═══════════════════════════════════════════════════════════════

class TriageAgent(BaseAgent):
    """
    The Triage Agent interacts with Databricks Jobs API, DLT event logs,
    and Unity Catalog lineage to:
      - Fetch error traces from the failed run
      - Evaluate downstream impacts on core insurance metrics
      - Author system post-mortems (Markdown runbooks)
      - Set the phase to EXECUTE once context is gathered
    """

    TRIAGE_PROMPT = """You are the Triage Agent for a P&C Insurance Data Engineering Agent Swarm.
Your job is to:
1. Fetch and analyze error traces from failed Databricks job runs.
2. Identify downstream impacts on insurance metrics (Loss Ratio, Combined Ratio, Premium, Claims).
3. Generate a comprehensive Markdown post-mortem runbook for every failure.
4. Classify the error type: schema_drift, data_quality, mapping_variance, infrastructure, or unknown.

Return JSON with keys: error_class, affected_table, downstream_impact (list), post_mortem_markdown, reasoning.
"""

    def __init__(self, llm_client: LLMServingClient):
        super().__init__(
            name="Triage",
            llm_client=llm_client,
            system_prompt=self.TRIAGE_PROMPT,
            endpoint=SUBAGENT_LLM_ENDPOINT
        )

    def process(self, state: SwarmState) -> SwarmState:
        self.logger.info("Triage Agent activated")

        # ── Step 1: Fetch the error log via UC Toolkit ──
        run_id = state.get("run_id", "")
        if not run_id:
            state = self._log_action(state, "triage_error", "No run_id provided in state")
            state["phase"] = SwarmPhase.HALTED.value
            return state

        error_info = get_pipeline_error_log(run_id)
        state["error_log"] = error_info.get("error_message", "")
        if error_info.get("dlt_events"):
            dlt_errors = "\n".join(
                e.get("message", "") for e in error_info["dlt_events"]
            )
            state["error_log"] += f"\n\nDLT Events:\n{dlt_errors}"

        # Compute error signature for deduplication
        error_text = state["error_log"][:1000]
        state["error_signature"] = hashlib.md5(error_text.encode()).hexdigest()[:16]

        state = self._log_action(state, "error_fetched",
            f"Error class: {error_info.get('state', 'UNKNOWN')}, signature: {state['error_signature']}")

        # ── Step 1b: Circuit Breaker Check ──
        # Halt if 3+ failed attempts for same error in last 6h
        circuit_breaker = self._check_circuit_breaker(state["error_signature"])
        if circuit_breaker["tripped"]:
            state["circuit_breaker_triggered"] = True
            state["phase"] = SwarmPhase.HALTED.value
            state["human_escalation_reason"] = f"Circuit breaker: {circuit_breaker['reason']}"
            state["final_status"] = "halted"
            state = self._log_action(state, "circuit_breaker_tripped",
                f"Circuit breaker triggered: {circuit_breaker['reason']}")
            return state

        # ── Step 1c: Fix Knowledge Base ──
        # Query swarm_fix_history for past successful fixes with same error_class
        past_fixes = self._query_fix_knowledge_base(state.get("error_class", ""))
        state["similar_past_fixes"] = past_fixes
        if past_fixes:
            state = self._log_action(state, "fix_knowledge_base",
                f"Found {len(past_fixes)} similar past fixes for guidance")

        # Record pre-fix timestamp for potential rollback
        state["pre_fix_timestamp"] = datetime.datetime.now().isoformat()

        # ── Step 2: Determine downstream impact via UC lineage ──
        affected_table = state.get("affected_table", "")
        if not affected_table and state.get("affected_x_center"):
            # Infer the affected table from x_center + layer
            x_center = state.get("affected_x_center", "").lower()
            layer = state.get("affected_layer", "bronze")
            affected_table = f"{PROD_CATALOG}.{layer}.{x_center}_raw"
            state["affected_table"] = affected_table

        downstream_impact = self._fetch_downstream_impact(affected_table)
        state["downstream_impact"] = downstream_impact

        state = self._log_action(state, "downstream_impact",
            f"{len(downstream_impact)} downstream tables/metrics affected")

        # ── Step 3: LLM-powered error classification + post-mortem generation ──
        classification_prompt = self._build_classification_prompt(state)
        llm_response, usage = self._llm_call(classification_prompt, temperature=0.1, max_tokens=3000)
        state = self._update_token_budget(state, usage)

        try:
            triage_result = json.loads(llm_response)
            state["error_class"] = triage_result.get("error_class", "unknown")
            if triage_result.get("affected_table"):
                state["affected_table"] = triage_result["affected_table"]
            if triage_result.get("downstream_impact"):
                state["downstream_impact"] = triage_result["downstream_impact"]

            # Store the post-mortem markdown in technical_docs_payload
            post_mortem_md = triage_result.get("post_mortem_markdown", "")
            state["technical_docs_payload"]["post_mortem"] = post_mortem_md

            state = self._log_action(state, "error_classified",
                f"Error class: {state['error_class']}, table: {state.get('affected_table')}")
        except json.JSONDecodeError:
            self.logger.warning(f"LLM triage response not valid JSON, using heuristic classification")
            state["error_class"] = self._heuristic_classify(state.get("error_log", ""))
            state = self._log_action(state, "error_classified_heuristic",
                f"Heuristic error class: {state['error_class']}")

        # ── Step 4: Transition to EXECUTE phase ──
        state["phase"] = SwarmPhase.EXECUTE.value
        state["next_agent"] = AgentRole.SUPERVISOR.value
        state = self._log_action(state, "triage_complete", "Transitioning to EXECUTE phase")

        return state

    def _fetch_downstream_impact(self, table_name: str) -> list:
        """Query UC lineage to find downstream tables/metrics."""
        if not table_name:
            return []
        try:
            lineage_df = spark.sql(f"""
                SELECT DISTINCT target_table_full_name AS downstream_table
                FROM system.access.table_lineage
                WHERE source_table_full_name = '{table_name}'
                AND target_table_full_name IS NOT NULL
                LIMIT 20
            """)
            return [row["downstream_table"] for row in lineage_df.collect()]
        except Exception as e:
            self.logger.warning(f"Lineage query failed: {e}")
            return []

    def _build_classification_prompt(self, state: SwarmState) -> str:
        return f"""Analyze this error and classify it for a P&C Insurance data pipeline.

Error Log:
{state.get('error_log', '')[:3000]}

Affected X-Center: {state.get('affected_x_center', 'unknown')}
Affected Layer: {state.get('affected_layer', 'unknown')}
Affected Table: {state.get('affected_table', 'unknown')}
Downstream Impact: {state.get('downstream_impact', [])}

Error classes: schema_drift, data_quality, mapping_variance, infrastructure, unknown

Generate a comprehensive Markdown post-mortem runbook including:
- Error signature and timestamp
- Affected pipeline lineage graph
- Root cause analysis
- Downstream metric impact assessment

Return JSON:
{{
  "error_class": "...",
  "affected_table": "...",
  "downstream_impact": ["..."],
  "post_mortem_markdown": "# Post-Mortem Runbook\n...",
  "reasoning": "..."
}}"""

    def _heuristic_classify(self, error_log: str) -> str:
        """Fallback classification when LLM output is not parseable."""
        error_lower = error_log.lower()
        if "schema" in error_lower or "column" in error_lower or "mismatch" in error_lower:
            return "schema_drift"
        if "not null" in error_lower or "constraint" in error_lower or "quality" in error_lower:
            return "data_quality"
        if "threshold" in error_lower or "variance" in error_lower or "kpi" in error_lower:
            return "mapping_variance"
        if "timeout" in error_lower or "resource" in error_lower or "cluster" in error_lower:
            return "infrastructure"
        return "unknown"

    def _check_circuit_breaker(self, error_signature: str) -> dict:
        """Check swarm_fix_history for repeated failures on same error signature."""
        try:
            df = spark.sql(f"""
                SELECT COUNT(*) AS failed_count,
                       MAX(fix_timestamp) AS last_attempt
                FROM {METADATA_CATALOG}.metadata.swarm_fix_history
                WHERE error_message LIKE '%{error_signature}%'
                  AND resolution_status IN ('halted', 'rollback')
                  AND fix_timestamp >= TIMESTAMPADD(HOUR, -6, CURRENT_TIMESTAMP())
            """)
            row = df.collect()[0]
            failed_count = row["failed_count"] or 0
            if failed_count >= 3:
                return {"tripped": True,
                        "reason": f"{failed_count} failed attempts in last 6h for error {error_signature}"}
            return {"tripped": False}
        except Exception as e:
            self.logger.warning(f"Circuit breaker check failed: {e}")
            return {"tripped": False}

    def _query_fix_knowledge_base(self, error_class: str) -> list:
        """Query past successful fixes for similar error classes."""
        if not error_class:
            return []
        try:
            df = spark.sql(f"""
                SELECT fix_id, error_class, error_message, fix_applied,
                       resolution_status, dq_score_before, dq_score_after
                FROM {METADATA_CATALOG}.metadata.swarm_fix_history
                WHERE error_class = '{error_class}'
                  AND resolution_status = 'resolved'
                ORDER BY fix_timestamp DESC
                LIMIT 5
            """)
            return [row.asDict() for row in df.collect()]
        except Exception as e:
            self.logger.warning(f"Fix knowledge base query failed: {e}")
            return []

# COMMAND ----------

# DBTITLE 1,BA Agent — Mapping Owner
# ═══════════════════════════════════════════════════════════════
# Cell 9: Business Analyst (BA) Agent — The Mapping Owner
# ═══════════════════════════════════════════════════════════════

class BusinessAnalystAgent(BaseAgent):
    """
    The BA Agent is the absolute owner of all functional mapping documents
    for each core insurance application ecosystem (ClaimCenter, PolicyCenter,
    BillingCenter, MGA feeds) spanning Bronze to Gold.

    Responsibilities:
      - Interpret validation errors and anomalies
      - Dynamically create, update, and maintain functional rules
      - Update business glossaries
      - Generate metadata_delta for the Data Engineer to apply
    """

    BA_PROMPT = """You are the Business Analyst Agent for a P&C Insurance metadata-driven platform.
You are the absolute owner of all functional mapping documents.

Your job is to:
1. Read the current mapping document for the affected X-Center and layer.
2. Interpret the error/validation failure to determine the required mapping change.
3. Generate a precise metadata_delta payload that describes the mapping update.
4. Include a business justification for the change.
5. Update the business description/glossary for any new or modified fields.

P&C Insurance domain knowledge:
- PolicyCenter: policy data, coverage fields, ACORD standards
- ClaimCenter: claims data, loss events, adjuster notes
- BillingCenter: premium, deductible, commission data
- MGA_Feed: third-party managing general agent feeds

Return JSON with keys: metadata_delta, column_mappings, transformation_rules,
  business_description, uc_column_comments, justification.
"""

    def __init__(self, llm_client: LLMServingClient):
        super().__init__(
            name="BA",
            llm_client=llm_client,
            system_prompt=self.BA_PROMPT,
            endpoint=SUBAGENT_LLM_ENDPOINT
        )

    def process(self, state: SwarmState) -> SwarmState:
        self.logger.info("BA Agent activated — Mapping Owner")

        x_center = state.get("affected_x_center", "")
        layer = state.get("affected_layer", "")

        if not x_center or not layer:
            state = self._log_action(state, "ba_error", "Missing x_center or layer in state")
            state["phase"] = SwarmPhase.HALTED.value
            return state

        # ── Step 1: Read the current mapping document ──
        state["target_x_center"] = x_center
        state["target_layer"] = layer
        current_mapping = read_mapping_document(x_center, layer)
        state["mapping_document_before"] = current_mapping

        state = self._log_action(state, "mapping_read",
            f"Read {x_center}/{layer} mapping v{current_mapping.get('version', 0)}")

        # ── Step 2: LLM-powered mapping update generation ──
        update_prompt = self._build_mapping_update_prompt(state, current_mapping)
        llm_response, usage = self._llm_call(update_prompt, temperature=0.1, max_tokens=4000)
        state = self._update_token_budget(state, usage)

        try:
            ba_result = json.loads(llm_response)

            # Construct the update payload for update_mapping_document
            update_payload = {
                "column_mappings": ba_result.get("column_mappings", {}),
                "transformation_rules": ba_result.get("transformation_rules", []),
                "business_description": ba_result.get("business_description", ""),
                "updated_by": "ba_agent"
            }

            state["metadata_delta"] = ba_result
            state["technical_docs_payload"]["business_justification"] = ba_result.get("justification", "")

            # Store UC column comments for the Data Engineer to apply later
            state["technical_docs_payload"]["uc_column_comments"] = ba_result.get("uc_column_comments", {})

            state = self._log_action(state, "mapping_delta_generated",
                f"Generated delta: {len(ba_result.get('column_mappings', {}))} column mappings, "
                f"{len(ba_result.get('transformation_rules', []))} rules")

        except json.JSONDecodeError:
            self.logger.warning(f"BA LLM response not valid JSON, using scenario-based fallback")
            update_payload = self._scenario_fallback(state)
            state["metadata_delta"] = update_payload
            state = self._log_action(state, "mapping_delta_fallback",
                f"Fallback delta generated for {state.get('error_class')}")

        # ── Step 3: Apply the mapping update to the metadata store ──
        # NOTE: BA applies the metadata update directly. The Data Engineer
        # then handles UC comments, metadata table validation, and sandbox runs.
        update_result = update_mapping_document(x_center, layer, update_payload)
        state["mapping_document_after"] = update_result
        state["metadata_update_applied"] = update_result.get("status") == "success"

        state = self._log_action(state, "mapping_updated",
            f"Mapping v{update_result.get('new_version')} applied: {update_result.get('status')}")

        # ── Step 4: Transition to VERIFY phase ──
        state["phase"] = SwarmPhase.VERIFY.value
        state["next_agent"] = AgentRole.SUPERVISOR.value
        state = self._log_action(state, "ba_complete", "Transitioning to VERIFY phase")

        return state

    def _build_mapping_update_prompt(self, state: SwarmState, current_mapping: dict) -> str:
        return f"""Analyze the error and generate the required mapping document update.

X-Center: {state.get('affected_x_center')}
Layer: {state.get('affected_layer')}
Error Class: {state.get('error_class')}
Error Log:
{state.get('error_log', '')[:2000]}

Current Mapping Document (version {current_mapping.get('version', 0)}):
{json.dumps(current_mapping.get('mapping', {}), indent=2)[:3000]}

Current Business Description:
{current_mapping.get('business_description', 'N/A')}

Generate the precise mapping update as JSON:
{{
  "column_mappings": {{
    "source_column_name": {{
      "target_col": "target_column",
      "data_type": "string|integer|decimal|date|timestamp",
      "nullable": true|false,
      "default_value": "...",
      "transformation": "optional SQL expression"
    }}
  }},
  "transformation_rules": [
    {{
      "rule_name": "unique_rule_name",
      "rule_type": "not_null|default_value|conditional|schema_evolution",
      "rule_sql": "SQL expression",
      "applies_to": "column_name",
      "condition": "optional WHERE condition"
    }}
  ],
  "business_description": "Updated business description including any new fields",
  "uc_column_comments": {{
    "new_or_changed_column": "Markdown description for Unity Catalog"
  }},
  "justification": "Business justification for this change"
}}"""

    def _scenario_fallback(self, state: SwarmState) -> dict:
        """Generate a fallback update payload based on error class heuristics."""
        error_class = state.get("error_class", "unknown")
        x_center = state.get("affected_x_center", "")
        layer = state.get("affected_layer", "")

        if error_class == "schema_drift":
            return {
                "column_mappings": {
                    "_auto_detected_new_column": {
                        "target_col": "_auto_detected_new_column",
                        "data_type": "string",
                        "nullable": True,
                        "default_value": None,
                        "transformation": None
                    }
                },
                "transformation_rules": [{
                    "rule_name": "auto_schema_evolution",
                    "rule_type": "schema_evolution",
                    "rule_sql": "ALTER TABLE ADD COLUMN IF NOT EXISTS",
                    "applies_to": "_auto_detected_new_column",
                    "condition": None
                }],
                "business_description": f"Auto-detected schema drift in {x_center}/{layer}",
                "updated_by": "ba_agent_fallback"
            }
        elif error_class == "data_quality":
            return {
                "column_mappings": {},
                "transformation_rules": [{
                    "rule_name": "null_fallback_deductible",
                    "rule_type": "default_value",
                    "rule_sql": "COALESCE(deductible_amount, 0)",
                    "applies_to": "deductible_amount",
                    "condition": "source_group_code = 'COMMERCIAL'"
                }],
                "business_description": f"DQ fallback rule for {x_center}/{layer}",
                "updated_by": "ba_agent_fallback"
            }
        elif error_class == "mapping_variance":
            return {
                "column_mappings": {},
                "transformation_rules": [{
                    "rule_name": "threshold_override",
                    "rule_type": "conditional",
                    "rule_sql": "UPDATE metadata_threshold SET max_loss_ratio = 500 WHERE event_type = 'CATASTROPHIC'",
                    "applies_to": "loss_ratio",
                    "condition": "event_type = 'CATASTROPHIC'"
                }],
                "business_description": f"Threshold variance accepted for {x_center}/{layer}",
                "updated_by": "ba_agent_fallback"
            }
        return {"column_mappings": {}, "transformation_rules": [], "business_description": "", "updated_by": "ba_agent_fallback"}

# COMMAND ----------

# DBTITLE 1,Data Engineer Agent
# ═══════════════════════════════════════════════════════════════
# Cell 10: Data Engineer Agent
# ═══════════════════════════════════════════════════════════════

class DataEngineerAgent(BaseAgent):
    """
    The Data Engineer Agent reads the updated mapping metadata configurations
    generated by the BA Agent and:
      - Applies programmatic validation tests
      - Updates the central metadata tables
      - Handles inline Unity Catalog metadata definitions (column comments)
      - Simulates runtime generation (sandbox compilation)
      - Updates technical documentation (dbt schema.yml, JSON schema defs)
    """

    DE_PROMPT = """You are the Data Engineer Agent for a P&C Insurance metadata-driven platform.
Your job is to:
1. Read the updated mapping metadata from the BA Agent.
2. Apply programmatic validation tests to ensure correctness.
3. Update Unity Catalog column and table comments with Markdown descriptions.
4. Generate sandbox DDL (ALTER TABLE statements) for schema evolution.
5. Update technical documentation (dbt schema.yml equivalents, JSON schema definitions).

Return JSON with keys: validation_passed, ddl_statements, uc_comment_update, technical_doc_update.
"""

    def __init__(self, llm_client: LLMServingClient):
        super().__init__(
            name="DataEngineer",
            llm_client=llm_client,
            system_prompt=self.DE_PROMPT,
            endpoint=SUBAGENT_LLM_ENDPOINT
        )

    def process(self, state: SwarmState) -> SwarmState:
        self.logger.info("Data Engineer Agent activated")

        x_center = state.get("target_x_center", state.get("affected_x_center", ""))
        layer = state.get("target_layer", state.get("affected_layer", ""))
        mapping_after = state.get("mapping_document_after", {})
        metadata_delta = state.get("metadata_delta", {})

        if not mapping_after or not mapping_after.get("mapping"):
            state = self._log_action(state, "de_error", "No updated mapping document available")
            return state

        # ── Step 1: Validate the updated mapping programmatically ──
        validation_result = self._validate_mapping(mapping_after, metadata_delta)
        state = self._log_action(state, "mapping_validated",
            f"Validation: {validation_result.get('status', 'unknown')}, "
            f"{validation_result.get('checks_passed', 0)} checks passed")

        if not validation_result.get("passed", False):
            state = self._log_action(state, "de_validation_failed",
                f"Mapping validation failed: {validation_result.get('errors', [])}")
            # Route back to BA for correction
            state["phase"] = SwarmPhase.EXECUTE.value
            state["next_agent"] = AgentRole.BA.value
            return state

        # ── Step 2: Update Unity Catalog column comments ──
        uc_comments = state.get("technical_docs_payload", {}).get("uc_column_comments", {})
        affected_table = state.get("affected_table", "")

        if uc_comments and affected_table:
            # Ensure __table_comment__ is included
            if "__table_comment__" not in uc_comments:
                uc_comments["__table_comment__"] = (
                    f"{affected_table} — Auto-updated by Data Engineer Agent on "
                    f"{datetime.datetime.now().isoformat()}. "
                    f"Source: {x_center}/{layer}."
                )

            comment_result = update_uc_catalog_comments(affected_table, uc_comments)
            state["uc_comments_updated"] = comment_result.get("status") in ("success", "partial")
            state = self._log_action(state, "uc_comments_updated",
                f"{len(comment_result.get('columns_updated', []))} columns updated, "
                f"status: {comment_result.get('status')}")

        # ── Step 3: Generate DDL for schema evolution (if needed) ──
        ddl_statements = self._generate_ddl(metadata_delta, affected_table, layer)
        if ddl_statements:
            state["technical_docs_payload"]["ddl_statements"] = ddl_statements
            state = self._log_action(state, "ddl_generated",
                f"{len(ddl_statements)} DDL statements generated")

        # ── Step 4: Generate sandbox staging catalog + trigger test run ──
        sandbox_catalog = f"{STAGING_CATALOG_PREFIX}{state.get('run_id', str(uuid.uuid4())[:8])}"
        state["sandbox_catalog"] = sandbox_catalog

        sandbox_result = execute_sandbox_metadata_run(x_center, sandbox_catalog)
        state["sandbox_run_id"] = sandbox_result.get("sandbox_run_id", "")

        state = self._log_action(state, "sandbox_triggered",
            f"Sandbox: {sandbox_catalog}, run: {state.get('sandbox_run_id', 'N/A')}, "
            f"status: {sandbox_result.get('status')}")

        # ── Step 5: Generate technical documentation updates ──
        tech_doc = self._generate_tech_doc(state, mapping_after, metadata_delta)
        state["technical_docs_payload"]["schema_doc"] = tech_doc
        state = self._log_action(state, "tech_doc_generated",
            f"Generated schema documentation ({len(tech_doc)} chars)")

        # ── Step 6: Transition to VERIFY phase (QA) ──
        state["phase"] = SwarmPhase.VERIFY.value
        state["next_agent"] = AgentRole.QA.value
        state = self._log_action(state, "de_complete", "Data Engineering complete, routing to QA")

        return state

    def _validate_mapping(self, mapping: dict, delta: dict) -> dict:
        """Run programmatic validation checks on the updated mapping."""
        checks = []
        errors = []

        # Check 1: All column mappings have required fields
        for col, spec in mapping.get("mapping", {}).get("column_mappings", {}).items():
            if "target_col" not in spec:
                errors.append(f"{col}: missing target_col")
            if "data_type" not in spec:
                errors.append(f"{col}: missing data_type")
            checks.append(f"column_mapping_{col}")

        # Check 2: Transformation rules have valid SQL
        for rule in mapping.get("mapping", {}).get("transformation_rules", []):
            if not rule.get("rule_name"):
                errors.append(f"Transformation rule missing rule_name: {rule}")
            if not rule.get("rule_type"):
                errors.append(f"Rule {rule.get('rule_name', '?')}: missing rule_type")
            checks.append(f"rule_{rule.get('rule_name', '?')}")

        # Check 3: No duplicate target column names
        target_cols = [spec.get("target_col") for spec in mapping.get("mapping", {}).get("column_mappings", {}).values()]
        duplicates = [c for c in target_cols if target_cols.count(c) > 1]
        if duplicates:
            errors.append(f"Duplicate target columns: {set(duplicates)}")

        return {
            "passed": len(errors) == 0,
            "checks_passed": len(checks),
            "errors": errors,
            "status": "passed" if not errors else "failed"
        }

    def _generate_ddl(self, delta: dict, table_name: str, layer: str) -> list:
        """Generate ALTER TABLE DDL statements for schema evolution."""
        ddl = []
        column_mappings = delta.get("column_mappings", {})

        for source_col, spec in column_mappings.items():
            target_col = spec.get("target_col", source_col)
            data_type = spec.get("data_type", "string")
            nullable = spec.get("nullable", True)

            col_def = f"{target_col} {data_type}"
            if not nullable:
                col_def += " NOT NULL"
            if spec.get("default_value") is not None:
                col_def += f" DEFAULT {spec['default_value']}"

            ddl.append(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_def};")

        return ddl

    def _generate_tech_doc(self, state: SwarmState, mapping: dict, delta: dict) -> str:
        """Generate a technical documentation snippet (dbt schema.yml equivalent)."""
        x_center = state.get("target_x_center", "")
        layer = state.get("target_layer", "")
        table_name = state.get("affected_table", "")

        doc_lines = [
            f"# Technical Schema Documentation: {table_name}",
            f"",
            f"**Source System:** {x_center}",
            f"**Medallion Layer:** {layer}",
            f"**Last Updated:** {datetime.datetime.now().isoformat()}",
            f"**Updated By:** data_engineer_agent",
            f"",
            f"## Column Mappings",
            f"",
            f"| Source Column | Target Column | Data Type | Nullable | Default | Transformation |",
            f"| --- | --- | --- | --- | --- | --- |"
        ]

        for col, spec in mapping.get("mapping", {}).get("column_mappings", {}).items():
            doc_lines.append(
                f"| {col} | {spec.get('target_col', col)} | {spec.get('data_type', 'string')} | "
                f"{spec.get('nullable', True)} | {spec.get('default_value', 'N/A')} | "
                f"{spec.get('transformation', 'N/A')} |"
            )

        doc_lines.extend(["", "## Transformation Rules", ""])
        for rule in mapping.get("mapping", {}).get("transformation_rules", []):
            doc_lines.extend([
                f"### {rule.get('rule_name', 'Unknown')}",
                f"- **Type:** {rule.get('rule_type', 'N/A')}",
                f"- **SQL:** `{rule.get('rule_sql', 'N/A')}`",
                f"- **Applies To:** {rule.get('applies_to', 'N/A')}",
                f"- **Condition:** {rule.get('condition', 'N/A')}",
                ""
            ])

        return "\n".join(doc_lines)

# COMMAND ----------

# DBTITLE 1,QA & Validation Agent
# ═══════════════════════════════════════════════════════════════
# Cell 11: QA & Validation Agent
# ═══════════════════════════════════════════════════════════════

class QAValidationAgent(BaseAgent):
    """
    The QA & Validation Agent runs automated checks using Delta Live Tables (DLT)
    expectations or Great Expectations to ensure data sanity and schema adherence
    after a metadata modification.

    Responsibilities:
      - Query the sandbox run output for validation results
      - Run DLT expectation checks (expect, expect_or_drop, expect_or_fail)
      - Run Great Expectations suites if configured
      - Verify schema adherence in the staging catalog
      - Report pass/fail back to the Supervisor
    """

    QA_PROMPT = """You are the QA & Validation Agent for a P&C Insurance metadata-driven platform.
Your job is to:
1. Validate the sandbox run results against DLT expectations.
2. Check schema adherence (column types, nullability, constraints).
3. Run data quality checks (row counts, null counts, value ranges).
4. Generate a validation report with pass/fail status.
5. If validation fails, identify the specific expectation that failed and suggest corrections.

Return JSON with keys: validation_passed, failed_expectations, row_count, null_violations, schema_issues, validation_report.
"""

    def __init__(self, llm_client: LLMServingClient):
        super().__init__(
            name="QA",
            llm_client=llm_client,
            system_prompt=self.QA_PROMPT,
            endpoint=SUBAGENT_LLM_ENDPOINT
        )

    def process(self, state: SwarmState) -> SwarmState:
        self.logger.info("QA & Validation Agent activated")

        sandbox_catalog = state.get("sandbox_catalog", "")
        sandbox_run_id = state.get("sandbox_run_id", "")
        x_center = state.get("target_x_center", state.get("affected_x_center", ""))
        layer = state.get("target_layer", state.get("affected_layer", ""))

        # ── Step 1: Check sandbox run status ──
        if sandbox_run_id:
            run_status = self._check_sandbox_run(sandbox_run_id)
            state = self._log_action(state, "sandbox_status",
                f"Run {sandbox_run_id}: {run_status.get('state', 'UNKNOWN')}")

            if run_status.get("state") == "FAILED":
                state["validation_passed"] = False
                state["validation_results"] = {
                    "sandbox_run_state": run_status.get("state"),
                    "error": run_status.get("error", ""),
                    "failed_expectations": []
                }
                state = self._log_action(state, "qa_failed",
                    f"Sandbox run failed: {run_status.get('error', '')[:200]}")
                # Route back to BA for correction
                state["phase"] = SwarmPhase.EXECUTE.value
                state["next_agent"] = AgentRole.BA.value
                return state

        # ── Step 2: Run DLT expectation validation queries on sandbox tables ──
        validation_results = self._run_dlt_expectations(sandbox_catalog, x_center, layer)

        # ── Step 3: Run schema adherence checks ──
        schema_issues = self._check_schema_adherence(sandbox_catalog, x_center, layer)
        validation_results["schema_issues"] = schema_issues

        # ── Step 4: LLM-powered validation report generation ──
        report_prompt = self._build_validation_prompt(state, validation_results)
        llm_response, usage = self._llm_call(report_prompt, temperature=0.1, max_tokens=2000)
        state = self._update_token_budget(state, usage)

        try:
            qa_result = json.loads(llm_response)
            state["validation_passed"] = qa_result.get("validation_passed", False)
            state["validation_results"] = {
                **validation_results,
                "report": qa_result.get("validation_report", llm_response)
            }
        except json.JSONDecodeError:
            state["validation_passed"] = validation_results.get("passed", False)
            state["validation_results"] = validation_results

        state = self._log_action(state, "qa_validation",
            f"Passed: {state['validation_passed']}, "
            f"Failed expectations: {len(validation_results.get('failed_expectations', []))}, "
            f"Schema issues: {len(schema_issues)}")

        # ── Step 5: Route based on validation result ──
        if state["validation_passed"]:
            state["phase"] = SwarmPhase.DEPLOY.value
            state["next_agent"] = AgentRole.SUPERVISOR.value
            state = self._log_action(state, "qa_passed", "Validation passed, transitioning to DEPLOY")
        else:
            state["phase"] = SwarmPhase.EXECUTE.value
            state["next_agent"] = AgentRole.BA.value
            state = self._log_action(state, "qa_failed", "Validation failed, routing back to BA for correction")

        return state

    def _check_sandbox_run(self, run_id: str) -> dict:
        """Check the status of the sandbox run via the Jobs API."""
        try:
            run_info = _w.jobs.get_run(run_id=int(run_id))
            state = run_info.state.result_state.value if run_info.state else "RUNNING"
            error = ""
            if state == "FAILED":
                try:
                    output = _w.jobs.get_run_output(run_id=int(run_id))
                    error = output.error or ""
                except Exception:
                    pass
            return {"state": state, "error": error}
        except Exception as e:
            return {"state": "UNKNOWN", "error": str(e)}

    def _run_dlt_expectations(self, catalog: str, x_center: str, layer: str) -> dict:
        """Run DLT expectation validation queries on sandbox tables."""
        results = {
            "passed": True,
            "failed_expectations": [],
            "row_count": 0,
            "null_violations": []
        }

        if not catalog or not x_center:
            return results

        table_name = f"{catalog}.{layer}.{x_center.lower()}_raw"

        try:
            # Check row count
            count_df = spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}")
            results["row_count"] = count_df.collect()[0]["cnt"]

            # Check for NOT NULL violations on key columns
            mapping = read_mapping_document(x_center, layer)
            for col, spec in mapping.get("mapping", {}).get("column_mappings", {}).items():
                if not spec.get("nullable", True):
                    target_col = spec.get("target_col", col)
                    null_df = spark.sql(
                        f"SELECT COUNT(*) as nulls FROM {table_name} WHERE {target_col} IS NULL"
                    )
                    null_count = null_df.collect()[0]["nulls"]
                    if null_count > 0:
                        results["null_violations"].append({
                            "column": target_col,
                            "null_count": null_count
                        })
                        results["passed"] = False
                        results["failed_expectations"].append(
                            f"NOT_NULL_{target_col}: {null_count} null values found"
                        )

        except Exception as e:
            results["passed"] = False
            results["failed_expectations"].append(f"Query error: {e}")

        return results

    def _check_schema_adherence(self, catalog: str, x_center: str, layer: str) -> list:
        """Check that the sandbox table schema matches the mapping document."""
        issues = []

        if not catalog or not x_center:
            return issues

        table_name = f"{catalog}.{layer}.{x_center.lower()}_raw"

        try:
            # Get the actual schema
            schema_df = spark.sql(f"DESCRIBE TABLE {table_name}")
            actual_cols = {row["col_name"]: row["data_type"] for row in schema_df.collect()
                          if row["col_name"] and not row["col_name"].startswith("#")}

            # Compare against mapping document
            mapping = read_mapping_document(x_center, layer)
            for col, spec in mapping.get("mapping", {}).get("column_mappings", {}).items():
                target_col = spec.get("target_col", col)
                expected_type = spec.get("data_type", "string")

                if target_col not in actual_cols:
                    issues.append(f"Missing column in sandbox: {target_col}")
                elif actual_cols[target_col].upper() != expected_type.upper():
                    issues.append(
                        f"Type mismatch for {target_col}: "
                        f"expected {expected_type}, got {actual_cols[target_col]}"
                    )

        except Exception as e:
            issues.append(f"Schema check error: {e}")

        return issues

    def _build_validation_prompt(self, state: SwarmState, results: dict) -> str:
        return f"""Generate a validation report for the QA checks.

Sandbox Catalog: {state.get('sandbox_catalog')}
X-Center: {state.get('target_x_center', state.get('affected_x_center', ''))}
Layer: {state.get('target_layer', state.get('affected_layer', ''))}

Validation Results:
{json.dumps(results, indent=2, default=str)[:2000]}

Return JSON:
{{
  "validation_passed": true|false,
  "failed_expectations": ["..."],
  "validation_report": "# QA Validation Report\n..."
}}"""

# COMMAND ----------

# DBTITLE 1,Deployment Agent
# ═══════════════════════════════════════════════════════════════
# Cell 12: Deployment Agent
# ═══════════════════════════════════════════════════════════════

class DeploymentAgent(BaseAgent):
    """
    The Deployment Agent manages isolated staging environments,
    triggers execution runs, and handles automated Git branch commits/PR
    creations for updated metadata assets and technical markdown files.

    Responsibilities:
      - Write post-mortem runbooks to UC Volume / Git
      - Write technical schema docs to UC Volume / Git
      - Trigger pipeline repair via the Jobs API
      - Create Git branch and PR for metadata + docs changes
      - Clean up staging catalogs after successful deployment
      - Set final_status to 'resolved'
    """

    DEPLOY_PROMPT = """You are the Deployment Agent for a P&C Insurance metadata-driven platform.
Your job is to:
1. Write technical documentation (post-mortems, schema docs) to UC Volumes and Git.
2. Trigger the pipeline repair to resume the failed run with updated metadata.
3. Create a Git branch and PR for the metadata + documentation changes.
4. Clean up the staging catalog after successful deployment.
5. Report the final deployment status.

Return JSON with keys: repair_triggered, docs_written, git_pr_url, staging_cleaned, deployment_status.
"""

    def __init__(self, llm_client: LLMServingClient):
        super().__init__(
            name="Deployment",
            llm_client=llm_client,
            system_prompt=self.DEPLOY_PROMPT,
            endpoint=SUBAGENT_LLM_ENDPOINT
        )

    def process(self, state: SwarmState) -> SwarmState:
        self.logger.info("Deployment Agent activated")

        run_id = state.get("run_id", "")
        session_id = state.get("session_id", "")
        sandbox_catalog = state.get("sandbox_catalog", "")

        # ── Step 1: Write post-mortem runbook to UC Volume ──
        post_mortem_md = state.get("technical_docs_payload", {}).get("post_mortem", "")
        if not post_mortem_md:
            post_mortem_md = self._generate_post_mortem(state)

        pm_file_path = f"{DOCS_VOLUME_PATH}/post_mortems/{run_id}_{session_id[:8]}.md"
        pm_result = write_technical_markdown_doc(pm_file_path, post_mortem_md)
        state = self._log_action(state, "post_mortem_written",
            f"{pm_file_path}: {pm_result.get('status')}")

        # ── Step 2: Write technical schema documentation ──
        schema_doc = state.get("technical_docs_payload", {}).get("schema_doc", "")
        if schema_doc:
            sd_file_path = f"{DOCS_VOLUME_PATH}/schema_docs/{state.get('affected_table', 'unknown')}.md"
            sd_result = write_technical_markdown_doc(sd_file_path, schema_doc)
            state = self._log_action(state, "schema_doc_written",
                f"{sd_file_path}: {sd_result.get('status')}")

        # ── Step 3: Trigger pipeline repair ──
        repair_result = trigger_pipeline_repair(run_id)
        state = self._log_action(state, "pipeline_repair",
            f"Run {run_id}: {repair_result.get('status')}")

        # ── Step 4: Create Git branch and PR (if Git is configured) ──
        git_pr_url = self._create_git_pr(state)
        state["git_pr_url"] = git_pr_url
        state = self._log_action(state, "git_pr",
            f"PR URL: {git_pr_url or 'N/A (Git not configured)'}")

        # ── Step 5: Clean up staging catalog ──
        if sandbox_catalog:
            cleanup_result = self._cleanup_staging(sandbox_catalog)
            state = self._log_action(state, "staging_cleanup",
                f"{sandbox_catalog}: {cleanup_result}")

        # ── Step 6: Set final status ──
        state["phase"] = SwarmPhase.COMPLETE.value
        state["final_status"] = "resolved"
        state["next_agent"] = "__end__"
        state = self._log_action(state, "deployment_complete",
            f"Pipeline repaired, docs written, PR: {git_pr_url or 'N/A'}")

        return state

    def _generate_post_mortem(self, state: SwarmState) -> str:
        """Generate a fallback post-mortem markdown if Triage didn't create one."""
        timestamp = datetime.datetime.now().isoformat()
        return f"""# Post-Mortem Runbook — Run {state.get('run_id', 'N/A')}

**Generated:** {timestamp}
**Session ID:** {state.get('session_id', 'N/A')}
**Error Class:** {state.get('error_class', 'unknown')}
**Error Signature:** {state.get('error_signature', 'N/A')}

## Error Summary

{state.get('error_log', 'No error log available')[:2000]}

## Affected Components

- **X-Center:** {state.get('affected_x_center', 'N/A')}
- **Layer:** {state.get('affected_layer', 'N/A')}
- **Table:** {state.get('affected_table', 'N/A')}

## Downstream Impact

{json.dumps(state.get('downstream_impact', []), indent=2)}

## Metadata Adjustments

**Business Justification:**
{state.get('technical_docs_payload', {}).get('business_justification', 'N/A')}

**Mapping Delta:**
{json.dumps(state.get('metadata_delta', {}), indent=2, default=str)[:2000]}

## Validation Results

{json.dumps(state.get('validation_results', {}), indent=2, default=str)[:2000]}

## Swarm Execution Summary

- **Total Attempts:** {state.get('attempt_counter', 0)}
- **Token Usage:** {state.get('token_usage', 0)}
- **Token Cost:** ${state.get('token_cost_usd', 0):.2f}
- **Final Status:** {state.get('final_status', 'resolved')}
"""

    def _create_git_pr(self, state: SwarmState) -> str:
        """Create a Git branch and PR for metadata + docs changes."""
        if not GIT_REPO_URL:
            self.logger.info("Git not configured, skipping PR creation")
            return ""

        branch_name = f"{GIT_BRANCH_PREFIX}/{state.get('run_id', 'unknown')}_{state.get('session_id', '')[:8]}"

        # In production, this would use the Databricks Repos API or Git CLI:
        # 1. Create a new branch
        # 2. Commit the updated mapping_documents table export
        # 3. Commit the technical docs markdown files
        # 4. Create a PR via the Git provider API

        # Placeholder for Git integration (implemented via runGit tool or Git CLI in production)
        self.logger.info(f"Git PR creation requested for branch: {branch_name}")

        # Return a mock PR URL (replace with actual Git provider API in production)
        return f"https://github.com/your-org/pc-insurance-metadata/pull/new/{branch_name}"

    def _cleanup_staging(self, catalog: str) -> str:
        """Drop the staging catalog after successful deployment."""
        try:
            spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
            return "cleaned_up"
        except Exception as e:
            return f"cleanup_failed: {e}"

# COMMAND ----------

# DBTITLE 1,Autonomous Rollback & Dependency Verification
# ═══════════════════════════════════════════════════════════════
# Cell 12b: Autonomous Rollback Manager & Dependency-Aware Repair
# ═══════════════════════════════════════════════════════════════
#
# Enhances swarm autonomy with:
#   1. Automated rollback via Delta RESTORE when DQ score drops after a fix
#   2. Dependency-aware repair verification (check downstream tables after fix)
#   3. Fix history logging to swarm_fix_history table
# ─────────────────────────────────────────────────────────────────

class RollbackManager:
    """
    Manages automated rollback of Delta tables when a fix degrades data quality.
    
    Uses Delta Lake's time travel (RESTORE TO VERSION/TIMESTAMP) to revert
    a table to its pre-fix state if the DQ score drops after a metadata change.
    """
    
    @staticmethod
    def capture_pre_fix_version(table_name: str) -> dict:
        """Capture the current version of a table before applying a fix."""
        try:
            hist = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").collect()[0]
            return {
                "table": table_name,
                "version": hist["version"],
                "timestamp": hist["timestamp"].isoformat()
            }
        except Exception as e:
            logger.warning(f"Could not capture pre-fix version for {table_name}: {e}")
            return {"table": table_name, "version": None, "timestamp": None}
    
    @staticmethod
    def perform_rollback(table_name: str, pre_fix_info: dict) -> dict:
        """
        Restore a Delta table to its pre-fix version.
        Uses RESTORE TO TIMESTAMP if available, otherwise RESTORE TO VERSION.
        """
        if not pre_fix_info.get("version") and not pre_fix_info.get("timestamp"):
            return {"success": False, "reason": "No pre-fix version/timestamp available"}
        
        try:
            if pre_fix_info.get("timestamp"):
                spark.sql(f"RESTORE TABLE {table_name} TO TIMESTAMP '{pre_fix_info['timestamp']}'")
            else:
                spark.sql(f"RESTORE TABLE {table_name} TO VERSION {pre_fix_info['version']}")
            logger.info(f"Rollback successful for {table_name}")
            return {"success": True, "table": table_name, "restored_to": pre_fix_info}
        except Exception as e:
            logger.error(f"Rollback failed for {table_name}: {e}")
            return {"success": False, "reason": str(e)}
    
    @staticmethod
    def evaluate_rollback_needed(state: SwarmState) -> tuple:
        """
        Determine if a rollback is needed based on DQ scores.
        Returns (should_rollback, reason).
        """
        score_before = state.get("dq_score_before", 0.0)
        score_after = state.get("dq_score_after", 0.0)
        
        if score_after < score_before:
            drop_pct = ((score_before - score_after) / score_before * 100) if score_before > 0 else 0
            if drop_pct > 10:  # More than 10% drop triggers rollback
                return True, f"DQ score dropped {drop_pct:.1f}% ({score_before:.3f} → {score_after:.3f})"
        return False, "OK"


class DependencyChecker:
    """
    Verifies that downstream tables are not broken after a fix.
    Uses UC lineage to find dependent tables and checks their freshness + DQ.
    """
    
    @staticmethod
    def verify_downstream_tables(affected_table: str, state: SwarmState) -> dict:
        """
        Check all downstream tables of the affected table.
        Returns a dict with verification results.
        """
        if not affected_table:
            return {"checked": 0, "issues": []}
        
        # Get downstream tables from UC lineage
        try:
            lineage_df = spark.sql(f"""
                SELECT DISTINCT target_table_full_name AS downstream
                FROM system.access.table_lineage
                WHERE source_table_full_name = '{affected_table}'
                AND target_table_full_name IS NOT NULL
                LIMIT 20
            """)
            downstream_tables = [row["downstream"] for row in lineage_df.collect()]
        except Exception as e:
            logger.warning(f"Lineage query failed: {e}")
            downstream_tables = []
        
        issues = []
        for table in downstream_tables:
            try:
                # Check if table is queryable
                spark.sql(f"SELECT 1 FROM {table} LIMIT 1")
                logger.info(f"Downstream check OK: {table}")
            except Exception as e:
                issues.append({"table": table, "error": str(e)})
                logger.warning(f"Downstream check FAILED: {table} — {e}")
        
        result = {
            "checked": len(downstream_tables),
            "issues": issues,
            "all_healthy": len(issues) == 0
        }
        
        if issues:
            state = self._log_dependency_issues(state, issues)
        
        return result
    
    @staticmethod
    def _log_dependency_issues(state: SwarmState, issues: list):
        """Log dependency issues to swarm state messages."""
        for issue in issues:
            state["messages"].append({
                "agent": "dependency_checker",
                "action": "downstream_failure",
                "details": f"{issue['table']}: {issue['error']}"
            })
        return state


def record_fix_history(state: SwarmState, fix_duration_sec: float):
    """Log the swarm fix attempt to swarm_fix_history table."""
    try:
        import uuid as _uuid
        fix_id = str(_uuid.uuid4())
        spark.sql(f"""
            INSERT INTO {METADATA_CATALOG}.metadata.swarm_fix_history
            (fix_id, run_id, trigger_source, error_class, error_message,
             affected_table, fix_applied, dq_score_before, dq_score_after,
             resolution_status, fix_timestamp, swarm_duration_sec, token_cost_usd,
             rollback_performed, circuit_breaker_triggered, post_mortem_path)
            VALUES (
                '{fix_id}',
                '{state.get('run_id', '')}',
                '{state.get('trigger_source', '')}',
                '{state.get('error_class', '')}',
                '{str(state.get('error_log', ''))[:500].replace("'", "''")}',
                '{state.get('affected_table', '')}',
                '{str(state.get('metadata_delta', ''))[:500].replace("'", "''")}',
                {state.get('dq_score_before', 0.0)},
                {state.get('dq_score_after', 0.0)},
                '{state.get('final_status', 'unknown')}',
                CURRENT_TIMESTAMP(),
                {fix_duration_sec},
                {state.get('token_cost_usd', 0.0)},
                {state.get('rollback_performed', False)},
                {state.get('circuit_breaker_triggered', False)},
                '{state.get('technical_docs_payload', {}).get('post_mortem', '')[:200].replace("'", "''")}'
            )
        """)
        state["fix_history_recorded"] = True
        logger.info(f"Fix history recorded: {fix_id}")
    except Exception as e:
        logger.warning(f"Failed to record fix history: {e}")
    return state


print("✓ RollbackManager initialized (Delta RESTORE on DQ drop >10%)")
print("✓ DependencyChecker initialized (UC lineage verification)")
print("✓ Fix history logging enabled (swarm_fix_history table)")

# COMMAND ----------

# DBTITLE 1,LangGraph State Graph Construction
# ═══════════════════════════════════════════════════════════════
# Cell 13: LangGraph State Graph Construction
# ═══════════════════════════════════════════════════════════════
#
# This cell wires all agents into a LangGraph StateGraph with conditional
# routing. The Supervisor acts as the central router, dispatching to
# sub-agents based on the current phase and next_agent field.
# ─────────────────────────────────────────────────────────────────

if HAS_LANGGRAPH:

    def build_swarm_graph(llm: LLMServingClient) -> CompiledStateGraph:
        """
        Constructs and compiles the LangGraph state graph for the agent swarm.

        Graph Topology:

            START → supervisor → (conditional) → triage / ba / qa / deployment / human_escalation / END
                                ↑                                    |
                                └────────── (sub-agents return to supervisor) ──┘

        The supervisor node is re-entered after each sub-agent completes,
        enabling the Plan-Execute-Verify-Deploy loop with iterative retries.
        """

        # Instantiate all agents
        supervisor = SupervisorAgent(llm)
        triage = TriageAgent(llm)
        ba = BusinessAnalystAgent(llm)
        data_engineer = DataEngineerAgent(llm)
        qa = QAValidationAgent(llm)
        deployment = DeploymentAgent(llm)

        # ── Define the routing function ──
        def route_from_supervisor(state: SwarmState) -> str:
            """Determines which node to transition to based on next_agent."""
            next_agent = state.get("next_agent", "")
            phase = state.get("phase", "")

            # Check for halt conditions
            if phase == SwarmPhase.HALTED.value:
                return "human_escalation"
            if phase == SwarmPhase.COMPLETE.value or next_agent == "__end__":
                return END

            routing_map = {
                AgentRole.TRIAGE.value: "triage",
                AgentRole.BA.value: "ba",
                AgentRole.DATA_ENGINEER.value: "data_engineer",
                AgentRole.QA.value: "qa",
                AgentRole.DEPLOYMENT.value: "deployment",
                AgentRole.HUMAN_ESCALATION.value: "human_escalation",
            }
            return routing_map.get(next_agent, "triage")

        # ── Define the sub-agent → supervisor return router ──
        def route_back_to_supervisor(state: SwarmState) -> str:
            """After a sub-agent completes, return to supervisor for next routing."""
            phase = state.get("phase", "")
            if phase == SwarmPhase.HALTED.value:
                return "human_escalation"
            if phase == SwarmPhase.COMPLETE.value:
                return END
            return "supervisor"

        # ── Human escalation node ──
        def human_escalation_node(state: SwarmState) -> SwarmState:
            reason = state.get("human_escalation_reason", "Unknown reason")
            logger.error(f"HUMAN ESCALATION: {reason}")
            logger.error(f"Session: {state.get('session_id')}, Run: {state.get('run_id')}")
            logger.error(f"Attempts: {state.get('attempt_counter')}, Cost: ${state.get('token_cost_usd', 0):.2f}")

            # Write escalation report
            escalation_md = f"""# Human Escalation Required

**Session:** {state.get('session_id')}
**Run ID:** {state.get('run_id')}
**Reason:** {reason}
**Attempts:** {state.get('attempt_counter')}
**Token Cost:** ${state.get('token_cost_usd', 0):.2f}

## Error Log
{state.get('error_log', 'N/A')[:3000]}

## Actions Taken
{json.dumps(state.get('messages', []), indent=2, default=str)[:3000]}
"""
            escalation_path = f"{DOCS_VOLUME_PATH}/escalations/{state.get('session_id', 'unknown')}.md"
            write_technical_markdown_doc(escalation_path, escalation_md)

            state["final_status"] = "escalated"
            return state

        # ── Build the graph ──
        workflow = StateGraph(SwarmState)

        # Add all nodes
        workflow.add_node("supervisor", supervisor.process)
        workflow.add_node("triage", triage.process)
        workflow.add_node("ba", ba.process)
        workflow.add_node("data_engineer", data_engineer.process)
        workflow.add_node("qa", qa.process)
        workflow.add_node("deployment", deployment.process)
        workflow.add_node("human_escalation", human_escalation_node)

        # Entry point → supervisor
        workflow.add_edge(START, "supervisor")

        # Supervisor → conditional routing to sub-agents
        workflow.add_conditional_edges(
            "supervisor",
            route_from_supervisor,
            {
                "triage": "triage",
                "ba": "ba",
                "data_engineer": "data_engineer",
                "qa": "qa",
                "deployment": "deployment",
                "human_escalation": "human_escalation",
                END: END,
            }
        )

        # Sub-agents → conditional return to supervisor or END
        for agent_node in ["triage", "ba", "data_engineer", "qa", "deployment"]:
            workflow.add_conditional_edges(
                agent_node,
                route_back_to_supervisor,
                {
                    "supervisor": "supervisor",
                    "human_escalation": "human_escalation",
                    END: END,
                }
            )

        # Human escalation → END
        workflow.add_edge("human_escalation", END)

        # Compile with memory checkpointer for state persistence
        checkpointer = MemorySaver()
        compiled_graph = workflow.compile(checkpointer=checkpointer)

        logger.info("LangGraph swarm compiled successfully")
        return compiled_graph

    # Build the graph
    swarm_graph = build_swarm_graph(llm_client)

else:
    swarm_graph = None
    logger.warning("LangGraph not available. Using fallback sequential executor.")

# COMMAND ----------

# DBTITLE 1,Scenario A — Bronze Schema Drift
# ═══════════════════════════════════════════════════════════════
# Cell 14: Scenario A — Bronze Ingestion & Schema Drift (PolicyCenter)
# ═══════════════════════════════════════════════════════════════
#
# Scenario: An inbound file from PolicyCenter breaks the Bronze layer
# due to a new, unmapped ACORD-standard coverage field.
#
# Swarm Response:
#   1. Triage: Fetches error, classifies as schema_drift
#   2. BA: Fetches PolicyCenter Bronze mapping, appends new ACORD field
#   3. Data Engineer: Updates metadata store, pushes UC comments, generates ALTER TABLE DDL
#   4. QA: Validates sandbox run with new schema
#   5. Deployment: Triggers pipeline repair, writes post-mortem, creates Git PR
# ─────────────────────────────────────────────────────────────────

def simulate_scenario_a() -> dict:
    """
    Simulates Scenario A: PolicyCenter Bronze Schema Drift.

    Creates a mock error state as if a PolicyCenter ingestion job failed
    due to an unmapped ACORD coverage field, then runs the full swarm.
    """
    print("=" * 70)
    print("SCENARIO A: PolicyCenter Bronze Schema Drift")
    print("=" * 70)

    # Simulated error from a failed Bronze ingestion job
    mock_run_id = "894776717783668"
    mock_error = """
    AnalysisException: [UNRESOLVED_COLUMN.IN] An unresolved column
    'new_acord_coverage_field_cd' was found in the PolicyCenter Bronze
    ingestion. The column does not exist in the current mapping document
    for pc_insurance_prod.bronze.policycenter_raw.
    Schema mismatch: expected 45 columns, found 46 columns.
    ACORD standard field: 'new_acord_coverage_field_cd' (string, nullable).
    """

    # Initialize state
    state = init_swarm_state(run_id=mock_run_id, trigger_source="alert")
    state["error_log"] = mock_error
    state["affected_x_center"] = "PolicyCenter"
    state["affected_layer"] = "bronze"
    state["affected_table"] = f"{PROD_CATALOG}.bronze.policycenter_raw"
    state["phase"] = SwarmPhase.PLAN.value

    print(f"\nInitial State:")
    print(f"  Run ID: {state['run_id']}")
    print(f"  X-Center: {state['affected_x_center']}")
    print(f"  Layer: {state['affected_layer']}")
    print(f"  Error: {mock_error.strip()[:100]}...")

    # The BA Agent would produce a delta like this for the ACORD field:
    expected_ba_delta = {
        "column_mappings": {
            "new_acord_coverage_field_cd": {
                "target_col": "new_acord_coverage_field_cd",
                "data_type": "string",
                "nullable": True,
                "default_value": None,
                "transformation": None
            }
        },
        "transformation_rules": [{
            "rule_name": "auto_schema_evolution_acord",
            "rule_type": "schema_evolution",
            "rule_sql": "ALTER TABLE pc_insurance_prod.bronze.policycenter_raw ADD COLUMN IF NOT EXISTS new_acord_coverage_field_cd STRING",
            "applies_to": "new_acord_coverage_field_cd",
            "condition": None
        }],
        "business_description": "PolicyCenter Bronze layer — added new ACORD-standard coverage field 'new_acord_coverage_field_cd'. This field represents the ACORD 140-schedule coverage type code for extended property coverage. Auto-detected via schema drift monitoring.",
        "uc_column_comments": {
            "new_acord_coverage_field_cd": "ACORD standard coverage type code for extended property coverage. Added via automated schema drift detection. Nullable — may not appear in all policy exports."
        },
        "justification": "New ACORD-standard coverage field introduced by PolicyCenter v10.4 update. Auto-mapped as nullable string in Bronze layer for backward compatibility. Silver layer transformation will normalize to a lookup table reference."
    }

    print(f"\nExpected BA Delta:")
    print(json.dumps(expected_ba_delta, indent=2)[:800])

    print("\n[In production, the full swarm graph would execute here]")
    print("  Supervisor → Triage → BA → Data Engineer → QA → Deployment")

    # Execute the swarm graph (uncomment in production)
    # result = execute_swarm(state)
    # print(f"\nFinal Status: {result.get('final_status')}")
    # print(f"Attempts: {result.get('attempt_counter')}, Cost: ${result.get('token_cost_usd', 0):.2f}")

    return {"scenario": "A", "state": state, "expected_delta": expected_ba_delta}

# COMMAND ----------

# DBTITLE 1,Scenario B — Silver DQ Constraint
# ═══════════════════════════════════════════════════════════════
# Cell 15: Scenario B — Silver DQ Constraint (MGA Feed)
# ═══════════════════════════════════════════════════════════════
#
# Scenario: A commercial policy payload from an MGA feed introduces
# a null value into a mandatory `deductible_amount` field, breaking a
# NOT NULL constraint in the Silver layer.
#
# Swarm Response:
#   1. Triage: Fetches error, classifies as data_quality
#   2. BA: Modifies Silver mapping for MGA_Feed to add conditional fallback
#      rule (COALESCE nulls to 0 for commercial group code)
#   3. Data Engineer: Updates metadata, generates DDL for fallback rule
#   4. QA: Validates sandbox with COALESCE applied — nulls become 0
#   5. Deployment: Triggers repair, documents the business justification
# ─────────────────────────────────────────────────────────────────

def simulate_scenario_b() -> dict:
    """
    Simulates Scenario B: MGA Feed Silver DQ Constraint Failure.
    """
    print("=" * 70)
    print("SCENARIO B: MGA Feed Silver Data Quality Constraint")
    print("=" * 70)

    mock_run_id = "894776717783669"
    mock_error = """
    DeltaAnalysisException: [NOT_NULL_VIOLATION] The NOT NULL constraint
    on column 'deductible_amount' in table pc_insurance_prod.silver.mga_policy_silver
    was violated. 1,247 rows from MGA_Feed source system (group_code='COMMERCIAL')
    contain NULL values for deductible_amount.
    DLT expectation 'expect_deductible_not_null' FAILED.
    """

    state = init_swarm_state(run_id=mock_run_id, trigger_source="alert")
    state["error_log"] = mock_error
    state["affected_x_center"] = "MGA_Feed"
    state["affected_layer"] = "silver"
    state["affected_table"] = f"{PROD_CATALOG}.silver.mga_policy_silver"
    state["phase"] = SwarmPhase.PLAN.value

    print(f"\nInitial State:")
    print(f"  Run ID: {state['run_id']}")
    print(f"  X-Center: {state['affected_x_center']}")
    print(f"  Layer: {state['affected_layer']}")
    print(f"  Error: {mock_error.strip()[:100]}...")

    # The BA Agent would produce a delta like this:
    expected_ba_delta = {
        "column_mappings": {},
        "transformation_rules": [{
            "rule_name": "commercial_deductible_null_fallback",
            "rule_type": "default_value",
            "rule_sql": "COALESCE(deductible_amount, 0)",
            "applies_to": "deductible_amount",
            "condition": "source_group_code = 'COMMERCIAL' AND source_system = 'MGA_Feed'"
        }],
        "business_description": "MGA_Feed Silver layer — conditional fallback rule for commercial policies. When source_group_code is 'COMMERCIAL' and deductible_amount is NULL, map to 0. Justification: Commercial MGA feeds from smaller carriers frequently omit deductible_amount for bundled coverage packages where the deductible is embedded in the premium calculation, not surfaced as a standalone field.",
        "uc_column_comments": {
            "deductible_amount": "Deductible amount for the policy coverage. For MGA commercial policies, NULL values are mapped to 0 per business rule 'commercial_deductible_null_fallback'. See mapping document v2 for details."
        },
        "justification": "MGA commercial feeds from smaller carriers omit deductible_amount for bundled coverage packages. COALESCE to 0 is the business-accepted fallback for commercial group codes only. Personal lines still enforce strict NOT NULL."
    }

    print(f"\nExpected BA Delta (conditional fallback rule):")
    print(json.dumps(expected_ba_delta, indent=2)[:800])

    print("\n[In production, the full swarm graph would execute here]")
    print("  Supervisor → Triage → BA (adds COALESCE rule) → Data Engineer → QA → Deployment")

    return {"scenario": "B", "state": state, "expected_delta": expected_ba_delta}

# COMMAND ----------

# DBTITLE 1,Scenario C — Gold Analytics Variance
# ═══════════════════════════════════════════════════════════════
# Cell 16: Scenario C — Gold Analytics Mapping Variance (ClaimCenter)
# ═══════════════════════════════════════════════════════════════
#
# Scenario: A catastrophic weather event causes localized claim volumes
# from ClaimCenter to skyrocket, pushing a Gold Layer Loss Ratio KPI
# over 500% and triggering a data validation exception.
#
# Swarm Response:
#   1. Triage: Fetches error, classifies as mapping_variance
#   2. BA: Analyzes operational metrics, determines valid real-world anomaly
#      Updates metadata control table threshold parameters for that region/date
#   3. Triage: Auto-generates technical incident report documenting extreme event
#   4. Deployment: Clears execution run boundary block, writes incident report
# ─────────────────────────────────────────────────────────────────

def simulate_scenario_c() -> dict:
    """
    Simulates Scenario C: ClaimCenter Gold Analytics Mapping Variance.
    """
    print("=" * 70)
    print("SCENARIO C: ClaimCenter Gold Analytics Mapping Variance")
    print("=" * 70)

    mock_run_id = "894776717783670"
    mock_error = """
    DLTValidationException: Gold layer validation failed.
    Expectation 'loss_ratio_threshold_check' FAILED.
    Table: pc_insurance_prod.gold.claims_kpi_gold
    Metric: loss_ratio = 547.3% (threshold max: 200%)
    Region: FLORIDA | Date Range: 2026-09-20 to 2026-09-25
    Context: Hurricane Helene — CAT 4 landfall. 18,432 claims filed in 5 days.
    Previous 5-day average: 342 claims. Variance: +5290%.
    """

    state = init_swarm_state(run_id=mock_run_id, trigger_source="alert")
    state["error_log"] = mock_error
    state["affected_x_center"] = "ClaimCenter"
    state["affected_layer"] = "gold"
    state["affected_table"] = f"{PROD_CATALOG}.gold.claims_kpi_gold"
    state["phase"] = SwarmPhase.PLAN.value

    print(f"\nInitial State:")
    print(f"  Run ID: {state['run_id']}")
    print(f"  X-Center: {state['affected_x_center']}")
    print(f"  Layer: {state['affected_layer']}")
    print(f"  Error: {mock_error.strip()[:100]}...")

    # The BA Agent would produce a delta like this:
    expected_ba_delta = {
        "column_mappings": {},
        "transformation_rules": [{
            "rule_name": "catastrophic_event_threshold_override",
            "rule_type": "conditional",
            "rule_sql": "UPDATE pc_insurance_dev.metadata.threshold_controls SET max_loss_ratio = 600 WHERE metric_name = 'loss_ratio' AND region = 'FLORIDA' AND event_type = 'CATASTROPHIC'",
            "applies_to": "loss_ratio",
            "condition": "region = 'FLORIDA' AND event_type = 'CATASTROPHIC' AND event_date BETWEEN '2026-09-20' AND '2026-09-25'"
        }],
        "business_description": "ClaimCenter Gold layer — threshold override for catastrophic weather events. Loss ratio KPI threshold temporarily raised to 600% for Florida region during Hurricane Helene (2026-09-20 to 2026-09-25). This is a valid real-world anomaly driven by CAT 4 landfall with 18,432 claims in 5 days.",
        "uc_column_comments": {
            "loss_ratio": "Loss Ratio KPI. Standard threshold: 200%. Catastrophic event override: 600% (applied for declared CAT events by region/date range). See threshold_controls metadata table for active overrides."
        },
        "justification": "Hurricane Helene CAT 4 landfall caused 18,432 claims in 5 days in Florida (+5290% variance). Loss ratio of 547% is a valid real-world anomaly for a catastrophic event, not a data quality issue. Threshold override to 600% for this region/date range is the business-accepted response. Triage Agent to generate incident report classifying this as an accepted variance."
    }

    print(f"\nExpected BA Delta (threshold override):")
    print(json.dumps(expected_ba_delta, indent=2)[:1000])

    print("\n[In production, the full swarm graph would execute here]")
    print("  Supervisor → Triage → BA (updates threshold) → QA → Deployment")
    print("  Triage also auto-generates incident report classifying as accepted variance")

    return {"scenario": "C", "state": state, "expected_delta": expected_ba_delta}

# COMMAND ----------

# DBTITLE 1,Security, Network & Budget Guardrails
# ═══════════════════════════════════════════════════════════════
# Cell 17: Security, Network & Budget Guardrails
# ═══════════════════════════════════════════════════════════════
#
# This cell implements the three guardrail pillars:
#   1. Network Access & Security: UC Network Rules for HTTPS egress
#   2. Sandbox Isolation: Agents cannot touch production directly
#   3. Infinite-Loop Token Protection: Max 5 ReAct loops + dollar cap
# ─────────────────────────────────────────────────────────────────


class SecurityGuardrails:
    """
    Encapsulates all security, network, and budget guardrails for the swarm.
    Called by the Supervisor before each iteration and by UC tools before
    any metadata modification.
    """

    # ── 1. Network Access & Security ──

    ALLOWED_EGRESS_DOMAINS = [
        "*.cloud.databricks.com",       # Local workspace domain
        "github.com",                   # Enterprise Git (metadata + docs)
        "api.github.com",               # Git API for PR creation
        "gitlab.com",                   # Alternative Git provider
        "dev.azure.com",                # Azure DevOps
    ]

    @staticmethod
    def validate_network_egress(target_url: str) -> bool:
        """Validate that a URL is in the allowed egress list for UC Network Rules."""
        from urllib.parse import urlparse
        parsed = urlparse(target_url)
        hostname = parsed.hostname or ""

        for pattern in SecurityGuardrails.ALLOWED_EGRESS_DOMAINS:
            if pattern.startswith("*."):
                if hostname.endswith(pattern[2:]) or hostname == pattern[2:]:
                    return True
            elif hostname == pattern:
                return True

        logger.warning(f"Network egress blocked: {hostname} not in allowed list")
        return False

    @staticmethod
    def create_uc_network_rules():
        """
        Creates Unity Catalog Network Rules restricting HTTPS egress to only
        the local Databricks workspace domain and target enterprise Git repos.

        In production, run this via the Databricks CLI or REST API:
            databricks uc create-network-rule ...
        """
        # Example: Create UC network rules (run once during setup)
        print("UC Network Rules Configuration:")
        print("  Rule: pc_insurance_workspace_egress")
        print(f"    Allowed domains: {SecurityGuardrails.ALLOWED_EGRESS_DOMAINS}")
        print("    Port: 443 (HTTPS only)")
        print("    Protocol: HTTPS")
        print()
        print("  To deploy via CLI:")
        print("    databricks uc create-network-rule --name pc_insurance_egress \\")
        print("          --destination-domains '*.cloud.databricks.com,github.com' \\")
        print("          --port-ranges 443")

    # ── 2. Sandbox Isolation ──

    @staticmethod
    def validate_sandbox_isolation(state: SwarmState, operation: str) -> tuple:
        """
        Ensures agents are operating within the staging catalog, not production.
        Returns (is_safe, reason).
        """
        phase = state.get("phase", "")
        sandbox_catalog = state.get("sandbox_catalog", "")

        # Before DEPLOY phase, all operations must target the staging catalog
        if phase in [SwarmPhase.EXECUTE.value, SwarmPhase.VERIFY.value]:
            if not sandbox_catalog and operation != "read_mapping":
                return False, "No staging catalog provisioned for write operations"

            # Verify we're not writing to the production catalog
            if PROD_CATALOG in str(state.get("affected_table", "")) and phase == SwarmPhase.VERIFY.value:
                return False, f"Attempted production write during VERIFY phase. Use staging catalog instead."

        # DEPLOY phase is the only phase where production repair is allowed
        if phase == SwarmPhase.DEPLOY.value and not state.get("validation_passed", False):
            return False, "Cannot deploy to production without passing QA validation"

        return True, "OK"

    # ── 3. Infinite-Loop Token Protection ──

    @staticmethod
    def check_budget(state: SwarmState) -> tuple:
        """
        Returns (within_budget, reason).
        Halt conditions:
          - attempt_counter >= MAX_REACT_ITERATIONS (5)
          - token_cost_usd >= MAX_TOKEN_BUDGET_USD ($25)
        """
        attempts = state.get("attempt_counter", 0)
        cost = state.get("token_cost_usd", 0.0)

        if attempts >= MAX_REACT_ITERATIONS:
            return False, f"Max ReAct iterations exceeded: {attempts} >= {MAX_REACT_ITERATIONS}"

        if cost >= MAX_TOKEN_BUDGET_USD:
            return False, f"Token budget exceeded: ${cost:.2f} >= ${MAX_TOKEN_BUDGET_USD}"

        return True, f"OK (attempts: {attempts}/{MAX_REACT_ITERATIONS}, cost: ${cost:.2f}/${MAX_TOKEN_BUDGET_USD})"

    @staticmethod
    def generate_escalation_alert(state: SwarmState) -> str:
        """Generate a human-readable escalation alert when guardrails trigger."""
        return f"""🚨 AGENT SWARM ESCALATION ALERT 🚨

**Session:** {state.get('session_id', 'N/A')}
**Run ID:** {state.get('run_id', 'N/A')}
**Trigger:** {state.get('human_escalation_reason', 'Unknown')}

**Execution Summary:**
- Attempts: {state.get('attempt_counter', 0)} / {MAX_REACT_ITERATIONS}
- Token Cost: ${state.get('token_cost_usd', 0):.2f} / ${MAX_TOKEN_BUDGET_USD}
- Error Class: {state.get('error_class', 'unknown')}
- Affected: {state.get('affected_x_center', '?')}/{state.get('affected_layer', '?')}

**Actions Taken:**
{json.dumps(state.get('messages', []), indent=2, default=str)[:2000]}

**Next Steps for Human Engineers:**
1. Review the error log and actions taken by the swarm
2. Check the staging catalog for partial results: {state.get('sandbox_catalog', 'N/A')}
3. Read the post-mortem in: {DOCS_VOLUME_PATH}/escalations/
4. Apply manual fix if the swarm could not resolve autonomously
5. Update the mapping document manually if needed
6. Trigger pipeline repair manually once fixed
"""


# ── Initialize guardrails ──
guardrails = SecurityGuardrails()

# Display the guardrail configuration
print("Security Guardrails Initialized:")
print(f"  Max ReAct Iterations: {MAX_REACT_ITERATIONS}")
print(f"  Max Token Budget: ${MAX_TOKEN_BUDGET_USD}")
print(f"  Allowed Egress: {SecurityGuardrails.ALLOWED_EGRESS_DOMAINS}")
print(f"  Staging Catalog Prefix: {STAGING_CATALOG_PREFIX}")
print(f"  Production Catalog: {PROD_CATALOG}")
print(f"  Docs Volume: {DOCS_VOLUME_PATH}")

# COMMAND ----------

# DBTITLE 1,UC Function Deployment DDL
# ═══════════════════════════════════════════════════════════════
# Cell 18: UC Function Deployment Templates (SQL DDL)
# ═══════════════════════════════════════════════════════════════
#
# These SQL statements register the Python toolkit functions as
# Unity Catalog AI Functions. Run these in a SQL notebook or the
# Databricks SQL editor to deploy them to the UC metastore.
# ─────────────────────────────────────────────────────────────────

UC_FUNCTION_DDL = '''
-- ────────────────────────────────────────────────────────────
-- Unity Catalog AI Function: get_pipeline_error_log
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pc_insurance.metadata.get_pipeline_error_log(run_id STRING)
RETURNS STRING
LANGUAGE PYTHON
RETURN {
    import json
    from databricks.sdk import WorkspaceClient
    _w = WorkspaceClient()
    run_info = _w.jobs.get_run(run_id=int(run_id))
    state = run_info.state.result_state.value if run_info.state else "UNKNOWN"
    result = {"run_id": run_id, "state": state, "error_message": "", "error_trace": ""}
    tasks = run_info.tasks or []
    for task in tasks:
        if task.state and task.state.result_state.value == "FAILED":
            task_run_id = str(task.run_id)
            try:
                run_output = _w.jobs.get_run_output(run_id=int(task_run_id))
                result["error_message"] = run_output.error or ""
            except Exception as e:
                result["error_message"] = str(e)
            break
    return json.dumps(result)
};

-- ────────────────────────────────────────────────────────────
-- Unity Catalog AI Function: read_mapping_document
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pc_insurance.metadata.read_mapping_document(x_center STRING, layer STRING)
RETURNS STRING
LANGUAGE PYTHON
RETURN {
    import json
    query = f""""
        SELECT mapping_json, business_description, version
        FROM pc_insurance.metadata.mapping_documents
        WHERE x_center = '{x_center}' AND layer = '{layer}' AND is_active = true
        ORDER BY version DESC LIMIT 1
    """
    df = spark.sql(query)
    rows = df.collect()
    if not rows:
        return json.dumps({"exists": False})
    row = rows[0]
    return json.dumps({
        "mapping": json.loads(row["mapping_json"]) if row["mapping_json"] else {},
        "business_description": row["business_description"],
        "version": int(row["version"]),
        "exists": True
    })
};

-- ────────────────────────────────────────────────────────────
-- Unity Catalog AI Function: update_uc_catalog_comments
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pc_insurance.metadata.update_uc_catalog_comments(
    table_name STRING, column_comments STRING
)
RETURNS STRING
LANGUAGE PYTHON
RETURN {
    import json
    comments = json.loads(column_comments)
    updated = []
    for col_name, comment in comments.items():
        if col_name == "__table_comment__":
            spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES ('comment' = '{comment.replace(chr(39), chr(39)+chr(39))}')")
            updated.append(col_name)
        else:
            spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} SET COMMENT '{comment.replace(chr(39), chr(39)+chr(39))}'")
            updated.append(col_name)
    return json.dumps({"table_name": table_name, "columns_updated": updated, "status": "success"})
};

-- ────────────────────────────────────────────────────────────
-- Unity Catalog AI Function: trigger_pipeline_repair
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION pc_insurance.metadata.trigger_pipeline_repair(run_id STRING)
RETURNS STRING
LANGUAGE PYTHON
RETURN {
    import json
    from databricks.sdk import WorkspaceClient
    _w = WorkspaceClient()
    _w.jobs.repair_run(run_id=int(run_id), rerun_all_failed_tasks=True, rerun_dependent_tasks=True)
    return json.dumps({"run_id": run_id, "status": "repair_triggered"})
};

-- ────────────────────────────────────────────────────────────
-- Metadata Tables (DDL for initial setup)
-- ────────────────────────────────────────────────────────────
CREATE CATALOG IF NOT EXISTS pc_insurance;
CREATE SCHEMA IF NOT EXISTS pc_insurance.metadata;

CREATE TABLE IF NOT EXISTS pc_insurance.metadata.mapping_documents (
    x_center STRING NOT NULL,
    layer STRING NOT NULL,
    version INT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    mapping_json STRING,
    business_description STRING,
    updated_by STRING,
    updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (x_center, layer);

CREATE TABLE IF NOT EXISTS pc_insurance.metadata.threshold_controls (
    metric_name STRING NOT NULL,
    region STRING,
    event_type STRING,
    max_threshold DOUBLE,
    min_threshold DOUBLE,
    effective_start_date DATE,
    effective_end_date DATE,
    is_active BOOLEAN DEFAULT true,
    updated_by STRING,
    updated_at TIMESTAMP
) USING DELTA;

-- UC Volume for technical documentation
CREATE VOLUME IF NOT EXISTS pc_insurance.metadata.technical_docs;
'''

print("UC Function Deployment DDL generated.")
print("Run the SQL above in a SQL notebook or the Databricks SQL editor to deploy.")
print("\nFunctions defined:")
print("  - pc_insurance.metadata.get_pipeline_error_log(run_id)")
print("  - pc_insurance.metadata.read_mapping_document(x_center, layer)")
print("  - pc_insurance.metadata.update_uc_catalog_comments(table_name, column_comments)")
print("  - pc_insurance.metadata.trigger_pipeline_repair(run_id)")
print("\nTables defined:")
print("  - pc_insurance.metadata.mapping_documents")
print("  - pc_insurance.metadata.threshold_controls")
print("\nVolume defined:")
print("  - pc_insurance.metadata.technical_docs")

# COMMAND ----------

# DBTITLE 1,Swarm Executor & Main Entry Point
# ═══════════════════════════════════════════════════════════════
# Cell 19: Swarm Executor & Main Entry Point
# ═══════════════════════════════════════════════════════════════

def execute_swarm(initial_state: SwarmState, use_graph: bool = True) -> SwarmState:
    """
    Main entry point for executing the autonomous agent swarm.

    Args:
        initial_state: A SwarmState dict initialized with run_id and error context.
        use_graph: If True and LangGraph is available, use the compiled graph.
                   Otherwise, use the fallback sequential executor.

    Returns:
        Final SwarmState after the swarm completes (resolved, halted, or escalated).
    """
    print("=" * 70)
    print("AUTONOMOUS AGENT SWARM — INITIATED")
    print(f"Session: {initial_state.get('session_id')}")
    print(f"Run ID: {initial_state.get('run_id')}")
    print(f"Trigger: {initial_state.get('trigger_source')}")
    print("=" * 70)

    # ── Pre-flight guardrail check ──
    within_budget, reason = guardrails.check_budget(initial_state)
    if not within_budget:
        print(f"PRE-FLIGHT CHECK FAILED: {reason}")
        initial_state["final_status"] = "halted"
        initial_state["human_escalation_reason"] = reason
        return initial_state

    if use_graph and swarm_graph is not None:
        # ── LangGraph execution ──
        print("\nExecuting via LangGraph state graph...")
        try:
            config = {"configurable": {"thread_id": initial_state["session_id"]}}
            final_state = swarm_graph.invoke(initial_state, config=config)
            return final_state
        except Exception as e:
            logger.error(f"LangGraph execution failed: {e}")
            print(f"LangGraph failed, falling back to sequential executor: {e}")
            return _execute_sequential(initial_state)
    else:
        # ── Fallback sequential executor ──
        print("\nExecuting via sequential fallback executor...")
        return _execute_sequential(initial_state)


def _execute_sequential(state: SwarmState) -> SwarmState:
    """
    Fallback executor that runs the Plan-Execute-Verify-Deploy loop
    sequentially without LangGraph. Used when LangGraph is not installed.
    """
    # Instantiate agents
    supervisor = SupervisorAgent(llm_client)
    triage = TriageAgent(llm_client)
    ba = BusinessAnalystAgent(llm_client)
    de = DataEngineerAgent(llm_client)
    qa = QAValidationAgent(llm_client)
    deploy = DeploymentAgent(llm_client)

    max_iters = MAX_REACT_ITERATIONS

    for iteration in range(max_iters):
        print(f"\n--- Iteration {iteration + 1}/{max_iters} ---")

        # Guardrail check
        within_budget, reason = guardrails.check_budget(state)
        if not within_budget:
            print(f"GUARDRAIL TRIGGERED: {reason}")
            state["phase"] = SwarmPhase.HALTED.value
            state["human_escalation_reason"] = reason
            state["final_status"] = "halted"
            alert = guardrails.generate_escalation_alert(state)
            print(alert)
            return state

        phase = state.get("phase", SwarmPhase.PLAN.value)

        if phase == SwarmPhase.PLAN.value:
            state = supervisor.process(state)
            if state.get("next_agent") == AgentRole.TRIAGE.value:
                state = triage.process(state)

        elif phase == SwarmPhase.EXECUTE.value:
            state = supervisor.process(state)
            if state.get("next_agent") == AgentRole.BA.value:
                state = ba.process(state)
            if state.get("next_agent") == AgentRole.DATA_ENGINEER.value:
                state = de.process(state)

        elif phase == SwarmPhase.VERIFY.value:
            state = supervisor.process(state)
            if state.get("next_agent") == AgentRole.QA.value:
                state = qa.process(state)

        elif phase == SwarmPhase.DEPLOY.value:
            state = supervisor.process(state)
            if state.get("next_agent") == AgentRole.DEPLOYMENT.value:
                state = deploy.process(state)

        elif phase == SwarmPhase.COMPLETE.value:
            break

        elif phase == SwarmPhase.HALTED.value:
            alert = guardrails.generate_escalation_alert(state)
            print(alert)
            break

    # ── Print final summary ──
    print("\n" + "=" * 70)
    print("SWARM EXECUTION COMPLETE")
    print("=" * 70)
    print(f"  Final Status: {state.get('final_status', 'unknown')}")
    print(f"  Total Attempts: {state.get('attempt_counter', 0)}")
    print(f"  Token Usage: {state.get('token_usage', 0)} tokens")
    print(f"  Token Cost: ${state.get('token_cost_usd', 0):.2f}")
    print(f"  Phase: {state.get('phase', 'unknown')}")
    print(f"  Error Class: {state.get('error_class', 'N/A')}")
    print(f"  Mapping Updated: {state.get('metadata_update_applied', False)}")
    print(f"  Validation Passed: {state.get('validation_passed', False)}")
    print(f"  UC Comments Updated: {state.get('uc_comments_updated', False)}")
    print(f"  Git PR: {state.get('git_pr_url', 'N/A')}")

    if state.get("messages"):
        print(f"\n  Action Log ({len(state['messages'])} actions):")
        for msg in state["messages"][-10:]:
            print(f"    [{msg.get('agent')}] {msg.get('action')}: {msg.get('details', '')[:80]}")

    return state


# ── Main Entry Point ─────────────────────────────────────────────

if __name__ == "__main__":
    # ── Run all three scenario simulations ──
    print("\n" + "#" * 70)
    print("# P&C INSURANCE AUTONOMOUS AGENT SWARM — SCENARIO SIMULATIONS")
    print("#" * 70)

    # Scenario A: Bronze Schema Drift (PolicyCenter)
    result_a = simulate_scenario_a()
    print()

    # Scenario B: Silver DQ Constraint (MGA Feed)
    result_b = simulate_scenario_b()
    print()

    # Scenario C: Gold Analytics Variance (ClaimCenter)
    result_c = simulate_scenario_c()
    print()

    # ── To execute the full swarm on a real failure, use: ──
    # state = init_swarm_state(run_id="<failed_run_id>")
    # state["error_log"] = "<error text>"
    # state["affected_x_center"] = "PolicyCenter"
    # state["affected_layer"] = "bronze"
    # final_state = execute_swarm(state)

    print("\n" + "=" * 70)
    print("All scenario simulations complete.")
    print("To execute on a real failure, call execute_swarm(init_swarm_state(run_id='<run_id>'))")
    print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Provision LLM Serving Endpoints
# ═══════════════════════════════════════════════════════════════
# Cell 20: Provision LLM Serving Endpoints
# ═══════════════════════════════════════════════════════════════
# Creates Mosaic AI Model Serving endpoints for the Supervisor and
# Sub-agent LLMs if they don't already exist. Uses Databricks
# Foundation Model APIs (pay-per-token, auto-provisioned).
# ─────────────────────────────────────────────────────────────────

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceConflict, NotFound

_w = WorkspaceClient()

ENDPOINTS_TO_CREATE = [
    {
        "name": SUPERVISOR_LLM_ENDPOINT,
        "model": "databricks-meta-llama-3-3-70b-instruct",
        "purpose": "Supervisor Agent — orchestrates the swarm, routes tasks",
    },
    {
        "name": SUBAGENT_LLM_ENDPOINT,
        "model": "databricks-meta-llama-3-1-8b-instruct",
        "purpose": "Sub-agents (Triage, BA, DE, QA, Deployment) — specialized reasoning",
    },
]

print("Provisioning LLM Serving Endpoints...")
print()

for ep in ENDPOINTS_TO_CREATE:
    ep_name = ep["name"]
    model_name = ep["model"]
    
    # Check if endpoint already exists
    try:
        existing = _w.serving_endpoints.get(name=ep_name)
        state = existing.state.ready if existing.state else "UNKNOWN"
        print(f"  [EXISTS] {ep_name}")
        print(f"    Model: {model_name}")
        print(f"    State: {state}")
        print(f"    Purpose: {ep['purpose']}")
        print()
        continue
    except NotFound:
        pass  # Endpoint doesn't exist — create it
    except Exception as e:
        print(f"  [CHECK ERROR] {ep_name}: {e}")
        # Try to create anyway
    
    # Create the endpoint
    try:
        from databricks.sdk.service.serving import (
            EndpointCoreConfigInput,
            ServedEntityInput,
            TrafficConfig,
            Route,
        )
        
        config = EndpointCoreConfigInput(
            name=ep_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=model_name,
                    entity_version="1",
                    scale_to_zero_enabled=True,
                    workload_size="Small",
                )
            ],
            traffic_config=TrafficConfig(
                routes=[
                    Route(
                        served_entity_name=model_name,
                        traffic_weight=100,
                    )
                ]
            ),
        )
        
        _w.serving_endpoints.create(name=ep_name, config=config)
        print(f"  [CREATED] {ep_name}")
        print(f"    Model: {model_name}")
        print(f"    Purpose: {ep['purpose']}")
        print()
    
    except Exception as create_err:
        # Fallback: try without TrafficConfig (SDK version differences)
        try:
            from databricks.sdk.service.serving import (
                EndpointCoreConfigInput,
                ServedEntityInput,
            )
            
            config = EndpointCoreConfigInput(
                name=ep_name,
                served_entities=[
                    ServedEntityInput(
                        entity_name=model_name,
                        entity_version="1",
                        scale_to_zero_enabled=True,
                        workload_size="Small",
                    )
                ],
            )
            
            _w.serving_endpoints.create(name=ep_name, config=config)
            print(f"  [CREATED] {ep_name} (fallback config)")
            print(f"    Model: {model_name}")
            print(f"    Purpose: {ep['purpose']}")
            print()
        except Exception as fallback_err:
            print(f"  [FAILED] {ep_name}")
            print(f"    Model: {model_name}")
            print(f"    Error: {fallback_err}")
            print(f"    Purpose: {ep['purpose']}")
            print(f"    Manual: Create via UI > Serving Endpoints > Create")
            print()

# ── Wait for endpoints to be ready (max 120s) ──
print("Waiting for endpoints to initialize (up to 120s)...")
ready_count = 0
for ep in ENDPOINTS_TO_CREATE:
    ep_name = ep["name"]
    for _ in range(12):  # 12 x 10s = 120s max
        try:
            status = _w.serving_endpoints.get(name=ep_name)
            state = status.state.ready if status.state else "UNKNOWN"
            if "READY" in str(state):
                print(f"  {ep_name}: READY ✓")
                ready_count += 1
                break
            elif "FAILED" in str(state):
                print(f"  {ep_name}: FAILED ✗")
                break
        except Exception:
            pass
        time.sleep(10)
    else:
        print(f"  {ep_name}: Still initializing (will be ready shortly)")

print()
print(f"LLM Endpoints: {ready_count}/{len(ENDPOINTS_TO_CREATE)} ready")
print("Endpoints can also be created via: UI > Serving Endpoints > Create Endpoint")

# COMMAND ----------

# DBTITLE 1,Environment Validation & Readiness Check
# Cell 21: Environment Validation & Production Readiness Check
# Validates all components before production use.

from databricks.sdk import WorkspaceClient
_w = WorkspaceClient()
pass_count = 0
fail_count = 0
warn_count = 0

def chk(name, ok, detail=""):
    global pass_count, fail_count
    tag = "PASS" if ok else "FAIL"
    extra = (" -- " + detail) if detail else ""
    print(f"  [{tag}] {name}{extra}")
    if ok:
        pass_count += 1
    else:
        fail_count += 1

def wrn(name, detail=""):
    global warn_count
    extra = (" -- " + detail) if detail else ""
    print(f"  [WARN] {name}{extra}")
    warn_count += 1

print("=" * 70)
print("P&C INSURANCE AGENT SWARM -- ENVIRONMENT VALIDATION")
print("=" * 70)

# 1. Catalog & Schema
print("\n-- 1. Catalog & Schema --")
try:
    spark.sql("USE CATALOG pc_insurance_dev")
    chk("Metadata catalog pc_insurance_dev", True)
except Exception as e:
    chk("Metadata catalog pc_insurance_dev", False, str(e)[:80])
try:
    spark.sql("USE pc_insurance_dev.metadata")
    chk("Schema metadata", True)
except Exception as e:
    chk("Schema metadata", False, str(e)[:80])
try:
    spark.sql("USE CATALOG " + PROD_CATALOG)
    chk("Production catalog " + PROD_CATALOG, True)
except Exception as e:
    chk("Production catalog " + PROD_CATALOG, False, str(e)[:80])

# 2. Metadata Tables
print("\n-- 2. Metadata Tables --")
try:
    mc = spark.sql("SELECT COUNT(*) as c FROM pc_insurance_dev.metadata.mapping_documents").collect()[0]["c"]
    chk("mapping_documents (" + str(mc) + " rows)", mc > 0)
except Exception as e:
    chk("mapping_documents", False, str(e)[:80])
try:
    tc = spark.sql("SELECT COUNT(*) as c FROM pc_insurance_dev.metadata.threshold_controls").collect()[0]["c"]
    chk("threshold_controls (" + str(tc) + " rows)", tc > 0)
except Exception as e:
    chk("threshold_controls", False, str(e)[:80])

# 3. Mapping Coverage
print("\n-- 3. Mapping Document Coverage --")
try:
    rows = spark.sql("SELECT x_center, layer, version FROM pc_insurance_dev.metadata.mapping_documents WHERE is_active = true ORDER BY x_center, layer").collect()
    chk("Active mappings (" + str(len(rows)) + ")", len(rows) > 0)
    for r in rows:
        print("    " + str(r["x_center"]) + "/" + str(r["layer"]) + " (v" + str(r["version"]) + ")")
except Exception as e:
    chk("Mapping query", False, str(e)[:80])

# 4. Threshold Controls
print("\n-- 4. Threshold Controls --")
try:
    rows = spark.sql("SELECT metric_name, region, event_type, max_threshold FROM pc_insurance_dev.metadata.threshold_controls WHERE is_active = true ORDER BY metric_name").collect()
    chk("Active thresholds (" + str(len(rows)) + ")", len(rows) > 0)
    for r in rows:
        reg = r["region"] or "ALL"
        evt = r["event_type"] or "DEFAULT"
        print("    " + str(r["metric_name"]) + " (" + reg + "/" + evt + "): max=" + str(r["max_threshold"]))
except Exception as e:
    chk("Threshold query", False, str(e)[:80])

# 5. UC Volume
print("\n-- 5. UC Volume --")
try:
    vf = dbutils.fs.ls("/Volumes/pc_insurance_dev/metadata/technical_docs")
    chk("UC Volume accessible (" + str(len(vf)) + " subdirs)", len(vf) >= 3)
except Exception as e:
    chk("UC Volume accessible", False, str(e)[:80])

# 6. LLM Endpoints
print("\n-- 6. LLM Serving Endpoints --")
for ep_name in [SUPERVISOR_LLM_ENDPOINT, SUBAGENT_LLM_ENDPOINT]:
    try:
        ep = _w.serving_endpoints.get(name=ep_name)
        state = ep.state.ready if ep.state else "UNKNOWN"
        if "READY" in str(state):
            chk("Endpoint " + ep_name + " READY", True)
        else:
            wrn("Endpoint " + ep_name, "state=" + str(state))
    except Exception:
        wrn("Endpoint " + ep_name, "not found, run provisioning cell")

# 7. LangGraph
print("\n-- 7. LangGraph --")
chk("LangGraph installed", HAS_LANGGRAPH)

# 8. Agent Classes
print("\n-- 8. Agent Classes --")
for c in ["BaseAgent", "SupervisorAgent", "TriageAgent", "BusinessAnalystAgent", "DataEngineerAgent", "QAValidationAgent", "DeploymentAgent"]:
    chk("Class " + c, c in dir())

# 9. Guardrails
print("\n-- 9. Security Guardrails --")
chk("guardrails initialized", "guardrails" in dir())
chk("Max iterations=5", MAX_REACT_ITERATIONS == 5)
chk("Max budget=$25", MAX_TOKEN_BUDGET_USD == 25.0)

# 10. Toolkit Functions
print("\n-- 10. UC Toolkit Functions --")
for fn in ["get_pipeline_error_log", "read_mapping_document", "update_mapping_document", "execute_sandbox_metadata_run", "update_uc_catalog_comments", "write_technical_markdown_doc", "trigger_pipeline_repair"]:
    chk("Function " + fn, fn in dir())

# 11. Swarm Graph
print("\n-- 11. Swarm Graph --")
chk("build_swarm_graph", "build_swarm_graph" in dir())
chk("execute_swarm", "execute_swarm" in dir())
chk("init_swarm_state", "init_swarm_state" in dir())

# 12. Production Tables
print("\n-- 12. Production Catalog Tables --")
try:
    pt = spark.sql("SELECT table_schema, table_name FROM " + PROD_CATALOG + ".information_schema.tables WHERE table_schema IN ('bronze','silver','gold') ORDER BY table_schema, table_name").collect()
    if pt:
        chk("Production tables in " + PROD_CATALOG + " (" + str(len(pt)) + " tables)", True)
        for t in pt:
            print("    " + str(t["table_schema"]) + "." + str(t["table_name"]))
    else:
        wrn("No bronze/silver/gold tables in " + PROD_CATALOG, "Run data pipeline first")
except Exception as e:
    wrn("Cannot query " + PROD_CATALOG, str(e)[:80])

print("\n" + "=" * 70)
print("VALIDATION SUMMARY: " + str(pass_count) + " passed, " + str(fail_count) + " failed, " + str(warn_count) + " warnings")
if fail_count == 0:
    print("\n  >>> Environment is READY for autonomous swarm operations. <<<")
else:
    print("\n  >>> " + str(fail_count) + " check(s) FAILED. Review before production use. <<<")
if warn_count > 0:
    print("  >>> " + str(warn_count) + " warning(s) -- non-blocking. <<<")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Production Entry Point — Job Parameter Handler
# ═══════════════════════════════════════════════════════════════
# Cell 24: Production Entry Point — Job Parameter Handler
# ═══════════════════════════════════════════════════════════════
# When run as a Lakeflow Job task, this cell reads the failed run_id
# from job parameters (passed as widgets) and triggers the autonomous
# swarm on a real pipeline failure.
# ─────────────────────────────────────────────────────────────────

# Define widgets for job parameters
import os

dbutils.widgets.text("run_id", "", "Failed Run ID")
dbutils.widgets.text("error_context", "", "Error Context (optional)")
dbutils.widgets.text("trigger_source", "pipeline_failure", "Trigger Source")

run_id = dbutils.widgets.get("run_id") or ""
error_context = dbutils.widgets.get("error_context") or ""
trigger_source = dbutils.widgets.get("trigger_source") or "pipeline_failure"

# Also check environment variables (for non-widget execution contexts)
if not run_id:
    run_id = os.environ.get("PC_SWARM_RUN_ID", "")
if not error_context:
    error_context = os.environ.get("PC_SWARM_ERROR_CONTEXT", "")

if run_id and run_id.strip():
    print("=" * 70)
    print("AUTONOMOUS SWARM — PRODUCTION TRIGGER")
    print("=" * 70)
    print("  Run ID: " + run_id)
    print("  Trigger: " + trigger_source)
    if error_context:
        print("  Error Context: " + error_context[:200])
    else:
        print("  Error Context: (will be fetched by Triage Agent)")
    print()

    # Initialize swarm state with the real run_id
    state = init_swarm_state(run_id=run_id, trigger_source=trigger_source)
    if error_context:
        state["error_log"] = error_context

    # Execute the autonomous swarm
    final_state = execute_swarm(state)

    # Print final results
    print()
    print("=" * 70)
    print("SWARM EXECUTION COMPLETE")
    print("=" * 70)
    status = final_state.get("final_status", "unknown")
    print("  Final Status: " + status)
    print("  Attempts: " + str(final_state.get("attempt_counter", 0)))
    print("  Token Cost: $" + str(round(final_state.get("token_cost_usd", 0), 2)))
    print("  Phase: " + str(final_state.get("phase", "unknown")))

    if final_state.get("human_escalation_reason"):
        print()
        print("  ESCALATION: " + final_state["human_escalation_reason"])
        session = final_state.get("session_id", "unknown")
        print("  Report: " + DOCS_VOLUME_PATH + "/escalations/" + session + ".md")

    if final_state.get("metadata_update_applied"):
        print("  Metadata Updated: YES")
    if final_state.get("validation_passed"):
        print("  QA Validation: PASSED")
    if final_state.get("uc_comments_updated"):
        print("  UC Comments: Updated")
    if final_state.get("git_pr_url"):
        print("  Git PR: " + final_state["git_pr_url"])

    print()
    print("Action Log:")
    for msg in final_state.get("messages", [])[-15:]:
        agent = msg.get("agent", "?")
        action = msg.get("action", "?")
        details = msg.get("details", "")[:100]
        print("  [" + agent + "] " + action + ": " + details)
else:
    print("No run_id provided via widget or environment.")
    print("This notebook is configured for autonomous pipeline failure response.")
    print()
    print("To trigger manually:")
    print("  state = init_swarm_state(run_id='<failed_run_id>')")
    print("  final_state = execute_swarm(state)")
    print()
    print("To trigger as a Lakeflow Job:")
    print("  Run this notebook with job parameter run_id=<failed_run_id>")
    print()
    print("The scenario simulations in the previous cells demonstrate")
    print("the expected swarm behavior for each failure type.")