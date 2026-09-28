# Databricks notebook source
# DBTITLE 1,Swarm Setup
# MAGIC %md
# MAGIC # PC Insurance Swarm Setup
# MAGIC
# MAGIC Provisions and validates the autonomous agent swarm infrastructure: UC catalog/schema, mapping metadata tables, threshold controls, technical docs volume, baseline seed data, and LLM endpoint readiness. Designed to run as a task in the `PC_Insurance_Agent_Setup` job.

# COMMAND ----------

# DBTITLE 1,Infrastructure Setup
# ═══════════════════════════════════════════════════════════════
# Swarm Infrastructure Setup — Catalog, Tables, Volume, Seeding
# Idempotent: safe to re-run. Creates if missing, validates if exists.
# ═══════════════════════════════════════════════════════════════

import json, datetime
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
results = {"checks": [], "passed": 0, "failed": 0}

def check(name, passed, detail=""):
    status = "PASS" if passed else "FAIL"
    results["checks"].append({"name": name, "status": status, "detail": detail})
    if passed:
        results["passed"] += 1
    else:
        results["failed"] += 1
    print(f"  [{status}] {name}: {detail}")

print("=" * 60)
print("Swarm Infrastructure Setup")
print("=" * 60)

# ── 1. Catalog + Schema ──
print("\n1. Catalog & Schema")
spark.sql("CREATE CATALOG IF NOT EXISTS pc_insurance_dev")
spark.sql("CREATE SCHEMA IF NOT EXISTS pc_insurance_dev.metadata")
check("pc_insurance_dev catalog", True, "created/verified")
check("pc_insurance_dev.metadata schema", True, "created/verified")

# ── 2. mapping_documents table ──
print("\n2. Metadata Tables")
spark.sql("""
    CREATE TABLE IF NOT EXISTS pc_insurance_dev.metadata.mapping_documents (
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
map_count = spark.sql("SELECT COUNT(*) AS cnt FROM pc_insurance_dev.metadata.mapping_documents").collect()[0]["cnt"]
check("mapping_documents table", True, f"created/verified ({map_count} rows)")

# ── 3. threshold_controls table ──
spark.sql("""
    CREATE TABLE IF NOT EXISTS pc_insurance_dev.metadata.threshold_controls (
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
thresh_count = spark.sql("SELECT COUNT(*) AS cnt FROM pc_insurance_dev.metadata.threshold_controls").collect()[0]["cnt"]
check("threshold_controls table", True, f"created/verified ({thresh_count} rows)")

# ── 4. UC Volume for technical docs ──
print("\n3. UC Volume")
spark.sql("CREATE VOLUME IF NOT EXISTS pc_insurance_dev.metadata.technical_docs")
for subdir in ["post_mortems", "schema_docs", "escalations"]:
    try:
        dbutils.fs.mkdirs(f"/Volumes/pc_insurance_dev/metadata/technical_docs/{subdir}")
    except Exception:
        pass
check("technical_docs volume + subdirs", True, "created/verified")

# ── 5. Seed baseline mappings if empty ──
print("\n4. Seed Baseline Data")
if map_count == 0:
    print("  Seeding baseline mapping documents...")
    # Minimal baseline — full mappings are in the main swarm notebook
    baseline = [
        ("PolicyCenter", "bronze", 1, True,
         json.dumps({"column_mappings": {"policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": False}}, "transformation_rules": []}),
         "PolicyCenter Bronze layer — raw ingestion of policy data",
         "swarm_setup", datetime.datetime.now().isoformat()),
        ("PolicyCenter", "silver", 1, True,
         json.dumps({"column_mappings": {"policy_id": {"target_col": "policy_id", "data_type": "string", "nullable": False}}, "transformation_rules": [{"rule_name": "deductible_not_null", "rule_type": "not_null", "rule_sql": "COALESCE(deductible_amount, 0)"}]}),
         "PolicyCenter Silver layer — cleansed and conformed policy data",
         "swarm_setup", datetime.datetime.now().isoformat()),
        ("ClaimCenter", "bronze", 1, True,
         json.dumps({"column_mappings": {"claim_id": {"target_col": "claim_id", "data_type": "string", "nullable": False}}, "transformation_rules": []}),
         "ClaimCenter Bronze layer — raw ingestion of claims data",
         "swarm_setup", datetime.datetime.now().isoformat()),
        ("ClaimCenter", "gold", 1, True,
         json.dumps({"column_mappings": {"loss_ratio": {"target_col": "loss_ratio", "data_type": "decimal(10,4)", "transformation": "SUM(loss_amount) / NULLIF(SUM(premium_amount), 0) * 100"}}, "transformation_rules": [{"rule_name": "loss_ratio_threshold_check", "rule_type": "conditional", "rule_sql": "loss_ratio <= 200"}]}),
         "ClaimCenter Gold layer — quarterly KPIs by LOB",
         "swarm_setup", datetime.datetime.now().isoformat()),
        ("BillingCenter", "bronze", 1, True,
         json.dumps({"column_mappings": {"billing_id": {"target_col": "billing_id", "data_type": "string", "nullable": False}}, "transformation_rules": []}),
         "BillingCenter Bronze layer — raw ingestion of billing data",
         "swarm_setup", datetime.datetime.now().isoformat()),
        ("MGA_Feed", "bronze", 1, True,
         json.dumps({"column_mappings": {"mga_policy_id": {"target_col": "mga_policy_id", "data_type": "string", "nullable": False}}, "transformation_rules": []}),
         "MGA Feed Bronze layer — raw ingestion of MGA policy data",
         "swarm_setup", datetime.datetime.now().isoformat()),
        ("MGA_Feed", "silver", 1, True,
         json.dumps({"column_mappings": {"mga_policy_id": {"target_col": "mga_policy_id", "data_type": "string", "nullable": False}}, "transformation_rules": [{"rule_name": "deductible_not_null", "rule_type": "not_null", "rule_sql": "COALESCE(deductible_amount, 0)"}]}),
         "MGA Feed Silver layer — cleansed MGA policy data",
         "swarm_setup", datetime.datetime.now().isoformat()),
    ]
    for x_center, layer, version, is_active, mapping_json, desc, updated_by, updated_at in baseline:
        spark.sql(f"""
            INSERT INTO pc_insurance_dev.metadata.mapping_documents
            (x_center, layer, version, is_active, mapping_json, business_description, updated_by, updated_at)
            VALUES ('{x_center}', '{layer}', {version}, {str(is_active).lower()},
                    '{mapping_json.replace(chr(39), chr(39)+chr(39))}',
                    '{desc.replace(chr(39), chr(39)+chr(39))}',
                    '{updated_by}', '{updated_at}')
        """)
    check("Baseline mappings seeded", True, f"{len(baseline)} mapping documents inserted")
else:
    check("Baseline mappings already present", True, f"{map_count} rows found — skipping seed")

if thresh_count == 0:
    print("  Seeding baseline threshold controls...")
    thresholds = [
        ("loss_ratio", None, None, 200.0, 0.0, datetime.date(2024, 1, 1), None, True, "swarm_setup"),
        ("claim_frequency", None, None, None, 0.0, datetime.date(2024, 1, 1), None, True, "swarm_setup"),
        ("premium_growth", None, None, 500.0, -100.0, datetime.date(2024, 1, 1), None, True, "swarm_setup"),
        ("retention_rate", None, None, 100.0, 50.0, datetime.date(2024, 1, 1), None, True, "swarm_setup"),
    ]
    for metric, region, event_type, max_t, min_t, start_d, end_d, active, updated_by in thresholds:
        max_sql = str(max_t) if max_t is not None else "NULL"
        min_sql = str(min_t) if min_t is not None else "NULL"
        end_sql = f"'{end_d}'" if end_d else "NULL"
        reg_sql = f"'{region}'" if region else "NULL"
        evt_sql = f"'{event_type}'" if event_type else "NULL"
        spark.sql(f"""
            INSERT INTO pc_insurance_dev.metadata.threshold_controls
            (metric_name, region, event_type, max_threshold, min_threshold, effective_start_date, effective_end_date, is_active, updated_by, updated_at)
            VALUES ('{metric}', {reg_sql}, {evt_sql}, {max_sql}, {min_sql}, '{start_d}', {end_sql}, {str(active).lower()}, '{updated_by}', current_timestamp())
        """)
    check("Baseline thresholds seeded", True, f"{len(thresholds)} threshold controls inserted")
else:
    check("Baseline thresholds already present", True, f"{thresh_count} rows found — skipping seed")

# COMMAND ----------

# DBTITLE 1,LLM Validation & Summary
# ═══════════════════════════════════════════════════════════════
# LLM Endpoint Validation + Summary Report
# ═══════════════════════════════════════════════════════════════

print("\n5. LLM Endpoint Validation")

SUPERVISOR_LLM = "databricks-meta-llama-3-3-70b-instruct"
SUBAGENT_LLM = "databricks-meta-llama-3-1-8b-instruct"

for ep_name, label in [(SUPERVISOR_LLM, "Supervisor LLM"), (SUBAGENT_LLM, "Sub-agent LLM")]:
    try:
        ep = w.serving_endpoints.get(ep_name)
        ready = "READY" in str(ep.state.ready) if ep.state else False
        check(f"{label} ({ep_name})", ready, f"ready={ep.state.ready if ep.state else 'unknown'}")
    except Exception as e:
        check(f"{label} ({ep_name})", False, str(e)[:100])

# ── 6. Validate swarm notebook exists ──
print("\n6. Swarm Notebook Validation")
SWARM_NOTEBOOK_PATH = "/Users/vedavyas.goparaju@gmail.com/PC_Insurance_Autonomous_Agent_Swarm"
try:
    status = w.workspace.get_status(SWARM_NOTEBOOK_PATH)
    exists = status is not None
    check("Swarm notebook exists", exists, SWARM_NOTEBOOK_PATH)
except Exception as e:
    # Try with .py extension on filesystem
    import os
    fs_path = "/Workspace" + SWARM_NOTEBOOK_PATH + ".py"
    exists = os.path.exists(fs_path)
    check("Swarm notebook exists", exists, f"{SWARM_NOTEBOOK_PATH} (fs: {fs_path})")

# ── 7. Validate swarm standalone job exists ──
print("\n7. Swarm Job Validation")
SWARM_JOB_ID = 774564996988013
try:
    job = w.jobs.get(SWARM_JOB_ID)
    check("Swarm standalone job", True, f"ID={SWARM_JOB_ID}, name={job.settings.name}")
except Exception as e:
    check("Swarm standalone job", False, f"ID={SWARM_JOB_ID}: {str(e)[:100]}")

# ── Summary ──
print("\n" + "=" * 60)
print(f"SWARM SETUP SUMMARY: {results['passed']} passed, {results['failed']} failed")
print("=" * 60)
if results["failed"] > 0:
    print("\nFAILED CHECKS:")
    for c in results["checks"]:
        if c["status"] == "FAIL":
            print(f"  ✗ {c['name']}: {c['detail']}")
    raise RuntimeError(f"Swarm setup completed with {results['failed']} failure(s)")
else:
    print("\n✓ All swarm infrastructure checks passed. Swarm is ready.")