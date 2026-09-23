# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,P&C Multi-Agent Orchestrator
# MAGIC %md
# MAGIC # P&C Insurance Multi-Agent System - Orchestrator
# MAGIC
# MAGIC This is the **master orchestrator notebook** that ties together the entire multi-agent system for designing and developing a Medallion architecture for P&C Insurance.
# MAGIC
# MAGIC ## Execution Order
# MAGIC
# MAGIC ```
# MAGIC 1. Bronze_Pipeline     → Ingest raw P&C data
# MAGIC 2. Silver_Pipeline     → Transform to conformed dimensions & facts
# MAGIC 3. Gold_Pipeline       → Build KPI aggregations
# MAGIC 4. Architect_Agent     → Deploy Architect AI agent endpoint
# MAGIC 5. Data_Engineer_Agent → Deploy Data Engineer AI agent endpoint
# MAGIC 6. Domain_Expert_Setup → Create Knowledge Assistant with domain docs
# MAGIC 7. Analyst_Genie_Setup → Create Genie Space over Gold tables
# MAGIC 8. Supervisor_Agent_Setup → Create Supervisor Agent with all subagents
# MAGIC ```
# MAGIC
# MAGIC ## Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │                  USER QUESTION                       │
# MAGIC │         ("Design the Bronze layer for claims")       │
# MAGIC └─────────────────────┬───────────────────────────────┘
# MAGIC                       ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │              SUPERVISOR AGENT                        │
# MAGIC │     (P&C Insurance Medallion Architecture Team)      │
# MAGIC │     Routes to the right specialist(s)                │
# MAGIC └──┬──────┬──────┬──────┬──────┬──────┬──────────────┘
# MAGIC    │      │      │      │      │      │
# MAGIC    ▼      ▼      ▼      ▼      ▼      ▼
# MAGIC ┌─────┐┌─────┐┌─────┐┌─────┐┌─────┐┌─────┐
# MAGIC │ARCH ││ DE  ││DOM  ││ANAL ││ QA  ││INT  │
# MAGIC │     ││     ││EXPT ││     ││     ││     │
# MAGIC └─────┘└─────┘└─────┘└─────┘└─────┘└─────┘
# MAGIC    │      │      │      │      │      │
# MAGIC    ▼      ▼      ▼      ▼      ▼      ▼
# MAGIC ┌─────────────────────────────────────────────────────┐
# MAGIC │              BRONZE → SILVER → GOLD                  │
# MAGIC │           (Medallion Data Architecture)              │
# MAGIC │                                                      │
# MAGIC │  Bronze: policies_raw, claims_raw, premiums_raw,    │
# MAGIC │          customers_raw, agents_raw                   │
# MAGIC │  Silver: policy_dim, claim_dim, customer_dim,       │
# MAGIC │          agent_dim, date_dim, premium_fact,         │
# MAGIC │          claim_fact, loss_fact                       │
# MAGIC │  Gold:   loss_ratio_by_lob, claim_freq_severity,    │
# MAGIC │          retention_by_agent, premium_growth,         │
# MAGIC │          exposure_summary, uw_dashboard_summary      │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Step 1: Bronze Pipeline
# ============================================
# STEP 1: Run Bronze Pipeline (Data Ingestion)
# ============================================
# This populates Bronze layer with sample P&C insurance data
# Run the Bronze_Pipeline notebook to generate and ingest:
#   - 1000 sample policies
#   - 300 sample claims
#   - 1200 premium transactions

print("=" * 60)
print("STEP 1: Bronze Pipeline - Data Ingestion")
print("=" * 60)

# Check if Bronze tables have data
bronze_counts = spark.sql("""
    SELECT 'policies_raw' AS tbl, COUNT(*) AS cnt FROM pc_insurance.bronze.policies_raw
    UNION ALL
    SELECT 'claims_raw', COUNT(*) FROM pc_insurance.bronze.claims_raw
    UNION ALL
    SELECT 'premiums_raw', COUNT(*) FROM pc_insurance.bronze.premiums_raw
""")
bronze_counts.show()

# If no data, instruct to run Bronze_Pipeline notebook
if bronze_counts.filter(F.col("cnt") > 0).count() == 0:
    print("⚠ Bronze tables are empty. Run the Bronze_Pipeline notebook first.")
    # For convenience, run it inline
    print("Running Bronze_Pipeline inline...")
    dbutils.notebook.run("/Users/vedavyas.goparaju@gmail.com/Bronze_Pipeline", 600)
else:
    print("✓ Bronze layer has data")

# COMMAND ----------

# DBTITLE 1,Step 2: Silver Pipeline
# ============================================
# STEP 2: Run Silver Pipeline (Transformation)
# ============================================
print("=" * 60)
print("STEP 2: Silver Pipeline - Transformation")
print("=" * 60)

# Check if Silver tables have data
silver_counts = spark.sql("""
    SELECT 'policy_dim' AS tbl, COUNT(*) AS cnt FROM pc_insurance.silver.policy_dim
    UNION ALL SELECT 'claim_dim', COUNT(*) FROM pc_insurance.silver.claim_dim
    UNION ALL SELECT 'premium_fact', COUNT(*) FROM pc_insurance.silver.premium_fact
    UNION ALL SELECT 'claim_fact', COUNT(*) FROM pc_insurance.silver.claim_fact
""")
silver_counts.show()

if silver_counts.filter(F.col("cnt") > 0).count() == 0:
    print("⚠ Silver tables are empty. Running Silver_Pipeline inline...")
    dbutils.notebook.run("/Users/vedavyas.goparaju@gmail.com/Silver_Pipeline", 600)
else:
    print("✓ Silver layer has data")

# COMMAND ----------

# DBTITLE 1,Step 3: Gold Pipeline
# ============================================
# STEP 3: Run Gold Pipeline (KPI Aggregation)
# ============================================
print("=" * 60)
print("STEP 3: Gold Pipeline - KPI Aggregation")
print("=" * 60)

gold_counts = spark.sql("""
    SELECT 'loss_ratio_by_lob' AS tbl, COUNT(*) AS cnt FROM pc_insurance.gold.loss_ratio_by_lob
    UNION ALL SELECT 'claim_frequency_severity', COUNT(*) FROM pc_insurance.gold.claim_frequency_severity
    UNION ALL SELECT 'retention_by_agent', COUNT(*) FROM pc_insurance.gold.retention_by_agent
    UNION ALL SELECT 'uw_dashboard_summary', COUNT(*) FROM pc_insurance.gold.uw_dashboard_summary
""")
gold_counts.show()

if gold_counts.filter(F.col("cnt") > 0).count() == 0:
    print("⚠ Gold tables are empty. Running Gold_Pipeline inline...")
    dbutils.notebook.run("/Users/vedavyas.goparaju@gmail.com/Gold_Pipeline", 600)
else:
    print("✓ Gold layer has data")

# Show sample KPIs
print("\n--- Sample Loss Ratio by LOB ---")
spark.sql("SELECT * FROM pc_insurance.gold.loss_ratio_by_lob ORDER BY reporting_period DESC LIMIT 10").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Steps 4-7: Deploy AI Agents
# ============================================
# STEP 4-7: Deploy AI Agents
# ============================================
print("=" * 60)
print("STEPS 4-7: Deploy AI Agents")
print("=" * 60)

# These notebooks create and deploy the AI agents:
#   4. Architect_Agent → MLflow model + serving endpoint
#   5. Data_Engineer_Agent → MLflow model + serving endpoint
#   6. Domain_Expert_Setup → Knowledge Assistant + UC volume docs
#   7. Analyst_Genie_Setup → Genie Space over Gold tables
#
# Run each notebook:

agent_notebooks = [
    ("/Users/vedavyas.goparaju@gmail.com/Architect_Agent", "Architect Agent"),
    ("/Users/vedavyas.goparaju@gmail.com/Data_Engineer_Agent", "Data Engineer Agent"),
    ("/Users/vedavyas.goparaju@gmail.com/Domain_Expert_Setup", "Domain Expert Setup"),
    ("/Users/vedavyas.goparaju@gmail.com/Analyst_Genie_Setup", "Analyst Genie Setup"),
]

print("\nTo deploy agents, run these notebooks:")
for path, name in agent_notebooks:
    print(f"  - {name}: {path}")

print("\nOr run them programmatically:")
for path, name in agent_notebooks:
    print(f"\n  Running {name}...")
    try:
        dbutils.notebook.run(path, 1200)
        print(f"  ✓ {name} completed")
    except Exception as e:
        print(f"  ⚠ {name}: {e}")
        print(f"    Run manually from the notebook: {path}")

# COMMAND ----------

# DBTITLE 1,Step 8: Supervisor Agent
# ============================================
# STEP 8: Create Supervisor Agent
# ============================================
print("=" * 60)
print("STEP 8: Supervisor Agent Setup")
print("=" * 60)

print("\nRunning Supervisor_Agent_Setup notebook...")
print("This creates the Supervisor Agent and registers all 6 subagents.")

try:
    dbutils.notebook.run("/Users/vedavyas.goparaju@gmail.com/Supervisor_Agent_Setup", 600)
    print("✓ Supervisor Agent created!")
except Exception as e:
    print(f"⚠ Supervisor Agent setup: {e}")
    print("  Run manually: /Users/vedavyas.goparaju@gmail.com/Supervisor_Agent_Setup")
    print("  Make sure to update the IDs (Knowledge Assistant, Genie Space) first!")

# COMMAND ----------

# DBTITLE 1,Demo: Query the Multi-Agent System
# ============================================
# DEMO: Query the Multi-Agent System
# ============================================
print("=" * 60)
print("DEMO: Querying the Multi-Agent System")
print("=" * 60)

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# List all supervisor agents
print("\nAvailable Supervisor Agents:")
try:
    for sa in w.supervisor_agents.list_supervisor_agents():
        print(f"  - {sa.display_name} ({sa.name})")
        print(f"    Description: {sa.description[:100]}...")
        
        # List tools for this agent
        print(f"    Subagents:")
        for tool in w.supervisor_agents.list_tools(parent=sa.name):
            print(f"      [{tool.tool_type}] {tool.name.split('/')[-1]}")
except Exception as e:
    print(f"  Listing agents: {e}")

print("\n" + "=" * 60)
print("HOW TO QUERY THE SUPERVISOR AGENT")
print("=" * 60)
print("""
Option 1: AI Playground (UI)
  - Go to Agents in the left navigation
  - Find 'P&C Insurance Medallion Architecture Team'
  - Start a conversation with your question

Option 2: Python SDK
  from databricks.sdk import WorkspaceClient
  w = WorkspaceClient()
  response = w.supervisor_agents.query(
      name="supervisor-agents/<your-agent-id>",
      messages=[{"role": "user", "content": "Design the Bronze layer for claims data"}],
  )

Option 3: REST API
  curl -X POST '{workspace}/api/2.0/serving-endpoints/supervisor-agents/<id>/invocations' \\
    -H "Authorization: Bearer $TOKEN" \\
    -H "Content-Type: application/json" \\
    -d '{"messages": [{"role": "user", "content": "What is our loss ratio by LOB?"}]}'

Example Questions to Try:
  1. "Design the Bronze layer for ingesting P&C policy data"
  2. "Write the Silver layer MERGE for policy_dim with SCD2"
  3. "What is a loss ratio and how is it calculated?"
  4. "What is our loss ratio by line of business for the latest quarter?"
  5. "Validate data quality on the claims table"
  6. "Design and implement the complete Gold layer for P&C KPIs"
""")

# COMMAND ----------

# DBTITLE 1,Final Verification - Data Architecture Summary
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- FINAL VERIFICATION: Complete Data Architecture Summary
# MAGIC -- ============================================
# MAGIC
# MAGIC -- Bronze Layer
# MAGIC SELECT 'BRONZE LAYER' AS layer, table_name, record_count FROM (
# MAGIC   SELECT 'policies_raw' AS table_name, COUNT(*) AS record_count FROM pc_insurance.bronze.policies_raw
# MAGIC   UNION ALL SELECT 'claims_raw', COUNT(*) FROM pc_insurance.bronze.claims_raw
# MAGIC   UNION ALL SELECT 'premiums_raw', COUNT(*) FROM pc_insurance.bronze.premiums_raw
# MAGIC )
# MAGIC UNION ALL
# MAGIC -- Silver Layer
# MAGIC SELECT 'SILVER LAYER', table_name, record_count FROM (
# MAGIC   SELECT 'policy_dim' AS table_name, COUNT(*) AS record_count FROM pc_insurance.silver.policy_dim
# MAGIC   UNION ALL SELECT 'claim_dim', COUNT(*) FROM pc_insurance.silver.claim_dim
# MAGIC   UNION ALL SELECT 'customer_dim', COUNT(*) FROM pc_insurance.silver.customer_dim
# MAGIC   UNION ALL SELECT 'agent_dim', COUNT(*) FROM pc_insurance.silver.agent_dim
# MAGIC   UNION ALL SELECT 'date_dim', COUNT(*) FROM pc_insurance.silver.date_dim
# MAGIC   UNION ALL SELECT 'premium_fact', COUNT(*) FROM pc_insurance.silver.premium_fact
# MAGIC   UNION ALL SELECT 'claim_fact', COUNT(*) FROM pc_insurance.silver.claim_fact
# MAGIC )
# MAGIC UNION ALL
# MAGIC -- Gold Layer
# MAGIC SELECT 'GOLD LAYER', table_name, record_count FROM (
# MAGIC   SELECT 'loss_ratio_by_lob' AS table_name, COUNT(*) AS record_count FROM pc_insurance.gold.loss_ratio_by_lob
# MAGIC   UNION ALL SELECT 'claim_frequency_severity', COUNT(*) FROM pc_insurance.gold.claim_frequency_severity
# MAGIC   UNION ALL SELECT 'retention_by_agent', COUNT(*) FROM pc_insurance.gold.retention_by_agent
# MAGIC   UNION ALL SELECT 'premium_growth', COUNT(*) FROM pc_insurance.gold.premium_growth
# MAGIC   UNION ALL SELECT 'exposure_summary', COUNT(*) FROM pc_insurance.gold.exposure_summary
# MAGIC   UNION ALL SELECT 'uw_dashboard_summary', COUNT(*) FROM pc_insurance.gold.uw_dashboard_summary
# MAGIC )
# MAGIC ORDER BY layer, table_name;