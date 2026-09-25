# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Analyst Genie Space Setup - P&C Gold Layer
# MAGIC %md
# MAGIC # Analyst Genie Space Setup - P&C Insurance Gold Layer
# MAGIC
# MAGIC This notebook prepares the Gold layer tables for a **Genie Space** that will serve as the **Analyst agent** in the multi-agent system.
# MAGIC
# MAGIC ## What This Does
# MAGIC 1. Adds rich table and column comments to Gold layer tables (Genie uses these for query routing)
# MAGIC 2. Creates a Genie Space programmatically (or provides manual setup instructions)
# MAGIC 3. The Genie Space enables natural-language queries like:
# MAGIC    - "What is our loss ratio by line of business for Q1?"
# MAGIC    - "Show me claim frequency and severity by state"
# MAGIC    - "Which agents have the best retention rates?"
# MAGIC
# MAGIC ## Genie Space as Analyst Agent
# MAGIC The Genie Space acts as the **Analyst** (Mid-level) agent in the multi-agent team, answering business questions directly from Gold layer data.

# COMMAND ----------

# DBTITLE 1,Setup
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

# COMMAND ----------

# DBTITLE 1,Add Rich Comments for Genie
# ============================================
# Add rich comments to Gold layer tables for Genie
# ============================================

# Table-level comments (Genie uses these to understand what data is available)
gold_table_descriptions = {
    "loss_ratio_by_lob": "P&C insurance loss ratio, expense ratio, and combined ratio by line of business and reporting period. Contains earned premium, incurred losses, expense amounts, and calculated ratios. Use for questions about profitability, loss ratios, combined ratios by LOB.",
    "claim_frequency_severity": "P&C insurance claim frequency and severity metrics by line of business, state, and reporting period. Contains exposure units (policy count), claim count, frequency rate, average severity, and incurred losses. Use for questions about how often claims occur and average claim costs.",
    "retention_by_agent": "Policy retention and growth metrics by agent, agency, and reporting period. Contains total policies, renewed, cancelled, new business counts, retention rate, and new business growth. Use for agent performance and retention questions.",
    "premium_growth": "Premium growth by line of business and reporting period. Contains new business, renewal, endorsement, and cancelled premium amounts. Use for questions about premium trends and growth.",
    "exposure_summary": "Exposure summary by line of business, state, and reporting period. Contains total policies, active/cancelled counts, coverage limits, average premium, and earned exposure. Use for exposure and policy volume questions.",
    "uw_dashboard_summary": "Executive underwriting dashboard summary combining loss ratios, combined ratios, policy counts, and claim statistics by line of business and reporting period. Use for high-level executive KPI questions.",
}

for table_name, description in gold_table_descriptions.items():
    spark.sql(f"""
        COMMENT ON TABLE pc_insurance.gold.{table_name} IS '{description.replace("'", "''")}'
    """)
    print(f"✓ Commented: pc_insurance.gold.{table_name}")

# Column-level comments for key KPI columns
# Wrap in try/except since not all columns may exist in all tables
column_comments = [
    ("pc_insurance.gold.loss_ratio_by_lob.loss_ratio", "Loss Ratio = Incurred Losses / Earned Premium. Values 0 to 5.0. Below 0.60 is good, above 0.70 is concerning."),
    ("pc_insurance.gold.claim_frequency_severity.claim_frequency", "Claim Frequency = Claim Count / Exposure Units. Measures how often claims occur per policy."),
    ("pc_insurance.gold.claim_frequency_severity.claim_severity", "Claim Severity = Incurred Losses / Claim Count. Average cost per claim."),
    ("pc_insurance.gold.retention_by_agent.retention_rate", "Retention Rate = Renewed Policies / (Renewed + Cancelled). Target: >90% personal, >85% commercial."),
]

for full_col, comment_text in column_comments:
    try:
        spark.sql(f"""
            COMMENT ON COLUMN {full_col} IS '{comment_text.replace("'", "''")}'
        """)
        print(f"  ✓ Commented: {full_col}")
    except Exception as e:
        print(f"  ⚠ Skipped: {full_col} - {str(e)[:100]}")

print("\n✓ All Gold layer table and column comments added for Genie")

# COMMAND ----------

# DBTITLE 1,Create Genie Space
# ============================================
# Create Genie Space via SDK
# ============================================
#
# Note: Genie Space creation may need to be done from the UI.
# The SDK method below is attempted, with manual instructions as fallback.

try:
    from databricks.sdk.service.catalog import GenieSpaceCreateRequest
    
    genie_space = w.genie.create_genie_space(
        name="PC_Insurance_Analyst",
        description="P&C Insurance Analyst - Answers business questions about loss ratios, combined ratios, claim frequency/severity, retention rates, and premium growth from Gold layer tables.",
    )
    
    genie_space_id = genie_space.id
    print(f"✓ Genie Space created: {genie_space_id}")
    
except Exception as e:
    print(f"Genie Space creation via SDK not available or failed: {e}")
    print("\n📋 Please create manually from the Databricks UI:")
    print("   1. Go to Genie → Create Space")
    print("   2. Name: PC_Insurance_Analyst")
    print("   3. Description: P&C Insurance Analyst for Gold layer KPIs")
    print("   4. Add tables from pc_insurance.gold schema:")
    print("      - pc_insurance.gold.loss_ratio_by_lob")
    print("      - pc_insurance.gold.claim_frequency_severity")
    print("      - pc_insurance.gold.retention_by_agent")
    print("      - pc_insurance.gold.premium_growth")
    print("      - pc_insurance.gold.exposure_summary")
    print("      - pc_insurance.gold.uw_dashboard_summary")
    print("   5. Note the Genie Space ID for the Supervisor Agent setup")
    genie_space_id = "<replace-with-your-genie-space-id>"

# COMMAND ----------

# DBTITLE 1,Add Example Queries to Genie Space
# ============================================
# Add example queries to Genie Space
# ============================================

# These examples help Genie understand common question patterns
example_queries = [
    ("What is our loss ratio by line of business?", 
     "SELECT line_of_business, reporting_period, loss_ratio, combined_ratio FROM pc_insurance.gold.loss_ratio_by_lob ORDER BY reporting_period DESC, line_of_business"),
    ("Show claim frequency and severity by state",
     "SELECT state, line_of_business, reporting_period, claim_frequency, claim_severity FROM pc_insurance.gold.claim_frequency_severity ORDER BY reporting_period DESC, state"),
    ("Which agents have the best retention rates?",
     "SELECT agent_name, agency_name, reporting_period, retention_rate, total_policies FROM pc_insurance.gold.retention_by_agent ORDER BY retention_rate DESC"),
    ("What is our premium growth trend?",
     "SELECT reporting_period, line_of_business, new_business_premium, renewal_premium, total_written_premium FROM pc_insurance.gold.premium_growth ORDER BY reporting_period DESC, line_of_business"),
    ("Give me the executive dashboard summary",
     "SELECT * FROM pc_insurance.gold.uw_dashboard_summary ORDER BY reporting_period DESC, line_of_business"),
]

print("Example queries for Genie Space (add via UI if SDK not available):")
for question, sql in example_queries:
    print(f"\n  Q: {question}")
    print(f"  SQL: {sql[:80]}...")

print(f"\n✓ Analyst Genie Space Setup Complete")
print(f"Genie Space ID: {genie_space_id}")
print("\nNext: Run the Supervisor_Agent_Setup notebook to register this as a subagent.")