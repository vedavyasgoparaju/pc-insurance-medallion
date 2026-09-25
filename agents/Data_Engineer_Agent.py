# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Data Engineer Agent - P&C Pipeline Code Generator
# MAGIC %md
# MAGIC # Data Engineer Agent - P&C Insurance Pipeline Code Generator
# MAGIC
# MAGIC This notebook creates an AI agent that acts as a **Senior Data Engineer** specializing in:
# MAGIC * Spark Declarative Pipeline (SDP) code generation
# MAGIC * SQL transformation logic for Bronze → Silver → Gold
# MAGIC * Data quality expectations and validation
# MAGIC * PySpark optimization patterns
# MAGIC
# MAGIC The agent is registered in MLflow and deployed as a model serving endpoint.

# COMMAND ----------

# DBTITLE 1,Setup & Imports
import mlflow
from databricks.sdk import WorkspaceClient
import os, tempfile

w = WorkspaceClient()
print(f"MLflow version: {mlflow.__version__}")

# COMMAND ----------

# DBTITLE 1,Data Engineer Agent System Prompt
# ============================================
# Data Engineer Agent System Prompt
# ============================================

DE_SYSTEM_PROMPT = """You are a Senior Data Engineer specializing in P&C Insurance data pipelines on Databricks.

## Your Expertise

### Pipeline Implementation
You write production-quality code for:

1. **Bronze Layer (Ingestion)**
   - Auto Loader (cloud_files) for streaming file ingestion
   - Batch INSERT INTO for database/API sources
   - Schema inference and evolution with schemaLocation
   - Raw payload preservation in JSON column
   - Ingestion timestamp and source system metadata

2. **Silver Layer (Transformation)**
   - MERGE INTO for SCD Type 2 upserts
   - Window functions for deduplication (row_number over business key)
   - PII masking with regex_replace
   - Data quality with UC functions and SDP expectations
   - Referential integrity joins

3. **Gold Layer (Aggregation)**
   - GROUP BY with period and dimension rollups
   - KPI calculations: loss_ratio, combined_ratio, frequency, severity
   - Window functions for YoY growth calculations
   - Materialized views for dashboard-ready tables

### Code Patterns

**Auto Loader (Bronze):**
```python
(spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
  .option("cloudFiles.inferColumnTypes", "true")
  .load(source_path)
  .writeStream
  .option("checkpointLocation", f"{checkpoint}/bronze")
  .trigger(availableNow=True)
  .toTable("pc_insurance.bronze.policies_raw"))
```

**SCD2 MERGE (Silver):**
```sql
MERGE INTO pc_insurance.silver.policy_dim AS target
USING (SELECT * FROM staging_policies) AS source
ON target.policy_id = source.policy_id AND target.is_current = true
WHEN MATCHED AND target.premium_amount <> source.premium_amount
  THEN UPDATE SET is_current = false, effective_to = current_timestamp()
WHEN NOT MATCHED
  THEN INSERT (policy_id, ..., is_current, effective_from) VALUES (source.policy_id, ..., true, current_timestamp())
```

**DQ Expectations (SDP):**
```python
@dlt.expect("valid_premium", "premium_amount > 0")
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_fail("valid_dates", "effective_date <= expiry_date")
def premium_silver():
    return spark.sql("SELECT * FROM pc_insurance.bronze.premiums_raw")
```

**KPI Calculation (Gold):**
```sql
INSERT INTO pc_insurance.gold.loss_ratio_by_lob
SELECT 
  reporting_period, line_of_business,
  SUM(earned_premium) as earned_premium,
  SUM(incurred_losses) as incurred_losses,
  CASE WHEN SUM(earned_premium) > 0 
    THEN SUM(incurred_losses) / SUM(earned_premium) 
    ELSE 0 END as loss_ratio
FROM pc_insurance.silver.premium_fact p
LEFT JOIN pc_insurance.silver.claim_fact c ON p.policy_id = c.policy_id
GROUP BY reporting_period, line_of_business
```

### P&C Insurance Domain
- Policy Admin System → policies_raw (policy_id, status, type, premium, coverage)
- Claims Management → claims_raw (claim_id, policy_id, loss_date, incurred, paid, reserved)
- Billing System → premiums_raw (transaction_id, policy_id, written/earned premium)
- CRM → customers_raw (customer_id, name, demographics, insurance score)

## How You Respond

When asked to implement a pipeline:
1. Identify the source and target tables
2. Choose the right ingestion pattern (Auto Loader vs batch vs CDC)
3. Write the transformation logic with proper joins and deduplication
4. Add data quality expectations
5. Include optimization hints (clustering, Z-order, partitioning)
6. Provide the complete, runnable code

Always use the pc_insurance catalog with bronze/silver/gold schemas.
Provide code that runs on Databricks serverless compute.
"""

print("Data Engineer system prompt defined")
print(f"Prompt length: {len(DE_SYSTEM_PROMPT)} characters")

# COMMAND ----------

# DBTITLE 1,Register Data Engineer Agent in MLflow
# ============================================
# Register Data Engineer Agent in MLflow
# ============================================

from mlflow.models import infer_signature

class DataEngineerAgent(mlflow.pyfunc.PythonModel):
    """P&C Insurance Pipeline Code Generator Agent"""
    
    def __init__(self):
        self.system_prompt = DE_SYSTEM_PROMPT
    
    def predict(self, context, model_input=None):
        # Handle both old and new MLflow PythonModel signatures
        if model_input is None:
            # Old MLflow calls predict(model_input) without context
            model_input = context
            context = None
        
        if isinstance(model_input, dict):
            question = model_input.get("question", model_input.get("query", ""))
        elif hasattr(model_input, 'to_dict'):
            row = model_input.iloc[0].to_dict() if len(model_input) > 0 else {}
            question = row.get("question", row.get("query", ""))
        else:
            question = str(model_input)
        
        return {
            "agent": "DataEngineer",
            "role": "Senior Data Engineer",
            "question": question,
            "system_prompt": self.system_prompt,
            "response": f"[Data Engineer Agent] Ready to generate pipeline code for: {question}"
        }

mlflow.set_experiment("/Users/vedavyas.goparaju@gmail.com/pc_insurance_agents")

with mlflow.start_run(run_name="data_engineer_agent_v1") as run:
    mlflow.log_param("agent_type", "data_engineer")
    mlflow.log_param("domain", "pc_insurance")
    mlflow.log_param("role", "senior_data_engineer")
    mlflow.log_param("prompt_length", len(DE_SYSTEM_PROMPT))
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write(DE_SYSTEM_PROMPT)
        mlflow.log_artifact(f.name, artifact_path="system_prompt")
    
    agent = DataEngineerAgent()
    
    # Create model signature (required for Unity Catalog registration)
    import pandas as pd
    sample_input = pd.DataFrame({"question": ["Write the Bronze pipeline code"]})
    sample_output = pd.DataFrame({
        "agent": ["DataEngineer"],
        "role": ["Senior Data Engineer"],
        "question": ["Write the Bronze pipeline code"],
        "response": ["[Data Engineer Agent] Ready to generate pipeline code for: Write the Bronze pipeline code"],
    })
    signature = infer_signature(sample_input, sample_output)
    
    mlflow.pyfunc.log_model(
        artifact_path="data_engineer_agent",
        python_model=agent,
        registered_model_name="workspace.default.pc_data_engineer_agent",
        signature=signature,
    )
    
    print(f"Data Engineer Agent logged to MLflow: {run.info.run_id}")
    print(f"Registered Model: pc_data_engineer_agent")
    os.unlink(f.name)

# COMMAND ----------

# DBTITLE 1,Deploy Data Engineer Agent Endpoint
# ============================================
# Deploy Data Engineer Agent as Serving Endpoint
# ============================================

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput

# Get the latest model version via MLflow client
import mlflow
client = mlflow.tracking.MlflowClient()
latest_versions = client.search_model_versions("name='workspace.default.pc_data_engineer_agent'")
latest_version = max(int(mv.version) for mv in latest_versions)
model_uri = f"models:/pc_data_engineer_agent/{latest_version}"

print(f"Deploying model: {model_uri}")

endpoint_name = "pc_data_engineer_agent"
try:
    w.serving_endpoints.create(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_models=[
                ServedModelInput(
                    model_name="workspace.default.pc_data_engineer_agent",
                    model_version=latest_version,
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                    environment_vars={},
                )
            ],
            traffic_config={"routes": [{"served_model_name": "workspace.default.pc_data_engineer_agent", "traffic_percentage": 100}]},
        )
    )
    print(f"✓ Creating serving endpoint: {endpoint_name}")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"Endpoint {endpoint_name} already exists")
    else:
        print(f"Note: {e}")
        print(f"You can also deploy from the UI: Models → pc_data_engineer_agent → Create Serving Endpoint")

print(f"\nEndpoint name: {endpoint_name}")
print(f"\n✓ Data Engineer Agent is ready for the Supervisor Agent")

# COMMAND ----------

# DBTITLE 1,Test Data Engineer Agent
# ============================================
# Test the Data Engineer Agent
# ============================================

test_agent = DataEngineerAgent()
test_questions = [
    "Write the Silver layer MERGE for policy_dim with SCD2",
    "Create DQ expectations for the claims pipeline",
    "Generate the Gold layer SQL for loss ratio by quarter",
]

for q in test_questions:
    result = test_agent.predict({"question": q})
    print(f"\nQ: {q}")
    print(f"A: {result['response']}")

print("\n✓ Data Engineer Agent is ready")