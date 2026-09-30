# DEPRECATED (2026-09-30): Superseded by agents/Unified_Insurance_Agent. Merged into single ChatAgent (pc_insurance_agent).


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
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatContext
from databricks.sdk import WorkspaceClient
from databricks.agents import deploy
from mlflow.models.resources import DatabricksServingEndpoint
import os, tempfile

w = WorkspaceClient()
print(f"MLflow version: {mlflow.__version__}")
print("✓ ChatAgent and databricks.agents.deploy available")

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
from mlflow.models.resources import DatabricksServingEndpoint

class DataEngineerAgent(ChatAgent):
    """P&C Insurance Pipeline Code Generator Agent"""
    
    def __init__(self):
        super().__init__()
        self.system_prompt = DE_SYSTEM_PROMPT
    
    def predict(self, messages, context=None, custom_inputs=None):
        """Process pipeline code generation questions via ChatAgent protocol.
        
        Args:
            messages: List of ChatAgentMessage objects (chat history)
            context: Optional ChatContext with conversation metadata
            custom_inputs: Optional dict of custom inputs
            
        Returns:
            ChatAgentResponse with assistant message containing LLM response
        """
        import mlflow.deployments
        
        # Extract the latest user message from chat history
        user_messages = [m for m in messages if m.role == "user"]
        question = user_messages[-1].content if user_messages else ""
        
        # Call foundation model with system prompt
        deploy_client = mlflow.deployments.get_deploy_client("databricks")
        llm_response = deploy_client.predict(
            endpoint="databricks-gpt-oss-120b",
            inputs={
                "messages": [
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": question}
                ],
                "max_tokens": 2000,
                "temperature": 0.3
            }
        )
        content = llm_response["choices"][0]["message"]["content"]
        # gpt-oss-120b returns content as a list of structured objects (reasoning + text)
        if isinstance(content, list):
            text_parts = [c.get("text", "") for c in content if c.get("type") == "text"]
            answer = " ".join(text_parts) if text_parts else str(content)
        else:
            answer = content
        
        # Return ChatAgentResponse (NOT a plain dict) - enables streaming
        import uuid
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=answer, id=str(uuid.uuid4()))]
        )

# Log the agent to MLflow as ChatAgent
# IMPORTANT: Do NOT set explicit signature - ChatAgent auto-infers
# ChatAgentRequest/ChatAgentResponse schemas and sets task: agent/v2/chat
mlflow.set_experiment("/Users/vedavyas.goparaju@gmail.com/pc_insurance_agents")

with mlflow.start_run(run_name="data_engineer_agent_chatagent_v2") as run:
    mlflow.log_param("agent_type", "data_engineer")
    mlflow.log_param("domain", "pc_insurance")
    mlflow.log_param("role", "senior_data_engineer")
    mlflow.log_param("prompt_length", len(DE_SYSTEM_PROMPT))
    mlflow.log_param("model_type", "ChatAgent")
    mlflow.log_param("task", "agent/v2/chat")
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write(DE_SYSTEM_PROMPT)
        mlflow.log_artifact(f.name, artifact_path="system_prompt")
    
    # Log WITHOUT explicit signature - ChatAgent auto-infers
    agent = DataEngineerAgent()
    mlflow.pyfunc.log_model(
        artifact_path="data_engineer_agent",
        python_model=agent,
        registered_model_name="workspace.default.pc_data_engineer_agent",
        resources=[
            DatabricksServingEndpoint(endpoint_name="databricks-gpt-oss-120b")
        ],
    )
    
    print(f"Data Engineer Agent logged to MLflow: {run.info.run_id}")
    print(f"Registered Model: workspace.default.pc_data_engineer_agent")
    print(f"Model type: ChatAgent (task: agent/v2/chat)")
    os.unlink(f.name)

# COMMAND ----------

# DBTITLE 1,Deploy Data Engineer Agent as ChatAgent Endpoint
# ============================================
# Deploy Data Engineer Agent as ChatAgent Serving Endpoint
# ============================================

# databricks.agents.deploy() creates an agent-serving endpoint
# that supports streaming (unlike w.serving_endpoints.create which
# creates plain model endpoints without task field)

# Get the latest model version
import mlflow
client = mlflow.tracking.MlflowClient()
latest_versions = client.search_model_versions("name='workspace.default.pc_data_engineer_agent'")
latest_version = max(int(mv.version) for mv in latest_versions)
print(f"Deploying model version: {latest_version}")

endpoint_name = "pc_data_engineer_agent"

# Delete old plain endpoint first (it was created without task field)
try:
    w.serving_endpoints.delete(name=endpoint_name)
    print(f"Deleted old plain endpoint: {endpoint_name}")
except Exception:
    print(f"No existing endpoint to delete: {endpoint_name}")

# Deploy as ChatAgent endpoint using databricks.agents.deploy()
# This creates an endpoint with task: agent/v2/chat, enabling streaming
agent_endpoint_info = deploy(
    model_name="workspace.default.pc_data_engineer_agent",
    model_version=latest_version,
    endpoint_name=endpoint_name,
    scale_to_zero=True,
)

print(f"\n✓ Deployed ChatAgent endpoint: {endpoint_name}")
print(f"  Model: workspace.default.pc_data_engineer_agent v{latest_version}")
print(f"  Task: agent/v2/chat (streaming enabled)")
print(f"\n✓ Data Engineer Agent (ChatAgent) is ready for the Supervisor Agent")

# COMMAND ----------

# DBTITLE 1,Test Data Engineer Agent
# ============================================
# Test the Data Engineer Agent
# ============================================

# Test locally using ChatAgent predict
test_agent = DataEngineerAgent()
test_questions = [
    "Write the Silver layer MERGE for policy_dim with SCD2",
    "Create DQ expectations for the claims pipeline",
    "Generate the Gold layer SQL for loss ratio by quarter",
]

for q in test_questions:
    test_messages = [ChatAgentMessage(role="user", content=q)]
    result = test_agent.predict(test_messages)
    answer = result.messages[0].content if result.messages else "No response"
    print(f"\nQ: {q}")
    print(f"A: {answer[:200]}...")

print("\n✓ Data Engineer Agent (ChatAgent) is ready")