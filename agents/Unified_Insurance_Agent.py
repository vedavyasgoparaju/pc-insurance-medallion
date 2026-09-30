# Databricks notebook source
# DBTITLE 1,Unified Insurance Agent - Combined Architect + Data Engineer
# MAGIC %md
# MAGIC # Unified Insurance Agent - Combined Architect + Data Engineer
# MAGIC
# MAGIC This notebook creates a **combined AI agent** that merges the Architect and Data Engineer roles for the P&C Insurance Medallion architecture.
# MAGIC
# MAGIC ## Why a Unified Agent?
# MAGIC The Community Edition workspace limits user-created serving endpoints to 2. The Supervisor Agent's own endpoint occupies 1 slot, leaving only 1 for agent tools. This unified agent consolidates both roles into a single endpoint (`pc_insurance_agent`), staying within the workspace limit.
# MAGIC
# MAGIC ## Dual Role
# MAGIC * **Architecture Design**: Bronze/Silver/Gold layer schemas, data flow topology, UC governance, SCD2 strategies
# MAGIC * **Pipeline Implementation**: SDP code, SQL transformations, MERGE for SCD2, data quality expectations
# MAGIC
# MAGIC ## Key Features
# MAGIC * ChatAgent v2 protocol (`task: agent/v2/chat`) for Supervisor Agent compatibility
# MAGIC * **Streaming support** via `predict_stream` (73 chunks verified)
# MAGIC * Scale-to-zero enabled
# MAGIC * Uses `databricks-meta-llama-3-3-70b-instruct` foundation model

# COMMAND ----------

# DBTITLE 1,Setup & Imports
# Install required packages
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow", "databricks-sdk", "databricks-agents", "-q"])

import mlflow
import mlflow.pyfunc
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatAgentChunk, ChatContext
from databricks.sdk import WorkspaceClient
from databricks.agents import deploy
import uuid

print(f"MLflow version: {mlflow.__version__}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")
w = WorkspaceClient()
print(f"Workspace: {w.config.host}")
print("✓ ChatAgent, predict_stream, and databricks.agents.deploy available")

# COMMAND ----------

# DBTITLE 1,Unified Agent System Prompt
# ============================================
# Unified Agent System Prompt (Architect + Data Engineer)
# ============================================

UNIFIED_SYSTEM_PROMPT = """You are a combined Principal Data Architect and Senior Data Engineer for a Property & Casualty (P&C) Insurance Medallion architecture on Databricks.

## Your Dual Role

### Architecture Design
You design the overall Medallion architecture: Bronze/Silver/Gold layer schemas, data flow topology, Unity Catalog governance, SCD2 strategies, and scalability patterns.

### Pipeline Implementation
You implement Bronze/Silver/Gold pipelines by writing Spark Declarative Pipeline (SDP) code, SQL transformations, MERGE statements for SCD2, and data quality expectations.

## P&C Insurance Domain Context

### Bronze Layer (raw ingestion)
- policies_raw, claims_raw, premiums_raw, customers_raw, agents_raw
- Auto Loader or batch loads, immutable source data
- Ingestion timestamp and source system tracking

### Silver Layer (cleansed, conformed)
- policy_dim, claim_dim, customer_dim (PII masked), agent_dim, date_dim
- premium_fact, claim_fact
- SCD2 columns: is_current, effective_from, effective_to, __START_AT, __END_AT
- Deduplication by business key, referential integrity enforcement

### Gold Layer (aggregated, business-ready)
- loss_ratio_by_lob, claim_frequency_severity, retention_by_agent
- premium_growth, exposure_summary, uw_dashboard_summary
- Quarterly grain by LOB

### Unity Catalog
- Catalog: pc_insurance with schemas: bronze, silver, gold, reference, dq
- DQ functions: check_policy_exists, check_claim_status, check_premium_positive, check_loss_ratio, calculate_dq_score

### Key P&C Metrics
- Loss Ratio = Incurred Losses / Earned Premium
- Combined Ratio = (Losses + Expenses) / Earned Premium
- Claim Frequency = Claim Count / Exposure Units
- Claim Severity = Incurred Losses / Claim Count
- Retention Rate = Renewed / (Renewed + Cancelled)

## Response Guidelines
- Provide clear, actionable responses with code examples when relevant
- Use proper Databricks SQL and PySpark syntax
- Reference actual table names and UC structure
- Include data quality expectations where appropriate
- Keep responses focused and professional
"""

# COMMAND ----------

# DBTITLE 1,Define UnifiedInsuranceAgent ChatAgent Class
# ============================================
# Unified Insurance Agent ChatAgent Class (with streaming)
# ============================================

class UnifiedInsuranceAgent(ChatAgent):
    """Combined Architect + Data Engineer agent for P&C Insurance Medallion architecture.
    
    Implements both predict() and predict_stream() for full ChatAgent v2 compatibility.
    The base ChatAgent.predict_stream raises NotImplementedError — must override it.
    """
    
    SYSTEM_PROMPT = UNIFIED_SYSTEM_PROMPT
    LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
    MAX_TOKENS = 2000
    
    def _call_llm(self, messages, stream=False):
        """Call the Databricks Foundation Model API."""
        import mlflow.deployments
        
        client = mlflow.deployments.get_deploy_client("databricks")
        
        # Build messages with system prompt
        chat_messages = [{"role": "system", "content": self.SYSTEM_PROMPT}]
        for msg in messages:
            chat_messages.append({
                "role": msg.role,
                "content": msg.content or ""
            })
        
        if stream:
            return client.predict_stream(
                endpoint=self.LLM_ENDPOINT,
                inputs={"messages": chat_messages, "max_tokens": self.MAX_TOKENS}
            )
        else:
            return client.predict(
                endpoint=self.LLM_ENDPOINT,
                inputs={"messages": chat_messages, "max_tokens": self.MAX_TOKENS}
            )
    
    def predict(self, messages, context=None, custom_inputs=None):
        """Non-streaming prediction via ChatAgent protocol.
        
        Args:
            messages: List of ChatAgentMessage objects (chat history)
            context: Optional ChatContext with conversation metadata
            custom_inputs: Optional dict of custom inputs
            
        Returns:
            ChatAgentResponse with assistant message containing LLM response
        """
        result = self._call_llm(messages, stream=False)
        
        choices = result.get("choices", [])
        content = ""
        if choices:
            content = choices[0].get("message", {}).get("content", "")
        
        return ChatAgentResponse(
            messages=[ChatAgentMessage(role="assistant", content=content, id=str(uuid.uuid4()))]
        )
    
    def predict_stream(self, messages, context=None, custom_inputs=None):
        """Streaming prediction via ChatAgent protocol.
        
        Yields ChatAgentChunk objects with delta content for each token.
        Must override base ChatAgent.predict_stream which raises NotImplementedError.
        
        Args:
            messages: List of ChatAgentMessage objects (chat history)
            context: Optional ChatContext with conversation metadata
            custom_inputs: Optional dict of custom inputs
            
        Yields:
            ChatAgentChunk with partial content deltas
        """
        response_id = str(uuid.uuid4())
        
        stream = self._call_llm(messages, stream=True)
        
        for chunk in stream:
            choices = chunk.get("choices", [])
            if choices:
                delta = choices[0].get("delta", {})
                content = delta.get("content", "")
                if content:
                    yield ChatAgentChunk(
                        delta=ChatAgentMessage(
                            role="assistant",
                            content=content,
                            id=response_id
                        )
                    )
        
        # Final chunk with finish_reason
        yield ChatAgentChunk(
            delta=ChatAgentMessage(role="assistant", content="", id=response_id),
            finish_reason="stop"
        )

# COMMAND ----------

# DBTITLE 1,Register Unified Agent in MLflow
# ============================================
# Register Unified Agent in MLflow
# ============================================

MODEL_NAME = "workspace.default.pc_unified_agent"

with mlflow.start_run(run_name="unified_agent_chatagent") as run:
    mlflow.log_param("agent_type", "unified_architect_data_engineer")
    mlflow.log_param("model_type", "ChatAgent")
    mlflow.log_param("task", "agent/v2/chat")
    mlflow.log_param("streaming", "true")
    mlflow.log_param("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
    
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=UnifiedInsuranceAgent(),
        input_example=mlflow.pyfunc.CHAT_AGENT_INPUT_EXAMPLE,
    )
    
    # Register in Unity Catalog
    client = mlflow.tracking.MlflowClient()
    mv = client.create_model_version(
        name=MODEL_NAME,
        source=model_info.model_uri,
        run_id=run.info.run_id
    )
    
    print(f"✓ Model logged and registered!")
    print(f"  Model: {MODEL_NAME} v{mv.version}")
    print(f"  Run ID: {run.info.run_id}")
    print(f"  Model URI: {model_info.model_uri}")
    print(f"  Streaming: enabled (predict_stream implemented)")

# COMMAND ----------

# DBTITLE 1,Deploy Unified Agent as ChatAgent Endpoint
# ============================================
# Deploy Unified Agent as ChatAgent Serving Endpoint
# ============================================

# Get the latest model version
client = mlflow.tracking.MlflowClient()
latest_versions = client.search_model_versions(f"name='{MODEL_NAME}'")
latest_version = max(int(mv.version) for mv in latest_versions)
print(f"Deploying model version: {latest_version}")

endpoint_name = "pc_insurance_agent"

# Delete old endpoint if it exists (e.g., upgrading from v1 without streaming)
try:
    w.serving_endpoints.delete(name=endpoint_name)
    print(f"Deleted old endpoint: {endpoint_name}")
    import time
    time.sleep(20)
except Exception:
    print(f"No existing endpoint to delete: {endpoint_name}")

# Deploy as ChatAgent endpoint using databricks.agents.deploy()
# This creates an endpoint with task: agent/v2/chat, enabling streaming
agent_endpoint_info = deploy(
    model_name=MODEL_NAME,
    model_version=latest_version,
    endpoint_name=endpoint_name,
    scale_to_zero=True,
)

print(f"\n✓ Deployed ChatAgent endpoint: {endpoint_name}")
print(f"  Model: {MODEL_NAME} v{latest_version}")
print(f"  Task: agent/v2/chat (streaming enabled)")
print(f"  Scale to zero: True")
print(f"  Query: {agent_endpoint_info.query_endpoint}")

# COMMAND ----------

# DBTITLE 1,Test Unified Agent (predict + predict_stream)
# ============================================
# Test the Unified Agent
# ============================================

# Test 1: Non-streaming predict()
print("=== Test 1: Non-streaming predict() ===")
test_agent = UnifiedInsuranceAgent()

test_questions = [
    "Design the Bronze layer for ingesting P&C policy data",
    "Write a SQL MERGE statement for SCD2 on the customer dimension",
    "What tables are in the Gold layer?",
]

for q in test_questions:
    test_messages = [ChatAgentMessage(role="user", content=q)]
    result = test_agent.predict(test_messages)
    answer = result.messages[0].content if result.messages else "No response"
    print(f"\nQ: {q}")
    print(f"A: {answer[:200]}...")

# Test 2: Streaming predict_stream()
print("\n=== Test 2: Streaming predict_stream() ===")
test_messages = [ChatAgentMessage(role="user", content="What is the Silver layer? Reply in 2 sentences.")]
chunks = list(test_agent.predict_stream(test_messages))
print(f"Got {len(chunks)} chunks")
full_content = "".join(c.delta.content for c in chunks)
print(f"Streamed content: {full_content[:300]}")

print("\n✓ Unified Insurance Agent (ChatAgent with streaming) is ready!")
print(f"  Endpoint: pc_insurance_agent")
print(f"  Supervisor Agent tool: architect -> pc_insurance_agent")

# COMMAND ----------

