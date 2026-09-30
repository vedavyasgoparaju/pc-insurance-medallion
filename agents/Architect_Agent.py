# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Architect Agent - P&C Medallion Architecture Designer
# MAGIC %md
# MAGIC # Architect Agent - P&C Insurance Medallion Architecture Designer
# MAGIC
# MAGIC This notebook creates an AI agent that acts as a **Principal Data Architect** specializing in:
# MAGIC * Medallion architecture design (Bronze/Silver/Gold)
# MAGIC * P&C Insurance data modeling
# MAGIC * Unity Catalog governance and schema design
# MAGIC * Pipeline topology and data flow planning
# MAGIC
# MAGIC The agent is registered in MLflow and deployed as a model serving endpoint.
# MAGIC
# MAGIC ## System Prompt Summary
# MAGIC The Architect agent provides expert guidance on:
# MAGIC - Layer design principles and table grain
# MAGIC - SCD Type 2 implementation patterns
# MAGIC - Data quality expectation placement
# MAGIC - Governance, PII masking, and access control
# MAGIC - Scalability and performance patterns

# COMMAND ----------

# DBTITLE 1,Setup & Imports
# Install required packages
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow", "databricks-sdk", "databricks-agents", "-q"])

import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse, ChatContext
from databricks.sdk import WorkspaceClient
from databricks.agents import deploy
from mlflow.models.resources import DatabricksServingEndpoint
print(f"MLflow version: {mlflow.__version__}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")
w = WorkspaceClient()
print(f"Workspace: {w.config.host}")
print("✓ ChatAgent and databricks.agents.deploy available")

# COMMAND ----------

# DBTITLE 1,Architect Agent System Prompt
# ============================================
# Architect Agent System Prompt
# ============================================

ARCHITECT_SYSTEM_PROMPT = """You are a Principal Data Architect specializing in Property & Casualty (P&C) Insurance data platforms on Databricks.

## Your Expertise

### Medallion Architecture
You design three-layer architectures:
1. **Bronze Layer**: Raw ingestion - data as-is from source systems (PAS, Claims, Billing, CRM)
   - Append-only, immutable records
   - Auto Loader for streaming ingestion
   - Schema evolution with raw_payload JSON capture
   - Ingestion timestamp and source system tracking

2. **Silver Layer**: Conformed dimensions and facts
   - SCD Type 2 for slowly changing dimensions (policy_dim, claim_dim, customer_dim)
   - Deduplication by business key
   - PII masking (phone, email, address)
   - Data quality validation using UC functions
   - Referential integrity enforcement

3. **Gold Layer**: Business-ready aggregations
   - KPIs: Loss Ratio, Combined Ratio, Frequency, Severity, Retention Rate
   - Aggregated by period (monthly/quarterly) and line of business
   - Executive dashboard summaries

### P&C Insurance Domain Knowledge
You understand:
- Policy lifecycle: Quote → Bind → Issue → Renew → Cancel
- Claim lifecycle: FNOL → Investigation → Reserve → Pay → Close
- Key metrics:
  - Loss Ratio = Incurred Losses / Earned Premium
  - Combined Ratio = (Losses + Expenses) / Earned Premium
  - Claim Frequency = Claim Count / Exposure Units (policy-years)
  - Claim Severity = Incurred Losses / Claim Count
  - Retention Rate = Renewed / (Renewed + Cancelled)
- Lines of business: Personal Auto, Homeowners, Commercial Property, General Liability, Workers Comp
- Regulatory: NAIC guidelines, state-by-state requirements, rate filing

### Databricks Best Practices
- Unity Catalog for governance: catalogs, schemas, tables, and column-level security
- Delta Lake features: CDF, liquid clustering, Z-order, optimize, vacuum
- Spark Declarative Pipelines (SDP) for streaming/batch ETL
- Auto Loader for file-based ingestion with schema inference
- Delta Live Tables expectations for data quality (expect/drop/fail)
- Model serving for ML/AI agents
- Job orchestration for pipeline scheduling

## How You Respond

When asked to design an architecture:
1. Start with the business requirement and data sources
2. Design the Bronze layer schema and ingestion approach
3. Define Silver layer conformed dimensions and facts with grain
4. Specify Gold layer KPIs and aggregation logic
5. Include data quality expectations at each layer
6. Recommend governance: catalog structure, PII handling, access control
7. Provide scalability considerations

Always provide concrete table schemas with column names, types, and comments.
Use Databricks-specific terminology and best practices.
"""

print("Architect system prompt defined")
print(f"Prompt length: {len(ARCHITECT_SYSTEM_PROMPT)} characters")

# COMMAND ----------

# DBTITLE 1,Register Architect Agent in MLflow
# ============================================
# Register Architect Agent in MLflow
# ============================================

import os
from mlflow.models import infer_signature
from mlflow.models.resources import DatabricksServingEndpoint

# Define the agent as a ChatAgent subclass (NOT plain PythonModel)
# ChatAgent auto-sets task: agent/v2/chat, enabling streaming for Supervisor Agent
class ArchitectAgent(ChatAgent):
    """P&C Insurance Medallion Architecture Designer Agent"""
    
    def __init__(self):
        super().__init__()
        self.system_prompt = ARCHITECT_SYSTEM_PROMPT
    
    def predict(self, messages, context=None, custom_inputs=None):
        """Process architecture design questions via ChatAgent protocol.
        
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

with mlflow.start_run(run_name="architect_agent_chatagent_v2") as run:
    mlflow.log_param("agent_type", "architect")
    mlflow.log_param("domain", "pc_insurance")
    mlflow.log_param("role", "principal_architect")
    mlflow.log_param("prompt_length", len(ARCHITECT_SYSTEM_PROMPT))
    mlflow.log_param("model_type", "ChatAgent")
    mlflow.log_param("task", "agent/v2/chat")
    
    # Log the system prompt as an artifact
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write(ARCHITECT_SYSTEM_PROMPT)
        mlflow.log_artifact(f.name, artifact_path="system_prompt")
    
    # Log the model WITHOUT explicit signature
    # ChatAgent auto-infers signature and sets task: agent/v2/chat
    agent = ArchitectAgent()
    mlflow.pyfunc.log_model(
        artifact_path="architect_agent",
        python_model=agent,
        registered_model_name="workspace.default.pc_architect_agent",
        resources=[
            DatabricksServingEndpoint(endpoint_name="databricks-gpt-oss-120b")
        ],
        # No signature parameter - auto-detected for ChatAgent
    )
    
    print(f"Architect Agent logged to MLflow: {run.info.run_id}")
    print(f"MLflow Run URL: {run.info.artifact_uri}")
    print(f"Registered Model: workspace.default.pc_architect_agent")
    print(f"Model type: ChatAgent (task: agent/v2/chat)")
    
    # Clean up temp file
    os.unlink(f.name)

# COMMAND ----------

# DBTITLE 1,Deploy Architect Agent as ChatAgent Endpoint
# ============================================
# Deploy Architect Agent as ChatAgent Serving Endpoint
# ============================================

# databricks.agents.deploy() creates an agent-serving endpoint
# that supports streaming (unlike w.serving_endpoints.create which
# creates plain model endpoints without task field)

# Get the latest model version
import mlflow
client = mlflow.tracking.MlflowClient()
latest_versions = client.search_model_versions("name='workspace.default.pc_architect_agent'")
latest_version = max(int(mv.version) for mv in latest_versions)
print(f"Deploying model version: {latest_version}")

endpoint_name = "pc_architect_agent"

# Delete the old plain endpoint first (it was created without task field)
try:
    w.serving_endpoints.delete(name=endpoint_name)
    print(f"Deleted old plain endpoint: {endpoint_name}")
except Exception:
    print(f"No existing endpoint to delete: {endpoint_name}")

# Deploy as ChatAgent endpoint using databricks.agents.deploy()
# This creates an endpoint with task: agent/v2/chat, enabling streaming
agent_endpoint_info = deploy(
    model_name="workspace.default.pc_architect_agent",
    model_version=latest_version,
    endpoint_name=endpoint_name,
    scale_to_zero=True,
)

print(f"\n✓ Deployed ChatAgent endpoint: {endpoint_name}")
print(f"  Model: workspace.default.pc_architect_agent v{latest_version}")
print(f"  Task: agent/v2/chat (streaming enabled)")
print(f"\nTo query this agent (ChatCompletions format):")
print(f'  curl -X POST \'{w.config.host}/serving-endpoints/{endpoint_name}/invocations\' \\')
print(f'    -H "Authorization: Bearer $TOKEN" \\')
print(f'    -H "Content-Type: application/json" \\')
print(f'    -d \'{{"messages": [{{"role": "user", "content": "Design the Bronze layer for P&C claims data"}}]}}\'')

# COMMAND ----------

# DBTITLE 1,Test Architect Agent
# ============================================
# Test the Architect Agent
# ============================================

# Test locally using ChatAgent predict
test_agent = ArchitectAgent()
test_questions = [
    "Design the Bronze layer for ingesting P&C policy data",
    "What SCD2 strategy should I use for the customer dimension?",
    "How should I calculate loss ratio in the Gold layer?",
]

for q in test_questions:
    test_messages = [ChatAgentMessage(role="user", content=q)]
    result = test_agent.predict(test_messages)
    answer = result.messages[0].content if result.messages else "No response"
    print(f"\nQ: {q}")
    print(f"A: {answer[:200]}...")

print("\n✓ Architect Agent (ChatAgent) is ready for the Supervisor Agent")