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
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow", "databricks-sdk", "-q"])

import mlflow
from databricks.sdk import WorkspaceClient
print(f"MLflow version: {mlflow.__version__}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")
w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

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

# Define the agent as a callable Python class
class ArchitectAgent(mlflow.pyfunc.PythonModel):
    """P&C Insurance Medallion Architecture Designer Agent"""
    
    def __init__(self):
        self.system_prompt = ARCHITECT_SYSTEM_PROMPT
    
    def predict(self, context, model_input=None):
        """Process architecture design questions"""
        # Handle both old and new MLflow PythonModel signatures
        if model_input is None:
            # Old MLflow calls predict(model_input) without context
            model_input = context
            context = None
        
        # Call LLM with system prompt using Databricks Foundation Model API
        import mlflow.deployments
        
        # Extract question from input
        if isinstance(model_input, dict):
            question = model_input.get("question", model_input.get("query", ""))
        elif hasattr(model_input, 'iloc'):
            row = model_input.iloc[0].to_dict() if len(model_input) > 0 else {}
            question = row.get("question", row.get("query", ""))
        else:
            question = str(model_input)
        
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
        answer = llm_response["choices"][0]["message"]["content"]
        
        return {
            "agent": "Architect",
            "role": "Principal Data Architect",
            "question": question,
            "response": answer
        }

# Log the agent to MLflow
mlflow.set_experiment("/Users/vedavyas.goparaju@gmail.com/pc_insurance_agents")

with mlflow.start_run(run_name="architect_agent_v1") as run:
    mlflow.log_param("agent_type", "architect")
    mlflow.log_param("domain", "pc_insurance")
    mlflow.log_param("role", "principal_architect")
    mlflow.log_param("prompt_length", len(ARCHITECT_SYSTEM_PROMPT))
    
    # Log the system prompt as an artifact
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write(ARCHITECT_SYSTEM_PROMPT)
        mlflow.log_artifact(f.name, artifact_path="system_prompt")
    
    # Log the model
    agent = ArchitectAgent()
    
    # Create model signature (required for Unity Catalog registration)
    import pandas as pd
    sample_input = pd.DataFrame({"question": ["What is the Bronze layer design?"]})
    sample_output = pd.DataFrame({
        "agent": ["Architect"],
        "role": ["Principal Data Architect"],
        "question": ["What is the Bronze layer design?"],
        "response": ["[Architect Agent] Ready to design Medallion architecture for: What is the Bronze layer design?"],
    })
    signature = infer_signature(sample_input, sample_output)
    
    mlflow.pyfunc.log_model(
        artifact_path="architect_agent",
        python_model=agent,
        registered_model_name="workspace.default.pc_architect_agent",
        signature=signature,
        resources=[
            DatabricksServingEndpoint(endpoint_name="databricks-gpt-oss-120b")
        ],
    )
    
    print(f"Architect Agent logged to MLflow: {run.info.run_id}")
    print(f"MLflow Run URL: {run.info.artifact_uri}")
    print(f"Registered Model: pc_architect_agent")
    
    # Clean up temp file
    os.unlink(f.name)

# COMMAND ----------

# DBTITLE 1,Deploy Architect Agent as Serving Endpoint
# ============================================
# Deploy Architect Agent as Serving Endpoint
# ============================================

from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedModelInput

# Get the latest model version via MLflow client
import mlflow
client = mlflow.tracking.MlflowClient()
latest_versions = client.search_model_versions("name='workspace.default.pc_architect_agent'")
latest_version = max(int(mv.version) for mv in latest_versions)
model_uri = f"models:/pc_architect_agent/{latest_version}"

print(f"Deploying model: {model_uri}")

# Create serving endpoint (if not exists)
endpoint_name = "pc_architect_agent"
try:
    w.serving_endpoints.create(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_models=[
                ServedModelInput(
                    model_name="workspace.default.pc_architect_agent",
                    model_version=latest_version,
                    workload_size="Small",
                    scale_to_zero_enabled=True,
                    environment_vars={},
                )
            ],
            traffic_config={"routes": [{"served_model_name": "workspace.default.pc_architect_agent", "traffic_percentage": 100}]},
        )
    )
    print(f"✓ Creating serving endpoint: {endpoint_name}")
except Exception as e:
    if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
        print(f"Endpoint {endpoint_name} already exists - updating...")
    else:
        print(f"Note: {e}")
        print(f"You can also deploy from the UI: Models → pc_architect_agent → Create Serving Endpoint")

print(f"\nEndpoint name: {endpoint_name}")
print(f"Model URI: {model_uri}")
print(f"\nTo query this agent:")
print(f'  curl -X POST \'{w.config.host}/serving-endpoints/{endpoint_name}/invocations\' \\')
print(f'    -H "Authorization: Bearer $TOKEN" \\')
print(f'    -H "Content-Type: application/json" \\')
print(f'    -d \'{{"inputs": {{"question": "Design the Bronze layer for P&C claims data"}}}}\'')

# COMMAND ----------

# DBTITLE 1,Test Architect Agent
# ============================================
# Test the Architect Agent
# ============================================

# Test locally
test_agent = ArchitectAgent()
test_questions = [
    "Design the Bronze layer for ingesting P&C policy data",
    "What SCD2 strategy should I use for the customer dimension?",
    "How should I calculate loss ratio in the Gold layer?",
]

for q in test_questions:
    result = test_agent.predict({"question": q})
    print(f"\nQ: {q}")
    print(f"A: {result['response']}")

print("\n✓ Architect Agent is ready for the Supervisor Agent")