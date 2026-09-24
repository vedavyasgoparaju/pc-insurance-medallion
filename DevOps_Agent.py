# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,DevOps Agent - Git & CI/CD Specialist
# MAGIC %md
# MAGIC # DevOps Agent - Git, CI/CD & Deployment Specialist
# MAGIC
# MAGIC This notebook creates an AI agent that acts as a **DevOps Advisor** specializing in:
# MAGIC * Git operations (commit, push, pull, branch, merge)
# MAGIC * CI/CD pipeline guidance
# MAGIC * Databricks Repos setup and configuration
# MAGIC * Declarative Automation Bundles (DABs) deployment
# MAGIC * Version control workflows and best practices
# MAGIC
# MAGIC The agent is registered in MLflow and deployed as a model serving endpoint.
# MAGIC
# MAGIC ## System Prompt Summary
# MAGIC The DevOps agent provides expert guidance on:
# MAGIC - Git workflow and branching strategies
# MAGIC - CI/CD pipeline setup and automation
# MAGIC - Databricks Repos integration
# MAGIC - DAB deployment and environment management
# MAGIC - Version control best practices for data pipelines

# COMMAND ----------

# DBTITLE 1,Setup & Imports
# Install required packages
import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install", "mlflow", "databricks-sdk", "-q"])

import mlflow
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

print(f"MLflow version: {mlflow.__version__}")
print(f"Tracking URI: {mlflow.get_tracking_uri()}")
w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# DBTITLE 1,DevOps Agent System Prompt
# ============================================
# DevOps Agent System Prompt
# ============================================

DEVOPS_SYSTEM_PROMPT = """You are a DevOps Advisor specializing in Git operations, CI/CD pipelines, and deployment automation for Databricks data platforms.

## Your Expertise

### Git Operations
You provide guidance on:
1. **Version Control Basics**
   - Git commit best practices
   - Commit message conventions
   - Push/pull workflows
   - Branch management

2. **Branching Strategies**
   - Feature branch workflow
   - Main/development branch patterns
   - Release branching
   - Hotfix procedures

3. **Merge & Conflict Resolution**
   - Merge strategies
   - Conflict resolution techniques
   - Pull request workflows
   - Code review processes

### Databricks Repos
You guide users on:
1. **Repo Setup**
   - Connecting Git repositories
   - Authentication configuration
   - Workspace folder structure
   - Repo synchronization

2. **Best Practices**
   - Notebook organization
   - .gitignore patterns
   - Handling secrets and credentials
   - Collaborative development

### CI/CD Pipelines
You provide expertise on:
1. **Pipeline Design**
   - Build stages
   - Test automation
   - Deployment gates
   - Environment promotion

2. **Databricks Integration**
   - Job orchestration
   - Workflow automation
   - Testing strategies
   - Rollback procedures

### Declarative Automation Bundles (DABs)
You guide deployment using:
1. **DAB Configuration**
   - databricks.yml structure
   - Resource definitions
   - Environment variables
   - Target environments

2. **Deployment Workflow**
   - Bundle validation
   - Deployment commands
   - Environment-specific configs
   - Deployment best practices

## Your Approach

### When Users Ask About Git
1. Identify the specific Git operation needed
2. Provide step-by-step instructions
3. Include command examples
4. Explain the outcome and next steps
5. Reference relevant documentation (DOC-039, DOC-042, DOC-043)

### When Users Ask About CI/CD
1. Understand the pipeline stage or automation need
2. Explain the workflow and dependencies
3. Provide configuration examples
4. Highlight testing and validation steps
5. Reference relevant documentation (DOC-041)

### When Users Ask About DABs
1. Clarify the deployment target and environment
2. Show databricks.yml configuration
3. Explain deployment commands
4. Guide environment-specific setup
5. Reference relevant documentation (DOC-040)

### When Users Ask About Job Orchestration
1. Explain the multi-agent execution flow
2. Show job configuration
3. Describe dependencies and sequencing
4. Reference relevant documentation (DOC-037)

## Documentation Access

You have access to the project_documentation table containing:
- **DOC-037**: Multi-Agent Job orchestration
- **DOC-038**: DevOps Agent description
- **DOC-039**: Git Workflow guide
- **DOC-040**: Declarative Automation Bundles (DABs)
- **DOC-041**: CI/CD Pipeline configuration
- **DOC-042**: DevOps Agent instructions
- **DOC-043**: Git sync check utility

Query this table to retrieve specific guidance and examples.

## Response Format

### For Git Questions
Provide:
1. **Context**: What the operation does
2. **Prerequisites**: What needs to be in place
3. **Commands**: Exact git commands with explanations
4. **Validation**: How to verify success
5. **Next Steps**: What to do after

### For CI/CD Questions
Provide:
1. **Overview**: Pipeline stage or automation goal
2. **Configuration**: YAML or config examples
3. **Workflow**: Step-by-step process
4. **Testing**: Validation and testing approach
5. **Troubleshooting**: Common issues and solutions

### For DAB Questions
Provide:
1. **Setup**: databricks.yml structure
2. **Configuration**: Resource definitions
3. **Commands**: Deployment commands
4. **Environments**: Dev/staging/prod setup
5. **Best Practices**: Deployment guidelines

## Important Guidelines

1. **Be Practical**: Provide actionable, copy-paste-ready examples
2. **Be Specific**: Reference exact file paths, commands, and configurations
3. **Be Safe**: Warn about destructive operations (force push, etc.)
4. **Be Current**: Use Databricks best practices and modern Git workflows
5. **Be Helpful**: Anticipate follow-up questions and provide comprehensive guidance

## What You DON'T Do

- ❌ Don't answer business KPI questions (route to Analyst)
- ❌ Don't write pipeline code (route to Data Engineer)
- ❌ Don't design architecture (route to Architect)
- ❌ Don't answer insurance domain questions (route to P&C Domain Expert)
- ❌ Don't execute Git operations (guide only; execution goes to Workspace-Actions)

## What You DO

- ✅ Provide Git operation guidance
- ✅ Explain CI/CD workflows
- ✅ Guide DAB deployment
- ✅ Help with Databricks Repos setup
- ✅ Share version control best practices
- ✅ Reference relevant documentation
- ✅ Include practical examples

Remember: You provide GUIDANCE on DevOps operations. Actual execution (git commit, file writes, etc.) is handled by the Workspace-Actions agent.
"""

print(f"Prompt length: {len(DEVOPS_SYSTEM_PROMPT)} characters")

# COMMAND ----------

# DBTITLE 1,DevOps Agent Implementation
import mlflow
from mlflow.pyfunc import PythonModel
from pyspark.sql import SparkSession

class DevOpsAgent(PythonModel):
    """
    DevOps Agent for Git, CI/CD, and deployment guidance.
    
    Queries the project_documentation table for DevOps-related content
    and provides practical guidance on Git operations, CI/CD pipelines,
    and DAB deployment.
    """
    
    def __init__(self):
        self.system_prompt = DEVOPS_SYSTEM_PROMPT
        self.documentation_table = "pc_insurance.reference.project_documentation"
        
    def query_documentation(self, query_text: str, doc_ids: list = None) -> str:
        """
        Query the project_documentation table for DevOps guidance.
        
        Args:
            query_text: Search terms or topic
            doc_ids: Optional list of specific document IDs to retrieve
        
        Returns:
            Relevant documentation content
        """
        try:
            spark = SparkSession.builder.getOrCreate()
            
            if doc_ids:
                # Query specific document IDs
                doc_filter = " OR ".join([f"doc_id = '{doc_id}'" for doc_id in doc_ids])
                query = f"""
                    SELECT doc_id, title, content, category, tags
                    FROM {self.documentation_table}
                    WHERE ({doc_filter})
                    AND is_active = true
                    ORDER BY doc_id
                """
            else:
                # Search by keywords
                query = f"""
                    SELECT doc_id, title, content, category, tags
                    FROM {self.documentation_table}
                    WHERE category = 'devops'
                    AND is_active = true
                    AND (
                        LOWER(title) LIKE LOWER('%{query_text}%')
                        OR LOWER(content) LIKE LOWER('%{query_text}%')
                        OR LOWER(tags) LIKE LOWER('%{query_text}%')
                    )
                    ORDER BY doc_id
                    LIMIT 5
                """
            
            results = spark.sql(query).collect()
            
            if not results:
                return "No relevant documentation found."
            
            # Format results
            docs = []
            for row in results:
                doc = f"""
### {row['doc_id']}: {row['title']}

{row['content']}

**Tags**: {row['tags']}
---
"""
                docs.append(doc)
            
            return "\n".join(docs)
            
        except Exception as e:
            return f"Error querying documentation: {str(e)}"
    
    def predict(self, context, model_input):
        """
        Process user query and provide DevOps guidance.
        
        Args:
            context: MLflow context (unused)
            model_input: Dictionary with 'messages' key containing conversation history
        
        Returns:
            Dictionary with 'content' key containing the response
        """
        try:
            # Extract the user's question
            messages = model_input.get("messages", [])
            if not messages:
                return {"content": "No input provided."}
            
            user_query = messages[-1].get("content", "")
            
            # Determine which documentation to retrieve based on keywords
            query_lower = user_query.lower()
            
            doc_ids = []
            search_terms = []
            
            # Map keywords to document IDs
            if any(word in query_lower for word in ["git", "commit", "push", "pull", "branch", "merge"]):
                doc_ids.extend(["DOC-039", "DOC-042", "DOC-043"])
                search_terms.append("git")
            
            if any(word in query_lower for word in ["ci/cd", "cicd", "pipeline", "deploy", "automation"]):
                doc_ids.append("DOC-041")
                search_terms.append("ci/cd")
            
            if any(word in query_lower for word in ["dab", "bundle", "databricks.yml"]):
                doc_ids.append("DOC-040")
                search_terms.append("dab")
            
            if any(word in query_lower for word in ["job", "orchestrat", "workflow"]):
                doc_ids.append("DOC-037")
                search_terms.append("orchestration")
            
            # Query documentation
            if doc_ids:
                docs = self.query_documentation("", doc_ids=doc_ids)
            elif search_terms:
                docs = self.query_documentation(search_terms[0])
            else:
                # General DevOps query
                docs = self.query_documentation("devops")
            
            # Build response
            response = f"""Based on the DevOps documentation:

{docs}

**Guidance for your question**: "{user_query}"

"""
            
            # Add specific guidance based on question type
            if "git" in query_lower and "commit" in query_lower:
                response += """
### Git Commit Best Practices

1. **Stage your changes**:
   ```bash
   git add <file1> <file2>
   # or
   git add .
   ```

2. **Commit with a descriptive message**:
   ```bash
   git commit -m "feat: Add DevOps agent implementation"
   ```

3. **Push to remote**:
   ```bash
   git push origin <branch-name>
   ```

**Commit Message Convention**:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation changes
- `refactor:` Code refactoring
- `test:` Test additions/changes
"""
            
            elif "ci/cd" in query_lower or "pipeline" in query_lower:
                response += """
### CI/CD Pipeline Overview

The pipeline typically includes:
1. **Build Stage**: Validate code and dependencies
2. **Test Stage**: Run unit and integration tests
3. **Deploy Stage**: Deploy to target environment
4. **Validate Stage**: Post-deployment validation

Refer to DOC-041 for detailed pipeline configuration.
"""
            
            elif "dab" in query_lower or "bundle" in query_lower:
                response += """
### DAB Deployment Steps

1. **Validate bundle**:
   ```bash
   databricks bundle validate
   ```

2. **Deploy to target environment**:
   ```bash
   databricks bundle deploy -t dev
   databricks bundle deploy -t prod
   ```

3. **Run deployed resources**:
   ```bash
   databricks bundle run -t dev
   ```

Refer to DOC-040 for detailed DAB configuration.
"""
            
            return {"content": response}
            
        except Exception as e:
            return {"content": f"Error processing request: {str(e)}"}

# Test the agent
agent = DevOpsAgent()
print("✅ DevOps Agent initialized successfully")

# COMMAND ----------

# DBTITLE 1,Register Agent in MLflow
# Set up MLflow experiment
experiment_name = "/Users/vedavyas.goparaju@gmail.com/pc_insurance_devops_agent"
mlflow.set_experiment(experiment_name)

# Start MLflow run
with mlflow.start_run(run_name="devops_agent_v1") as run:
    
    # Log parameters
    mlflow.log_param("agent_type", "devops_advisor")
    mlflow.log_param("documentation_table", "pc_insurance.reference.project_documentation")
    mlflow.log_param("prompt_length", len(DEVOPS_SYSTEM_PROMPT))
    
    # Save system prompt as artifact
    with open("/tmp/devops_system_prompt.txt", "w") as f:
        f.write(DEVOPS_SYSTEM_PROMPT)
    mlflow.log_artifact("/tmp/devops_system_prompt.txt")
    
    # Log the model
    mlflow.pyfunc.log_model(
        artifact_path="devops_agent",
        python_model=DevOpsAgent(),
        registered_model_name="pc_insurance_devops_agent",
        pip_requirements=[
            "mlflow",
            "databricks-sdk"
        ]
    )
    
    run_id = run.info.run_id
    print(f"✅ Model logged with run_id: {run_id}")

# COMMAND ----------

# DBTITLE 1,Test the Agent
# Test with sample queries
test_queries = [
    "How do I commit and push changes to Git?",
    "What is the CI/CD pipeline workflow?",
    "How do I deploy using Databricks Bundles (DABs)?",
    "How do I check Git sync status?"
]

print("=== Testing DevOps Agent ===\n")

for query in test_queries:
    print(f"Query: {query}")
    print("-" * 80)
    
    response = agent.predict(None, {"messages": [{"content": query}]})
    print(response["content"][:500] + "...\n")
    print("=" * 80)
    print()

# COMMAND ----------

# DBTITLE 1,Create Model Serving Endpoint
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput

w = WorkspaceClient()

endpoint_name = "pc_devops_agent"

# Get the latest model version
model_name = "pc_insurance_devops_agent"
client = mlflow.tracking.MlflowClient()
latest_version = client.get_latest_versions(model_name, stages=["None"])[0].version

print(f"Latest model version: {latest_version}")

# Create or update endpoint
try:
    # Try to get existing endpoint
    existing_endpoint = w.serving_endpoints.get(endpoint_name)
    print(f"Endpoint {endpoint_name} already exists. Updating...")
    
    w.serving_endpoints.update_config_and_wait(
        name=endpoint_name,
        served_entities=[
            ServedEntityInput(
                entity_name=f"{model_name}",
                entity_version=latest_version,
                scale_to_zero_enabled=True,
                workload_size="Small"
            )
        ]
    )
    print(f"✅ Endpoint {endpoint_name} updated successfully")
    
except Exception as e:
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print(f"Creating new endpoint {endpoint_name}...")
        
        w.serving_endpoints.create_and_wait(
            name=endpoint_name,
            config=EndpointCoreConfigInput(
                served_entities=[
                    ServedEntityInput(
                        entity_name=f"{model_name}",
                        entity_version=latest_version,
                        scale_to_zero_enabled=True,
                        workload_size="Small"
                    )
                ]
            )
        )
        print(f"✅ Endpoint {endpoint_name} created successfully")
    else:
        print(f"❌ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Endpoint Information
print(f"""
╔════════════════════════════════════════════════════════════════════════════╗
║                                                                            ║
║                     DEVOPS AGENT DEPLOYMENT COMPLETE                       ║
║                                                                            ║
╚════════════════════════════════════════════════════════════════════════════╝

✅ Model: {model_name}
✅ Version: {latest_version}
✅ Endpoint: {endpoint_name}
✅ Status: Active

📚 Documentation Access:
   - DOC-037: Multi-Agent Job orchestration
   - DOC-038: DevOps Agent description
   - DOC-039: Git Workflow
   - DOC-040: Declarative Automation Bundles (DABs)
   - DOC-041: CI/CD Pipeline
   - DOC-042: DevOps Agent instructions
   - DOC-043: Git sync check utility

🎯 Capabilities:
   ✓ Git operations guidance (commit, push, pull, branch, merge)
   ✓ CI/CD pipeline configuration
   ✓ Databricks Repos setup
   ✓ DAB deployment guidance
   ✓ Version control best practices

🔗 Integration:
   - Add to Supervisor Agent configuration
   - Route Git/CI-CD questions to this endpoint
   - Use Workspace-Actions for actual Git execution

📝 Next Steps:
   1. Test the endpoint with sample queries
   2. Update Supervisor_Agent_Setup.py to reference this endpoint
   3. Document the agent in project documentation
   4. Validate routing in multi-agent setup

════════════════════════════════════════════════════════════════════════════
""")

# COMMAND ----------

# DBTITLE 1,Sample Query Test
# Test the deployed endpoint
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

test_query = {
    "messages": [
        {
            "role": "user",
            "content": "How do I commit and push changes to Git in Databricks?"
        }
    ]
}

print("Testing deployed endpoint...")
print(f"Query: {test_query['messages'][0]['content']}\n")

response = w.serving_endpoints.query(
    name=endpoint_name,
    inputs=test_query
)

print("Response:")
print("=" * 80)
print(response.predictions[0]["content"])
print("=" * 80)

print("\n✅ DevOps Agent is ready for use!")
