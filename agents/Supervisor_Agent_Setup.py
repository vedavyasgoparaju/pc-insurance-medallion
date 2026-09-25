# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Supervisor Agent Setup - P&C Multi-Agent Team
# MAGIC %md
# MAGIC # Supervisor Agent Setup - P&C Insurance Multi-Agent Team
# MAGIC
# MAGIC This notebook creates the **Supervisor Agent** that orchestrates all subagents:
# MAGIC
# MAGIC | Agent | Role | Skill Level | Type | Description |
# MAGIC |---|---|---|---|---|
# MAGIC | Architect | Principal Architect | Expert | Serving Endpoint | Designs Medallion architecture, schemas, data flow |
# MAGIC | Data Engineer | Senior Data Engineer | Senior | Serving Endpoint | Generates pipeline code, SQL, DQ expectations |
# MAGIC | Domain Expert | P&C Insurance SME | Senior | Knowledge Assistant | Answers insurance domain questions |
# MAGIC | Analyst | Business Analyst | Mid | Genie Space | Queries Gold layer for KPIs and business metrics |
# MAGIC | QA Validator | QA Engineer | Junior | UC Function | Runs data quality validation checks |
# MAGIC | Documentation | Technical Writer | Senior | Genie Space | Generates/updates technical documentation |
# MAGIC | DevOps | DevOps Engineer | Senior | Genie Space | Git/CI-CD guidance only (cannot execute) |
# MAGIC | Workspace-Actions | MCP Server | N/A | Databricks App | Executes workspace changes: file writes, SQL, git commits |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Run `Architect_Agent` notebook → creates `pc_architect_agent` serving endpoint
# MAGIC 2. Run `Data_Engineer_Agent` notebook → creates `pc_data_engineer_agent` serving endpoint
# MAGIC 3. Run `Domain_Expert_Setup` notebook → creates UC volume `pc_insurance.reference.pc_domain_docs`
# MAGIC 4. Run `Analyst_Genie_Setup` notebook → creates Genie Spaces (analyst, documentation, devops)
# MAGIC 5. UC functions in `pc_insurance.dq` must exist
# MAGIC 6. MCP app `pc-insurance-workspace-actions` must be running
# MAGIC
# MAGIC ## Idempotent
# MAGIC This notebook is idempotent — it checks for an existing Supervisor Agent by display name and only creates if not found. Tools are registered only if not already present. Safe to re-run.

# COMMAND ----------

# DBTITLE 1,Setup & Configuration (REST API)
# ============================================
# Setup & Imports
# ============================================
import urllib.request
import urllib.error
import json
import os
import time

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
host = w.config.host

# Get auth headers from SDK (works on job clusters, serverless, and local)
auth_headers = w.config.authenticate()
HEADERS = {**auth_headers, "Content-Type": "application/json"}
print(f"Workspace: {host}")
print(f"Auth headers: {list(auth_headers.keys())}")

# ============================================
# Configuration
# ============================================
SUPERVISOR_DISPLAY_NAME = "P&C Insurance Medallion Architecture Team"
SUPERVISOR_DESCRIPTION = "A multi-agent team that designs, develops, and operates a Medallion architecture for Property & Casualty (P&C) Insurance. Routes questions to the right specialist: Architect for design, Data Engineer for code, Domain Expert for insurance knowledge, Analyst for KPIs, QA for validation, Documentation for technical writing, DevOps for Git/CI-CD guidance, and Workspace-Actions for executing workspace changes."

SUPERVISOR_INSTRUCTIONS = """You are the team lead for a virtual team building a Medallion architecture for a Property & Casualty (P&C) Insurance use case on Databricks.

## Your Team Members

1. ARCHITECT (Principal Data Architect) - Designs the overall Medallion architecture, defines Bronze/Silver/Gold layer schemas, data flow topology, and Unity Catalog governance. Route architecture and design questions here. DO NOT route implementation, KPI, or Git questions here.

2. DATA ENGINEER (Senior Data Engineer) - Implements Bronze/Silver/Gold pipelines, writes Spark Declarative Pipeline (SDP) code, SQL transformations, and data quality expectations. Route code generation and pipeline implementation questions here. DO NOT route architecture design, business KPI queries, or Git operations here.

3. P&C DOMAIN EXPERT (Insurance SME) - Answers questions about P&C insurance domain: policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements. Route insurance domain questions here. DO NOT route KPI queries, code generation, or Git operations here.

4. ANALYST (Business Analyst) - Answers business questions by querying Gold layer tables: loss ratios, combined ratios, claim frequency/severity, retention rates, premium growth, exposure summaries. Route KPI and business metric questions here. DO NOT route architecture, code generation, Git operations, or documentation requests here.

5. QA VALIDATOR (QA Engineer) - Runs data quality validation checks on the data by calling the pc_insurance.dq.calculate_dq_score UC function. Validates premium amounts, claim statuses, policy existence, and loss ratios. Route data quality and validation questions here. DO NOT route architecture, code generation, or Git questions here.

6. DOCUMENTATION (Technical Writer) - Generates and updates technical documentation for the P&C Insurance Medallion architecture: README files, architecture guides, data dictionaries, pipeline documentation. Route documentation requests here. DO NOT route KPI queries, code generation, or Git operations here.

7. DEVOPS (DevOps Engineer) - Provides GUIDANCE on Git operations, CI/CD, branch management, and DAB deployment for the P&C Insurance Medallion project. Advises on best practices for version control and deployment. Route Git and CI/CD guidance questions here. DO NOT route KPI queries, business metrics, architecture design, code generation, or documentation requests here. DEVOPS provides GUIDANCE ONLY -- it cannot execute Git operations.

8. WORKSPACE-ACTIONS (MCP Server) - EXECUTES approved workspace actions for the P&C Insurance project: writes/updates notebooks and files, runs notebooks, executes SQL statements, triggers Databricks Jobs (Job 820361677269451 for agent setup, Job 894776717783668 for data pipeline with load_type INITIAL or INCREMENTAL), checks job run status, and performs git commit/push operations. Route execution requests here when the user wants to actually perform an action, including triggering data pipelines or retraining agents, not just get guidance.

## Anti-Routing Rules (HARD BOUNDARIES)

1. DO NOT route KPI or business metric questions to DEVOPS. DevOps is for Git/CI-CD guidance only.
2. DO NOT route KPI or business metric questions to DOCUMENTATION. Documentation is for technical writing only.
3. DO NOT route Git execution requests to DEVOPS. Use WORKSPACE-ACTIONS for actual git commit/push. DEVOPS is guidance only.
4. DO NOT route architecture design questions to DATA ENGINEER. Data Engineer implements code; Architect designs.
5. DO NOT route code implementation questions to ARCHITECT. Architect designs; Data Engineer implements.
6. DO NOT route insurance domain definition questions to ANALYST. Domain Expert explains concepts; Analyst queries actual metric values from Gold tables.

## Routing Rules

- Architecture and design questions -> ARCHITECT
- Code and pipeline questions -> DATA ENGINEER
- Insurance domain questions -> P&C DOMAIN EXPERT
- KPI and business questions -> ANALYST
- Data quality questions -> QA VALIDATOR
- Documentation requests -> DOCUMENTATION
- Git/CI-CD guidance -> DEVOPS (guidance only)
- Git/CI-CD execution -> WORKSPACE-ACTIONS (actual execution)
- Job/pipeline triggering -> WORKSPACE-ACTIONS (use run_job tool with job_id and job_parameters)
- Complex questions -> Decompose, route to multiple agents, synthesize.

## Synthesis Rules

1. Present in logical order (architecture first, then implementation, then domain context)
2. Cite which agent provided each part
3. Ensure consistency across agent responses
4. Add a summary at the end if the response is long
"""

# Tool configuration: (tool_id, tool_type, spec_dict, description)
TOOLS_CONFIG = [
    ("architect", "serving_endpoint",
     {"serving_endpoint": {"name": "pc_architect_agent"}},
     "Designs the overall Medallion architecture for P&C insurance: Bronze/Silver/Gold layer schemas, data flow topology, Unity Catalog structure, governance policies, SCD2 strategies, and scalability patterns. DO NOT route code implementation, KPI queries, or Git operations here."),
    ("data-engineer", "serving_endpoint",
     {"serving_endpoint": {"name": "pc_data_engineer_agent"}},
     "Implements Bronze/Silver/Gold pipelines for P&C insurance: writes SDP code, SQL transformations, MERGE statements for SCD2, and data quality expectations. DO NOT route architecture design, KPI queries, or Git operations here."),
    ("pc-domain-expert", "volume",
     {"volume": {"name": "pc_insurance.reference.pc_domain_docs"}},
     "Answers P&C insurance domain questions: policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements. DO NOT route KPI queries, code generation, or Git operations here."),
    ("qa-validator", "uc_function",
     {"uc_function": {"name": "pc_insurance.dq.calculate_dq_score"}},
     "Runs data quality validation checks on P&C insurance data by calling the pc_insurance.dq.calculate_dq_score UC function. DO NOT route architecture, code generation, or Git questions here."),
    ("analyst", "genie_space",
     {"genie_space": {"id": "01f1b6a93ad91b3d8e225bd409a14a44"}},
     "Answers business KPI questions by querying Gold layer tables: loss ratios, combined ratios, claim frequency/severity, retention rates, premium growth, exposure summaries. DO NOT route architecture, code generation, Git operations, or documentation requests here."),
    ("documentation", "genie_space",
     {"genie_space": {"id": "01f1b6af258f1a9ca1e984ab82ef6bc7"}},
     "Generates and updates technical documentation for the P&C Insurance Medallion architecture: README files, architecture guides, data dictionaries, pipeline documentation. DO NOT route KPI queries, code generation, or Git operations here."),
    ("devops", "genie_space",
     {"genie_space": {"id": "01f1b72df61b1fc3ac9dc984d402bdf0"}},
     "Provides GUIDANCE on Git operations, CI/CD, branch management, and DAB deployment. Advises on best practices for version control and deployment. DEVOPS provides GUIDANCE ONLY -- it cannot execute Git operations. DO NOT route KPI queries, business metrics, architecture design, code generation, or documentation requests here."),
    ("workspace-actions", "app",
     {"app": {"name": "pc-insurance-workspace-actions"}},
     "EXECUTES approved workspace actions for the P&C Insurance project: writes/updates notebooks and files, runs notebooks, executes SQL statements, triggers Databricks Jobs (Job 820361677269451 for agent setup, Job 894776717783668 for data pipeline with load_type INITIAL or INCREMENTAL), checks job run status, and performs git commit/push operations. Route execution requests here when the user wants to actually perform an action, including triggering data pipelines or retraining agents, not just get guidance."),
]

print(f"\nConfiguration loaded: {len(TOOLS_CONFIG)} tools to register")
print(f"Supervisor: {SUPERVISOR_DISPLAY_NAME}")

# COMMAND ----------

# DBTITLE 1,Create or Get Supervisor Agent
# ============================================
# Step 1: Create or Get Supervisor Agent (Idempotent)
# ============================================

def api_request(method, path, body=None):
    """Helper for REST API calls to Supervisor Agents API."""
    url = f"{host}/api/2.1/supervisor-agents{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        raise Exception(f"{e.code}: {error_body[:300]}")

# List existing Supervisor Agents to check if one already exists
supervisor_name = None
supervisor_id = None
try:
    result = api_request("GET", "")
    existing_agents = result.get("supervisor_agents", [])
    for agent in existing_agents:
        if agent.get("display_name") == SUPERVISOR_DISPLAY_NAME:
            supervisor_name = agent.get("name")
            supervisor_id = agent.get("supervisor_agent_id")
            print(f"✓ Found existing Supervisor Agent: {supervisor_name}")
            break
except Exception as e:
    print(f"Note: Could not list agents: {e}")

# Create new Supervisor Agent if not found
if not supervisor_name:
    print(f"\nCreating new Supervisor Agent: {SUPERVISOR_DISPLAY_NAME}...")
    create_body = {
        "display_name": SUPERVISOR_DISPLAY_NAME,
        "description": SUPERVISOR_DESCRIPTION,
        "instructions": SUPERVISOR_INSTRUCTIONS,
    }
    result = api_request("POST", "", create_body)
    supervisor_name = result.get("name")
    supervisor_id = result.get("supervisor_agent_id")
    print(f"✓ Supervisor Agent created: {supervisor_name}")
    print(f"  Endpoint: {result.get('endpoint_name', 'N/A')}")
    print(f"  Experiment: {result.get('experiment_id', 'N/A')}")
else:
    # Update instructions on existing agent (in case they changed)
    try:
        update_body = {
            "display_name": SUPERVISOR_DISPLAY_NAME,
            "description": SUPERVISOR_DESCRIPTION,
            "instructions": SUPERVISOR_INSTRUCTIONS,
        }
        api_request("PATCH", f"/{supervisor_id}?update_mask=display_name,description,instructions", update_body)
        print(f"✓ Updated existing Supervisor Agent instructions")
    except Exception as e:
        print(f"Note: Could not update instructions: {e}")

print(f"\nSupervisor Agent ID: {supervisor_id}")
print(f"Supervisor Agent Name: {supervisor_name}")

# COMMAND ----------

# DBTITLE 1,Register All Tools
# ============================================
# Step 2: Register All 8 Tools (Idempotent)
# ============================================

# Get existing tools to avoid duplicate registration
existing_tool_ids = set()
try:
    result = api_request("GET", f"/{supervisor_id}/tools")
    for t in result.get("tools", []):
        existing_tool_ids.add(t.get("tool_id"))
    print(f"Existing tools: {len(existing_tool_ids)}")
except Exception as e:
    print(f"Note: Could not list existing tools: {e}")

print(f"\nRegistering {len(TOOLS_CONFIG)} tools...")
success_count = 0
for tool_id, tool_type, spec, description in TOOLS_CONFIG:
    tool_body = {
        "tool_type": tool_type,
        "description": description,
        **spec,
    }
    
    if tool_id in existing_tool_ids:
        # PATCH existing tool to keep description in sync
        try:
            update_body = {**tool_body, "tool_id": tool_id, "name": f"supervisor-agents/{supervisor_id}/tools/{tool_id}"}
            api_request("PATCH", f"/{supervisor_id}/tools/{tool_id}?update_mask=description", update_body)
            print(f"  ✓ {tool_id} (updated)")
        except Exception as e:
            print(f"  ✓ {tool_id} (exists, update note: {str(e)[:100]})")
        success_count += 1
    else:
        try:
            api_request("POST", f"/{supervisor_id}/tools?tool_id={tool_id}", tool_body)
            print(f"  ✓ {tool_id} (registered)")
            success_count += 1
        except Exception as e:
            print(f"  ✗ {tool_id}: {str(e)[:200]}")

print(f"\n✓ {success_count}/{len(TOOLS_CONFIG)} tools registered")

# COMMAND ----------

# DBTITLE 1,Verify Supervisor Agent
# ============================================
# Step 3: Verify Configuration & Endpoint Status
# ============================================

print("=" * 60)
print("SUPERVISOR AGENT CONFIGURATION SUMMARY")
print("=" * 60)

# Get the Supervisor Agent
agent = api_request("GET", f"/{supervisor_id}")
print(f"\n  Display Name: {agent.get('display_name')}")
print(f"  Description: {agent.get('description', '')[:80]}...")
print(f"  Supervisor ID: {agent.get('supervisor_agent_id')}")
print(f"  Endpoint Name: {agent.get('endpoint_name', 'N/A')}")
print(f"  Experiment ID: {agent.get('experiment_id', 'N/A')}")
print(f"  Instructions: {len(agent.get('instructions', ''))} chars")

# Get registered tools
tools_result = api_request("GET", f"/{supervisor_id}/tools")
tools = tools_result.get("tools", [])
print(f"\n  Tools ({len(tools)}):")
for t in tools:
    print(f"    [{t.get('tool_type')}] {t.get('tool_id')}")

# Check endpoint status
print(f"\n  Endpoint Status:")
try:
    for ep in w.serving_endpoints.list():
        if ep.name == agent.get('endpoint_name'):
            print(f"    {ep.name}: {ep.state.ready}")
            break
    else:
        # Check sub-agent endpoints too
        for ep in w.serving_endpoints.list():
            if 'pc_' in ep.name.lower() or 'mas' in ep.name.lower():
                print(f"    {ep.name}: {ep.state.ready}")
except Exception as e:
    print(f"    Could not check endpoints: {e}")

print("\n" + "=" * 60)
print("SUPERVISOR AGENT SETUP COMPLETE")
print("=" * 60)
print(f"\nSupervisor Agent: {supervisor_name}")
print(f"Endpoint: {agent.get('endpoint_name', 'N/A')}")
print(f"Tools: {len(tools)}/8 registered")
print(f"\nTo test: Go to AI > Supervisor Agents > {SUPERVISOR_DISPLAY_NAME}")
print("Or use the serving endpoint to send queries.")