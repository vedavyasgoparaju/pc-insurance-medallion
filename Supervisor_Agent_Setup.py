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
# MAGIC | Intern | Data Analyst | Entry | Genie Space | Basic exploration, profiling, documentation |
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. Run `Architect_Agent` notebook → creates `pc_architect_agent` serving endpoint
# MAGIC 2. Run `Data_Engineer_Agent` notebook → creates `pc_data_engineer_agent` serving endpoint
# MAGIC 3. Run `Domain_Expert_Setup` notebook → creates Knowledge Assistant `pc_domain_expert`
# MAGIC 4. Run `Analyst_Genie_Setup` notebook → creates Genie Space `PC_Insurance_Analyst`
# MAGIC 5. UC functions in `pc_insurance.dq` must exist

# COMMAND ----------

# DBTITLE 1,Setup & Configuration
# ============================================
# Setup & Imports
# ============================================
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.supervisoragents import (
    SupervisorAgent, Tool, GenieSpace, KnowledgeAssistant,
    UcFunction, Example
)

w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

# ============================================
# Configuration - UPDATE THESE IDs
# ============================================

# Replace with your actual IDs after running the prerequisite notebooks
KNOWLEDGE_ASSISTANT_ID = "<replace-with-your-knowledge-assistant-id>"
GENIE_SPACE_ANALYST_ID = "<replace-with-your-analyst-genie-space-id>"
GENIE_SPACE_INTERN_ID = "<replace-with-your-intern-genie-space-id>"  # Optional: reuse analyst or create separate

# Serving endpoint names (created by the agent notebooks)
ARCHITECT_ENDPOINT = "pc_architect_agent"
DATA_ENGINEER_ENDPOINT = "pc_data_engineer_agent"

# UC function for DQ
DQ_FUNCTION = "pc_insurance.dq.calculate_dq_score"

print("Configuration loaded. Update the IDs above before running.")

# COMMAND ----------

# DBTITLE 1,Create Supervisor Agent
# ============================================
# Create the Supervisor Agent
# ============================================

supervisor = SupervisorAgent(
    display_name="P&C Insurance Medallion Architecture Team",
    description="A multi-agent team that designs, develops, and operates a Medallion architecture for Property & Casualty (P&C) Insurance. Routes questions to the right specialist: Architect for design, Data Engineer for code, Domain Expert for insurance knowledge, Analyst for KPIs, QA for validation, and Intern for basic queries.",
    instructions="""You are the team lead for a virtual team building a Medallion architecture for a Property & Casualty (P&C) Insurance use case on Databricks.

## Your Team Members

1. **ARCHITECT** (Principal Data Architect) — Designs the overall Medallion architecture, defines Bronze/Silver/Gold layer schemas, data flow topology, and Unity Catalog governance. Route architecture and design questions here.

2. **DATA ENGINEER** (Senior Data Engineer) — Implements Bronze/Silver/Gold pipelines, writes Spark Declarative Pipeline (SDP) code, SQL transformations, and data quality expectations. Route code generation and pipeline implementation questions here.

3. **P&C DOMAIN EXPERT** (Insurance SME) — Answers questions about P&C insurance domain: policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements. Route insurance domain questions here.

4. **ANALYST** (Business Analyst) — Answers business questions by querying Gold layer tables: loss ratios, combined ratios, claim frequency/severity, retention rates, premium growth, exposure summaries. Route KPI and business metric questions here.

5. **QA VALIDATOR** (QA Engineer) — Runs data quality validation checks on the data. Route data quality, validation, and testing questions here.

6. **INTERN** (Data Analyst) — Handles basic data exploration, profiling, row counts, and simple queries. Route simple documentation and profiling requests here.

## Routing Rules

When a user asks a question, decompose it and route to the appropriate agent(s):

- Architecture and design questions → **ARCHITECT**
  Examples: "Design the Bronze layer", "What schema should I use for claims?", "How should I structure Unity Catalog?"

- Code and pipeline questions → **DATA ENGINEER**
  Examples: "Write the Silver MERGE for SCD2", "Create DQ expectations", "Generate the Gold SQL"

- Insurance domain questions → **P&C DOMAIN EXPERT**
  Examples: "What is a loss ratio?", "How does reserving work?", "What are the P&C lines of business?"

- KPI and business questions → **ANALYST**
  Examples: "What is our loss ratio by LOB?", "Show claim frequency by state", "Which agents have the best retention?"

- Data quality questions → **QA VALIDATOR**
  Examples: "Check data quality on the policies table", "Validate claim statuses"

- Simple queries → **INTERN**
  Examples: "How many policies do we have?", "What columns are in the claims table?"

- Complex questions that span multiple domains → Decompose and route to multiple agents, then synthesize the results into a single cohesive answer.

## Synthesis Rules

When multiple agents contribute:
1. Present the synthesized answer in a logical order (architecture first, then implementation, then domain context)
2. Cite which agent provided each part
3. Ensure consistency across agent responses
4. Add a summary at the end if the response is long
""",
)

created_supervisor = w.supervisor_agents.create_supervisor_agent(supervisor_agent=supervisor)
supervisor_name = created_supervisor.name
print(f"✓ Supervisor Agent created: {supervisor_name}")
print(f"  Display Name: {created_supervisor.display_name}")

# COMMAND ----------

# DBTITLE 1,Register Architect Subagent
# ============================================
# Register Subagent 1: ARCHITECT (Serving Endpoint)
# ============================================

architect_tool = Tool(
    tool_type="serving_endpoint",
    description="Designs Medallion architecture for P&C insurance. Defines Bronze/Silver/Gold layer schemas, data flow topology, Unity Catalog structure, governance policies, SCD2 strategies, and scalability patterns. Answers questions about architecture design, table schemas, and pipeline topology.",
)

# Note: serving_endpoint type uses a different spec format
# The Tool spec for serving_endpoint requires the endpoint name
architect_tool_dict = {
    "tool_type": "serving_endpoint",
    "description": "Designs Medallion architecture for P&C insurance. Defines Bronze/Silver/Gold layer schemas, data flow topology, Unity Catalog structure, governance policies, SCD2 strategies, and scalability patterns. Answers questions about architecture design, table schemas, and pipeline topology.",
}

try:
    created_architect = w.supervisor_agents.create_tool(
        parent=supervisor_name,
        tool=Tool(
            tool_type="serving_endpoint",
            description="Designs Medallion architecture for P&C insurance. Defines Bronze/Silver/Gold layer schemas, data flow topology, Unity Catalog structure, governance policies, SCD2 strategies, and scalability patterns.",
        ),
        tool_id="architect",
    )
    print(f"✓ Architect tool registered: {created_architect.name}")
except Exception as e:
    print(f"Architect registration note: {e}")
    print("The serving endpoint must exist before registering. Run Architect_Agent notebook first.")

# COMMAND ----------

# DBTITLE 1,Register Data Engineer Subagent
# ============================================
# Register Subagent 2: DATA ENGINEER (Serving Endpoint)
# ============================================

try:
    created_de = w.supervisor_agents.create_tool(
        parent=supervisor_name,
        tool=Tool(
            tool_type="serving_endpoint",
            description="Implements Bronze/Silver/Gold pipelines for P&C insurance data. Writes Spark Declarative Pipeline (SDP) code, SQL transformations, MERGE statements for SCD2, and data quality expectations. Generates Auto Loader code, PySpark transformations, and Delta Lake operations.",
        ),
        tool_id="data-engineer",
    )
    print(f"✓ Data Engineer tool registered: {created_de.name}")
except Exception as e:
    print(f"Data Engineer registration note: {e}")
    print("The serving endpoint must exist before registering. Run Data_Engineer_Agent notebook first.")

# COMMAND ----------

# DBTITLE 1,Register Domain Expert Subagent
# ============================================
# Register Subagent 3: P&C DOMAIN EXPERT (Knowledge Assistant)
# ============================================

if KNOWLEDGE_ASSISTANT_ID and KNOWLEDGE_ASSISTANT_ID != "<replace-with-your-knowledge-assistant-id>":
    try:
        created_domain = w.supervisor_agents.create_tool(
            parent=supervisor_name,
            tool=Tool(
                tool_type="knowledge_assistant",
                description="Answers questions about Property & Casualty insurance domain. Knows about policy lifecycle (quote/bind/issue/renew/cancel), claims processing (FNOL/investigation/reserve/settle), underwriting guidelines, loss ratios, combined ratios, frequency/severity, retention rates, reserving practices, NAIC regulatory requirements, and all P&C lines of business.",
                knowledge_assistant=KnowledgeAssistant(knowledge_assistant_id=KNOWLEDGE_ASSISTANT_ID),
            ),
            tool_id="pc-domain-expert",
        )
        print(f"✓ Domain Expert tool registered: {created_domain.name}")
    except Exception as e:
        print(f"Domain Expert registration note: {e}")
else:
    print("⚠ Knowledge Assistant ID not set. Run Domain_Expert_Setup notebook first.")
    print("  Then update KNOWLEDGE_ASSISTANT_ID in this notebook and re-run this cell.")

# COMMAND ----------

# DBTITLE 1,Register Analyst Subagent
# ============================================
# Register Subagent 4: ANALYST (Genie Space)
# ============================================

if GENIE_SPACE_ANALYST_ID and GENIE_SPACE_ANALYST_ID != "<replace-with-your-analyst-genie-space-id>":
    try:
        created_analyst = w.supervisor_agents.create_tool(
            parent=supervisor_name,
            tool=Tool(
                tool_type="genie_space",
                description="Answers business questions about P&C insurance KPIs by querying Gold layer tables. Provides loss ratio, combined ratio, claim frequency/severity, retention rate, premium growth, exposure summary, and executive dashboard metrics. Use for questions like 'What is our loss ratio by LOB?' or 'Show me retention by agent.'",
                genie_space=GenieSpace(id=GENIE_SPACE_ANALYST_ID),
            ),
            tool_id="analyst",
        )
        print(f"✓ Analyst tool registered: {created_analyst.name}")
    except Exception as e:
        print(f"Analyst registration note: {e}")
else:
    print("⚠ Genie Space ID not set. Run Analyst_Genie_Setup notebook first.")
    print("  Then update GENIE_SPACE_ANALYST_ID in this notebook and re-run this cell.")

# COMMAND ----------

# DBTITLE 1,Register QA Validator Subagent
# ============================================
# Register Subagent 5: QA VALIDATOR (UC Function)
# ============================================

try:
    created_qa = w.supervisor_agents.create_tool(
        parent=supervisor_name,
        tool=Tool(
            tool_type="uc_function",
            description="Runs data quality validation checks on P&C insurance data. Validates premium amounts are positive, claim statuses are valid, loss ratios are within bounds, policy IDs are not null, and date ordering is correct. Use for data quality and validation questions.",
            uc_function=UcFunction(name=DQ_FUNCTION),
        ),
        tool_id="qa-validator",
    )
    print(f"✓ QA Validator tool registered: {created_qa.name}")
except Exception as e:
    print(f"QA Validator registration note: {e}")
    print("Ensure UC functions in pc_insurance.dq schema exist.")

# COMMAND ----------

# DBTITLE 1,Register Intern Subagent
# ============================================
# Register Subagent 6: INTERN (Genie Space)
# ============================================
# The Intern can reuse the Analyst Genie Space or have its own
# For simplicity, we reuse the analyst Genie Space with a different description

if GENIE_SPACE_INTERN_ID and GENIE_SPACE_INTERN_ID != "<replace-with-your-intern-genie-space-id>":
    genie_id = GENIE_SPACE_INTERN_ID
elif GENIE_SPACE_ANALYST_ID and GENIE_SPACE_ANALYST_ID != "<replace-with-your-analyst-genie-space-id>":
    genie_id = GENIE_SPACE_ANALYST_ID  # reuse analyst space
else:
    genie_id = None

if genie_id:
    try:
        created_intern = w.supervisor_agents.create_tool(
            parent=supervisor_name,
            tool=Tool(
                tool_type="genie_space",
                description="Handles basic data exploration, profiling, and documentation queries. Counts rows, profiles columns, checks data distributions across Bronze and Silver layers. Use for simple questions like 'How many policies do we have?' or 'What is the distribution of claim statuses?'",
                genie_space=GenieSpace(id=genie_id),
            ),
            tool_id="intern",
        )
        print(f"✓ Intern tool registered: {created_intern.name}")
    except Exception as e:
        print(f"Intern registration note: {e}")
else:
    print("⚠ No Genie Space ID available for Intern. Skipping (optional).")

# COMMAND ----------

# DBTITLE 1,Add Quality Examples
# ============================================
# Add Quality Examples for Routing
# ============================================

examples = [
    {
        "question": "Design the Bronze layer for ingesting P&C policy data from our policy admin system",
        "guidelines": [
            "Route to the ARCHITECT agent for the schema design and ingestion approach",
            "Also route to the P&C DOMAIN EXPERT for policy data domain knowledge",
            "Synthesize both responses into a comprehensive Bronze layer design with table schemas"
        ]
    },
    {
        "question": "Write the Silver layer transformation for claims data with data quality checks",
        "guidelines": [
            "Route to the DATA ENGINEER for the transformation code (MERGE, dedup, SCD2)",
            "Route to the QA VALIDATOR for data quality expectations",
            "Combine into a complete Silver layer pipeline with DQ rules"
        ]
    },
    {
        "question": "What is our loss ratio by line of business for the latest quarter?",
        "guidelines": [
            "Route to the ANALYST agent which queries the Gold layer loss_ratio_by_lob table",
            "Format the answer as a table with line_of_business and loss_ratio columns"
        ]
    },
    {
        "question": "What is a combined ratio and how is it calculated?",
        "guidelines": [
            "Route to the P&C DOMAIN EXPERT for the definition and formula",
            "Provide the formula: Combined Ratio = (Incurred Losses + Expenses) / Earned Premium"
        ]
    },
    {
        "question": "How many claims do we have and what is the distribution of claim statuses?",
        "guidelines": [
            "Route to the INTERN agent for basic row count and status distribution",
            "Present the results as a simple summary table"
        ]
    },
    {
        "question": "Design and implement the complete Gold layer for P&C insurance KPIs",
        "guidelines": [
            "Route to the ARCHITECT for the Gold layer design and KPI definitions",
            "Route to the DATA ENGINEER for the SQL implementation code",
            "Route to the P&C DOMAIN EXPERT for KPI formulas and industry benchmarks",
            "Synthesize into a complete Gold layer design with code"
        ]
    },
]

for i, ex in enumerate(examples, 1):
    try:
        w.supervisor_agents.create_example(
            parent=supervisor_name,
            example=Example(
                question=ex["question"],
                guidelines=ex["guidelines"],
            ),
        )
        print(f"✓ Example {i} added: {ex['question'][:60]}...")
    except Exception as e:
        print(f"Example {i} note: {e}")

print(f"\n✓ Added {len(examples)} quality examples to the Supervisor Agent")

# COMMAND ----------

# DBTITLE 1,Verify Supervisor Agent
# ============================================
# Verify Supervisor Agent Configuration
# ============================================

print("=" * 60)
print("SUPERVISOR AGENT CONFIGURATION SUMMARY")
print("=" * 60)

# Get the supervisor agent
sa = w.supervisor_agents.get_supervisor_agent(name=supervisor_name)
print(f"\nDisplay Name: {sa.display_name}")
print(f"Description: {sa.description[:100]}...")
print(f"Resource Name: {sa.name}")

# List all tools (subagents)
print("\n--- Registered Subagents ---")
tool_count = 0
try:
    for tool in w.supervisor_agents.list_tools(parent=supervisor_name):
        print(f"  [{tool.tool_type}] {tool.name.split('/')[-1]}: {tool.description[:80]}...")
        tool_count += 1
except Exception as e:
    print(f"  Listing tools: {e}")

print(f"\nTotal subagents: {tool_count}")

# List examples
print("\n--- Quality Examples ---")
example_count = 0
try:
    for ex in w.supervisor_agents.list_examples(parent=supervisor_name):
        print(f"  Q: {ex.question[:80]}...")
        example_count += 1
except Exception as e:
    print(f"  Listing examples: {e}")

print(f"\nTotal examples: {example_count}")
print("\n" + "=" * 60)
print("✓ Supervisor Agent Setup Complete!")
print("=" * 60)
print(f"\nTo query the Supervisor Agent:")
print(f"  Use AI Playground or the Databricks SDK to query: {supervisor_name}")
print(f"  Or via the UI: Agents → P&C Insurance Medallion Architecture Team")