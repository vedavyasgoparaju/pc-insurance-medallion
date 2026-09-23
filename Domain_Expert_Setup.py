# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Domain Expert Setup - P&C Knowledge Assistant
# MAGIC %md
# MAGIC # Domain Expert Setup - P&C Insurance Knowledge Assistant
# MAGIC
# MAGIC This notebook sets up a **Knowledge Assistant** (document-grounded RAG agent) with P&C insurance domain knowledge.
# MAGIC
# MAGIC The Knowledge Assistant is created using the Databricks SDK and is populated with reference documents covering:
# MAGIC * Policy lifecycle and underwriting guidelines
# MAGIC * Claims processing and reserving practices
# MAGIC * P&C insurance regulatory requirements (NAIC)
# MAGIC * Key insurance metrics and their definitions
# MAGIC * Lines of business terminology
# MAGIC
# MAGIC This agent will be attached as a subagent to the Supervisor Agent.

# COMMAND ----------

# DBTITLE 1,Setup
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print(f"Workspace: {w.config.host}")

# COMMAND ----------

# DBTITLE 1,Create UC Volume for Domain Documents
# ============================================
# Create a UC Volume for P&C Insurance reference documents
# ============================================

# Create volume for storing domain documents
spark.sql("""
CREATE VOLUME IF NOT EXISTS pc_insurance.reference.pc_domain_docs
  COMMENT 'Volume for P&C insurance domain reference documents for Knowledge Assistant'
""")

print("✓ Volume created: pc_insurance.reference.pc_domain_docs")
print("Upload the following document types to this volume:")
print("  - Policy forms and underwriting guidelines (PDF)")
print("  - Claims handling manuals (PDF)")
print("  - NAIC regulatory guidelines (PDF)")
print("  - Insurance terminology glossaries (PDF/TXT)")
print("  - Rate filing documentation (PDF)")

# COMMAND ----------

# DBTITLE 1,Generate P&C Reference Document
# ============================================
# Generate P&C Insurance Reference Document
# ============================================

# Create a comprehensive P&C insurance reference document
pc_reference_doc = """# Property & Casualty Insurance Reference Guide

## 1. Policy Lifecycle

### Quote to Bind
- **Quote**: Initial premium estimate based on underwriting factors
- **Bind**: Insurer commits to providing coverage at quoted terms
- **Issue**: Policy document generated and sent to insured
- **Effective**: Coverage begins on effective date

### Policy Types
- **Personal Auto**: Liability, collision, comprehensive, PIP, uninsured motorist
- **Homeowners**: Dwelling, other structures, personal property, liability
- **Commercial Property**: Building, business personal property, business income
- **General Liability**: Bodily injury, property damage, personal/advertising injury
- **Workers Compensation**: Statutory coverage for employee injuries

### Policy Status Values
- Active: Policy in force, premiums current
- Expired: Policy term ended, not renewed
- Cancelled: Policy terminated before expiry (non-pay, underwriting, request)
- Lapsed: Premium not paid, coverage suspended

## 2. Claims Lifecycle

### FNOL (First Notice of Loss)
- Claim reported by insured or agent
- Assign to claims adjuster
- Create claim record with initial details

### Investigation
- Gather facts: police reports, photos, witness statements
- Determine coverage applicability
- Assess liability (for liability claims)
- Evaluate damages

### Reserving
- **Initial Reserve**: Estimate of ultimate claim cost
- **Adjust Reserve**: Update as more information becomes available
- **Bulk Reserve**: Group reserves for similar claims
- Reserve types: Indemnity (payment to claimant) + ALAE (Adjusting expenses)

### Settlement & Closure
- Negotiate settlement
- Issue payment
- Close claim (may reopen if new information)
- Subrogation: Recover from responsible third party

### Claim Status Values
- Open: Active claim under investigation
- Closed: Claim resolved, no further action
- Reopened: Previously closed claim reopened
- Denied: Claim not covered under policy
- Pending: Awaiting documentation or decision

## 3. Key Metrics & Formulas

### Loss Ratio
- **Formula**: Incurred Losses / Earned Premium
- **Interpretation**: <60% = good, 60-70% = acceptable, >70% = concerning
- Incurred Losses = Paid Losses + Reserves

### Combined Ratio
- **Formula**: (Incurred Losses + Expenses) / Earned Premium
- **Components**: Loss Ratio + Expense Ratio
- **Interpretation**: <100% = underwriting profit, >100% = underwriting loss
- Expense Ratio = Underwriting Expenses / Earned Premium

### Claim Frequency
- **Formula**: Claim Count / Exposure Units (policy-years)
- **Interpretation**: Measures how often claims occur per unit of exposure

### Claim Severity
- **Formula**: Incurred Losses / Claim Count
- **Interpretation**: Average cost per claim

### Retention Rate
- **Formula**: Renewed Policies / (Renewed + Cancelled)
- **Target**: >90% for personal lines, >85% for commercial lines

### Earned vs Written Premium
- **Written Premium**: Total premium for policies written in a period
- **Earned Premium**: Portion of written premium earned over time
- **Unearned Premium**: Portion not yet earned (pro-rata by time)

## 4. Lines of Business

### Personal Lines
- Personal Auto, Homeowners, Renters, Umbrella, Watercraft

### Commercial Lines
- Commercial Auto, Commercial Property, General Liability, Workers Comp,
  Professional Liability (E&O), Cyber, Commercial Umbrella

## 5. Regulatory Framework

### NAIC (National Association of Insurance Commissioners)
- Sets model laws and regulations
- State-by-state implementation
- Annual statement reporting
- Solvency requirements

### Rate Filing
- Rates must be filed with state DOI (Department of Insurance)
- Some states require prior approval, others file-and-use
- Loss costs filed by rating bureaus (e.g., NCCI for Workers Comp)

### Data Security
- PII protection (customer SSN, DOB, address)
- GLBA (Gramm-Leach-Bliley Act) compliance
- State privacy laws (CCPA, etc.)

## 6. Data Quality Considerations

### Policy Data
- policy_id must be unique and non-null
- effective_date must be <= expiry_date
- premium_amount should be > 0
- state must be valid US state code

### Claim Data
- claim_id must be unique
- policy_id must exist in policies table (referential integrity)
- claim_status must be valid (Open/Closed/Reopened/Denied/Pending)
- loss_date must be >= policy effective_date
- incurred_loss >= paid_loss (typically)

### Premium Data
- transaction_id must be unique
- written_premium >= earned_premium (within a transaction)
- billing_status must be valid (Paid/Pending/Overdue)
"""

# Write the reference document to the volume
volume_path = "/Volumes/pc_insurance/reference/pc_domain_docs/pc_insurance_reference_guide.txt"

dbutils.fs.put(volume_path, pc_reference_doc, overwrite=True)

print(f"✓ P&C reference document written to: {volume_path}")
print(f"Document size: {len(pc_reference_doc)} characters")

# COMMAND ----------

# DBTITLE 1,Create Knowledge Assistant
# ============================================
# Create Knowledge Assistant via Databricks SDK
# ============================================
#
# Note: The Knowledge Assistant API may require the AI Generative Intelligence
# platform. If the SDK method is not available, create the Knowledge Assistant
# from the Databricks UI:
#
#   1. Go to Agents → Create Agent → Knowledge Assistant
#   2. Name: pc_domain_expert
#   3. Description: P&C Insurance domain expert for policy, claims, and underwriting
#   4. Knowledge Source: UC Volume pc_insurance.reference.pc_domain_docs
#   5. Save and wait for indexing to complete
#
# The Knowledge Assistant ID will be needed for the Supervisor Agent setup.

try:
    # Attempt to create via SDK (if available)
    from databricks.sdk.service.genai import CreateKnowledgeAssistantRequest
    
    ka = w.knowledge_assistants.create(
        name="pc_domain_expert",
        description="P&C Insurance domain expert. Answers questions about policy lifecycle, claims processing, underwriting, reserving, loss ratios, combined ratios, frequency/severity, retention, and regulatory requirements.",
    )
    
    print(f"✓ Knowledge Assistant created: {ka.name}")
    knowledge_assistant_id = ka.name
    
except Exception as e:
    print(f"Knowledge Assistant creation via SDK not available or failed: {e}")
    print("\n📋 Please create manually from the Databricks UI:")
    print("   Agents → Create Agent → Knowledge Assistant")
    print("   Name: pc_domain_expert")
    print("   Knowledge Source: pc_insurance.reference.pc_domain_docs (UC Volume)")
    print("\n   After creation, note the Knowledge Assistant ID for the Supervisor Agent setup.")
    knowledge_assistant_id = "<replace-with-your-knowledge-assistant-id>"

# COMMAND ----------

# DBTITLE 1,Summary
# ============================================
# Store Knowledge Assistant ID for Supervisor Agent
# ============================================

# Save the Knowledge Assistant ID to a file for use by the Supervisor Agent notebook
print(f"Knowledge Assistant ID: {knowledge_assistant_id}")
print("\n✓ Domain Expert Setup Complete")
print("\nNext: Run the Supervisor_Agent_Setup notebook to register this as a subagent.")