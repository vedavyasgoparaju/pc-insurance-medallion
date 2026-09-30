-- Migration: Remove Data Engineer Agent References
-- Date: 2026-09-30
-- Description: Remove DOC-029 (Data Engineer Agent) and update DOC-044 to remove Data Engineer references
--              after introducing the Unified Architect agent that handles both architecture and implementation.
-- Status: EXECUTED on 2026-09-30

-- Delete DOC-029 (Data Engineer Agent entry)
DELETE FROM pc_insurance.reference.project_documentation 
WHERE doc_id = 'DOC-029';

-- Update DOC-044 to remove reference to Data Engineer agent
UPDATE pc_insurance.reference.project_documentation 
SET description = 'The DevOps Agent is implemented as an MLflow serving endpoint (pc_devops_agent) following the same pattern as the Architect agent. Provides guidance on Git operations, CI/CD pipelines, Databricks Repos, DAB deployment, and version control best practices.'
WHERE doc_id = 'DOC-044';
