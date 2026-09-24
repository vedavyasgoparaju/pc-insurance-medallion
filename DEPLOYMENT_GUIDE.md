# P&C Insurance Medallion - Deployment Guide

**Version**: 1.0  
**Date**: 2026-09-24  
**Status**: Ready for Deployment

## Overview

This guide provides step-by-step instructions to deploy and configure the P&C Insurance Medallion architecture with proper access controls for all agents.

## Current Issues Resolved

### Issue 1: Agent Access Permissions
**Problem**: ANALYST agent only had access to Gold layer, preventing cross-layer validation and metric tracing.

**Solution**: Implemented comprehensive Unity Catalog permissions granting read access to all layers for all agents based on their roles.

### Issue 2: End-to-End Execution
**Problem**: No automated way to generate fresh data and execute the complete pipeline.

**Solution**: Created safe, non-destructive end-to-end execution notebook.

## Deployment Steps

### Step 1: Review Access Control Plan

📄 **File**: `/Workspace/Shared/pc_insurance/ACCESS_CONTROL_PLAN.md`

Review the access control model defining permissions for each role:
- **Data Engineers**: Full access to all layers
- **Analysts**: Read access to Bronze, Silver, Gold, Reference
- **QA Validators**: Read access + EXECUTE on DQ functions
- **Architects**: Read access for validation
- **DevOps**: Read access to metadata

### Step 2: Execute Unity Catalog Grants

**⚠️ Requires Unity Catalog Admin Privileges**

Execute the following SQL commands as a UC admin:

```sql
-- Bronze layer access
GRANT USAGE ON SCHEMA pc_insurance.bronze TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.bronze TO `account users`;

-- Silver layer access
GRANT USAGE ON SCHEMA pc_insurance.silver TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.silver TO `account users`;

-- Gold layer access (should already exist)
GRANT USAGE ON SCHEMA pc_insurance.gold TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.gold TO `account users`;

-- Reference layer access
GRANT USAGE ON SCHEMA pc_insurance.reference TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.reference TO `account users`;

-- DQ layer access
GRANT USAGE ON SCHEMA pc_insurance.dq TO `account users`;
```

**Verification**:
```sql
-- Verify grants
SHOW GRANTS ON SCHEMA pc_insurance.bronze;
SHOW GRANTS ON SCHEMA pc_insurance.silver;
SHOW GRANTS ON SCHEMA pc_insurance.gold;
```

### Step 3: Update Genie Space Configurations

#### 3.1 Update Analyst Genie Space

1. Navigate to **Databricks Genie** → **Spaces**
2. Select **pc_analyst_genie**
3. Click **Edit Space**
4. Under **Data Sources**, add:
   - `pc_insurance.bronze` (all tables)
   - `pc_insurance.silver` (all tables)
   - Keep existing `pc_insurance.gold` tables
5. Update **Instructions** to include:
   ```
   You have access to Bronze (raw), Silver (cleansed), and Gold (aggregated) layers.
   You can query across layers to validate metrics and trace data lineage.
   
   Example cross-layer query:
   SELECT 
     g.line_of_business,
     g.loss_ratio,
     COUNT(DISTINCT s.policy_id) as silver_policies,
     COUNT(DISTINCT b.policy_id) as bronze_policies
   FROM pc_insurance.gold.loss_ratio_by_lob g
   LEFT JOIN pc_insurance.silver.policy_dim s ON g.line_of_business = s.line_of_business
   LEFT JOIN pc_insurance.bronze.policies_raw b ON s.policy_id = b.policy_id
   GROUP BY g.line_of_business, g.loss_ratio;
   ```
6. **Save** and **Test** with a cross-layer query

#### 3.2 Update Other Genie Spaces

Repeat similar steps for:
- **pc_devops_genie**: Add reference layer access
- **pc_documentation_genie**: Add all layers for complete documentation

### Step 4: Test Access Permissions

Run the verification notebook or execute these test queries:

```sql
-- Test 1: Bronze access
SELECT 'Bronze Access' as test, COUNT(*) as row_count 
FROM pc_insurance.bronze.agents_raw;

-- Test 2: Silver access
SELECT 'Silver Access' as test, COUNT(*) as row_count 
FROM pc_insurance.silver.agent_dim;

-- Test 3: Gold access
SELECT 'Gold Access' as test, COUNT(*) as row_count 
FROM pc_insurance.gold.loss_ratio_by_lob;

-- Test 4: Cross-layer join
SELECT 'Cross-Layer Join' as test, COUNT(*) as row_count
FROM pc_insurance.bronze.policies_raw b
INNER JOIN pc_insurance.silver.policy_dim s ON b.policy_id = s.policy_id
INNER JOIN pc_insurance.gold.loss_ratio_by_lob g ON s.line_of_business = g.line_of_business;
```

Expected results: All queries should return row counts without permission errors.

### Step 5: Execute End-to-End Pipeline

📓 **Notebook**: `/Workspace/Shared/pc_insurance/pipelines/00_END_TO_END_SAFE`

This notebook generates fresh synthetic data for testing:

1. Open the notebook in Databricks
2. Attach to an appropriate cluster (or use serverless)
3. Review the configuration in the first cells
4. **Run All** cells
5. Verify output:
   - Bronze: 5 temp views created
   - Data preview tables show distributions
   - No destructive operations performed

**What it does**:
- Generates 100 agents
- Generates 1,000 customers
- Generates 2,000 policies
- Generates 5,000 premium transactions
- Generates 500 claims
- Creates temporary views (non-destructive)
- Provides data quality previews

**What it does NOT do**:
- Does NOT overwrite existing tables
- Does NOT modify production data
- Safe for testing and validation

### Step 6: Validate Agent Access

Test each agent's access to their required layers:

#### Test Analyst Agent
```python
# Use the analyst tool to query Bronze layer
analyst.query("Show me the count of policies by line of business from the Bronze layer")

# Expected: Should return results from pc_insurance.bronze.policies_raw
```

#### Test QA Validator
```python
# Use QA tool to run DQ checks
qa_validator.run_checks("pc_insurance.silver.policy_dim")

# Expected: Should execute DQ functions and return validation results
```

#### Test Data Engineer
```python
# Data Engineer should be able to query all layers
data_engineer.query("Show me the complete data flow from Bronze policies to Gold loss ratios")

# Expected: Should return lineage and transformation logic
```

### Step 7: Update Documentation

Run the documentation agent to update all docs:

```python
documentation.update("Update all documentation to reflect the new access control model and end-to-end execution capabilities")
```

Expected updates:
- README.md: Add access control section
- Architecture Guide: Add security model
- Data Dictionary: Include all layers
- Runbooks: Add permission verification steps

### Step 8: Commit Changes to Git

Use the DevOps agent for guidance, then execute via workspace-actions:

```python
# Get Git guidance
devops.advise("What's the best practice for committing these access control and pipeline changes?")

# Execute commit
workspace_actions.git_commit(
    commit_message="feat: Add comprehensive access control and end-to-end execution pipeline\n\n- Grant Bronze/Silver access to Analyst agent\n- Create safe end-to-end execution notebook\n- Update Genie space configurations\n- Add deployment guide and access control plan"
)
```

## Verification Checklist

Use this checklist to verify successful deployment:

- [ ] Unity Catalog grants executed successfully
- [ ] `SHOW GRANTS` confirms permissions on all schemas
- [ ] Analyst Genie space updated with Bronze/Silver schemas
- [ ] Test queries from each layer execute without errors
- [ ] Cross-layer joins work correctly
- [ ] End-to-end notebook runs successfully
- [ ] All agents can access their required layers
- [ ] Documentation updated
- [ ] Changes committed to Git

## Rollback Plan

If issues occur, rollback using these steps:

### Rollback Step 1: Revoke Permissions
```sql
-- Revoke Bronze access
REVOKE SELECT ON SCHEMA pc_insurance.bronze FROM `account users`;

-- Revoke Silver access
REVOKE SELECT ON SCHEMA pc_insurance.silver FROM `account users`;

-- Gold access remains unchanged
```

### Rollback Step 2: Revert Genie Spaces
1. Navigate to each Genie space
2. Remove Bronze and Silver schemas from data sources
3. Restore previous instructions

### Rollback Step 3: Git Revert
```bash
git revert <commit_hash>
git push origin main
```

## Monitoring & Maintenance

### Daily Checks
- Monitor failed access attempts in Unity Catalog audit logs
- Check Genie space query success rates
- Verify end-to-end pipeline execution logs

### Weekly Reviews
- Review access patterns by agent/role
- Audit new users and permissions
- Check for unused or excessive permissions

### Monthly Tasks
- Full access control audit
- Update documentation for any schema changes
- Review and optimize cross-layer queries

## Troubleshooting

### Issue: "Permission Denied" errors

**Cause**: Grants not applied or user not in `account users` group

**Solution**:
```sql
-- Check current user's groups
SELECT current_user();

-- Verify grants
SHOW GRANTS ON SCHEMA pc_insurance.bronze;

-- Re-apply grants if needed
GRANT SELECT ON SCHEMA pc_insurance.bronze TO `account users`;
```

### Issue: Genie space doesn't see new schemas

**Cause**: Genie space not refreshed after adding schemas

**Solution**:
1. Edit the Genie space
2. Remove and re-add the schemas
3. Click "Refresh Schema" button
4. Save changes
5. Wait 1-2 minutes for cache refresh

### Issue: End-to-end notebook fails

**Cause**: Cluster permissions or Unity Catalog access

**Solution**:
1. Verify cluster has Unity Catalog enabled
2. Check cluster access mode (Shared or Single User)
3. Verify user has CREATE TABLE permissions (if persisting data)
4. Review notebook execution logs for specific errors

## Support & Contacts

- **Architecture Questions**: Contact Architect agent or team lead
- **Access Issues**: Contact Unity Catalog admin
- **Pipeline Issues**: Contact Data Engineering team
- **Documentation**: Contact Technical Writer or use Documentation agent

## References

- [Unity Catalog Permissions](https://docs.databricks.com/data-governance/unity-catalog/manage-privileges/privileges.html)
- [Genie Spaces Configuration](https://docs.databricks.com/genie/index.html)
- [Medallion Architecture Best Practices](https://www.databricks.com/glossary/medallion-architecture)

---

**Deployment Status**: ✅ Ready  
**Last Updated**: 2026-09-24  
**Owner**: P&C Insurance Medallion Team
