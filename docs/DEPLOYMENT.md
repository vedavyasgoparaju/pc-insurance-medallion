# P&C Insurance Medallion - Deployment Guide

## Overview

This document provides comprehensive deployment instructions for the P&C Insurance Medallion data platform on Databricks.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Pre-Deployment Checklist](#pre-deployment-checklist)
3. [Environment Setup](#environment-setup)
4. [Deployment Steps](#deployment-steps)
5. [Post-Deployment Validation](#post-deployment-validation)
6. [Rollback Procedures](#rollback-procedures)
7. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### 1. Databricks Workspace

**Required**:
- Databricks workspace (AWS, Azure, or GCP)
- Workspace URL and access token
- Unity Catalog enabled
- Databricks Runtime 13.3 LTS or higher

**Permissions Required**:
- Workspace Admin (for initial setup)
- Unity Catalog Admin (for catalog/schema creation)
- Cluster creation permissions
- Git repository access

### 2. Unity Catalog Setup

**Catalog Structure**:
```
pc_insurance (catalog)
├── bronze (schema)
├── silver (schema)
├── gold (schema)
├── reference (schema)
└── dq (schema)
```

### 3. Compute Resources

**Development Cluster**:
- Runtime: 13.3 LTS or higher
- Workers: 2-4 nodes
- Node Type: Standard_DS3_v2 (Azure) / m5.xlarge (AWS)
- Autoscaling: Enabled (2-8 nodes)

**Production Cluster**:
- Runtime: 13.3 LTS or higher
- Workers: 4-8 nodes
- Node Type: Standard_DS4_v2 (Azure) / m5.2xlarge (AWS)
- Autoscaling: Enabled (4-16 nodes)

---

## Pre-Deployment Checklist

### Planning Phase
- [ ] Review architecture documentation
- [ ] Review data dictionary
- [ ] Identify deployment environment (dev/staging/prod)
- [ ] Confirm deployment window
- [ ] Prepare rollback plan

### Access & Permissions
- [ ] Databricks workspace access confirmed
- [ ] Unity Catalog admin access confirmed
- [ ] Git repository access confirmed
- [ ] Cloud storage access confirmed

### Infrastructure
- [ ] Unity Catalog metastore attached
- [ ] Storage location configured
- [ ] Compute clusters created/configured
- [ ] Network connectivity verified
- [ ] Git repository cloned to workspace

---

## Environment Setup

### Step 1: Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Verify connection
databricks workspace ls /
```

### Step 2: Create Unity Catalog Resources

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS pc_insurance
COMMENT 'P&C Insurance Medallion Architecture';

-- Create schemas
CREATE SCHEMA IF NOT EXISTS pc_insurance.bronze;
CREATE SCHEMA IF NOT EXISTS pc_insurance.silver;
CREATE SCHEMA IF NOT EXISTS pc_insurance.gold;
CREATE SCHEMA IF NOT EXISTS pc_insurance.reference;
CREATE SCHEMA IF NOT EXISTS pc_insurance.dq;

-- Grant permissions (replace with your service principal)
GRANT USE CATALOG ON CATALOG pc_insurance TO `your-service-principal`;
GRANT CREATE TABLE ON SCHEMA pc_insurance.bronze TO `your-service-principal`;
-- Repeat for all schemas

-- Verify
SHOW SCHEMAS IN pc_insurance;
```

### Step 3: Create Unity Catalog Volume

```sql
-- Create volume for P&C domain documents
CREATE VOLUME IF NOT EXISTS pc_insurance.reference.pc_domain_docs;

-- Grant access
GRANT READ VOLUME ON VOLUME pc_insurance.reference.pc_domain_docs TO `your-service-principal`;
```

### Step 4: Clone Git Repository

**Using Databricks UI**:
1. Navigate to Workspace → Repos
2. Click "Add Repo"
3. Enter Git URL: `https://github.com/vedavyasgoparaju/pc-insurance-medallion.git`
4. Select branch: `main`
5. Click "Create Repo"

---

## Deployment Steps

### Phase 1: Database Schema Creation (15 minutes)

#### Step 1.1: Create Bronze Tables

```sql
-- Run SQL scripts in order
%run /Repos/your-username/pc-insurance-medallion/sql/01_catalog_schemas.sql
%run /Repos/your-username/pc-insurance-medallion/sql/02_bronze_tables.sql
```

**Validation**:
```sql
SHOW TABLES IN pc_insurance.bronze;
-- Expected: policies_raw, claims_raw, premiums_raw, customers_raw, agents_raw
```

#### Step 1.2: Create Silver Metadata Configuration

```sql
%run /Repos/your-username/pc-insurance-medallion/sql/03_silver_transformation_config.sql
```

**Validation**:
```sql
SELECT transformation_id, transformation_name, is_active
FROM pc_insurance.reference.silver_transformation_config
ORDER BY execution_order;
-- Expected: 6 active transformations
```

#### Step 1.3: Create Gold Tables and Metadata

```sql
%run /Repos/your-username/pc-insurance-medallion/sql/04_gold_tables.sql
%run /Repos/your-username/pc-insurance-medallion/sql/05_gold_metric_config.sql
```

**Validation**:
```sql
SELECT metric_id, metric_name, is_active
FROM pc_insurance.reference.gold_metric_config
ORDER BY execution_order;
-- Expected: 6 active metrics
```

### Phase 2: Agent Setup (20 minutes)

#### Step 2.1: Upload Domain Documents

```python
# Upload P&C domain documents to Unity Catalog volume
dbutils.fs.cp(
  "file:/path/to/local/domain_docs/",
  "/Volumes/pc_insurance/reference/pc_domain_docs/",
  recurse=True
)
```

#### Step 2.2: Test Agent Notebooks

```python
# Test Domain Expert Agent
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/agents/Domain_Expert_Agent",
  timeout_seconds=300
)
print(result)

# Test Analyst Agent
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/agents/Analyst_Genie_Agent",
  timeout_seconds=300
)
print(result)
```

### Phase 3: Pipeline Deployment (30 minutes)

#### Step 3.1: Run Bronze Pipeline

```python
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/pipelines/Bronze_Pipeline",
  timeout_seconds=1800
)
print(f"Bronze Pipeline Result: {result}")
```

**Validation**:
```sql
SELECT 'policies_raw' AS table_name, COUNT(*) AS row_count
FROM pc_insurance.bronze.policies_raw
UNION ALL
SELECT 'claims_raw', COUNT(*) FROM pc_insurance.bronze.claims_raw;
-- Expected: ~1000 policies, ~300 claims
```

#### Step 3.2: Run Silver Pipeline

```python
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/pipelines/Silver_Pipeline_Metadata",
  timeout_seconds=3600
)
print(f"Silver Pipeline Result: {result}")
```

**Validation**:
```sql
-- Check Silver tables
SELECT 'policy_dim' AS table_name, COUNT(*) AS row_count
FROM pc_insurance.silver.policy_dim WHERE is_current = TRUE;

-- Check audit logs
SELECT transformation_id, status, execution_time_seconds
FROM pc_insurance.reference.silver_load_audit
ORDER BY run_timestamp DESC LIMIT 10;
-- All should have status = 'SUCCESS'
```

#### Step 3.3: Run Gold Pipeline

```python
result = dbutils.notebook.run(
  "/Repos/your-username/pc-insurance-medallion/pipelines/Gold_Pipeline",
  timeout_seconds=1800
)
print(f"Gold Pipeline Result: {result}")
```

**Validation**:
```sql
-- Verify Gold tables
SELECT line_of_business, loss_ratio, claim_count
FROM pc_insurance.gold.loss_ratio_by_lob
ORDER BY loss_ratio DESC;
```

### Phase 4: Job Orchestration Setup (20 minutes)

#### Deploy Databricks Asset Bundle

```bash
cd /path/to/local/pc-insurance-medallion

# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy -t dev

# Deploy to production
databricks bundle deploy -t prod
```

#### Or Create Scheduled Job Manually

1. Navigate to Workflows → Create Job
2. Add tasks: Bronze → Silver → Gold
3. Set schedule: Daily at 2:00 AM UTC
4. Configure notifications

### Phase 4b: MCP App Deployment (10 minutes)

#### Deploy `pc-insurance-workspace-actions`

The MCP app (`app/app.py`) provides git commit, file write, and SQL execution capabilities to the Supervisor Agent.

```bash
# Deploy via Databricks CLI
databricks apps deploy pc-insurance-workspace-actions \
  --source-file app/app.py
```

Or via the Databricks UI:
1. Navigate to Apps → Create App
2. Name: `pc-insurance-workspace-actions`
3. Source: `app/app.py`
4. Config: `app/app.yaml`
5. Requirements: `app/requirements.txt`
6. Click Deploy

#### Register as Supervisor Agent Tool

Add the MCP app as the 8th tool in the Supervisor Agent configuration:
- **Tool ID**: `workspace-actions`
- **Type**: MCP server
- **Description**: Executes workspace changes (git commits, file writes, SQL execution)

### Phase 5: Monitoring Setup (15 minutes)

#### Configure SQL Alerts

```sql
-- DQ Alert
CREATE ALERT pc_insurance_dq_alert
ON SCHEDULE CRON '0 8 * * *'
AS
SELECT table_name, 
       AVG(CASE WHEN validation_result = 'PASS' THEN 1.0 ELSE 0.0 END) AS dq_score
FROM pc_insurance.dq.dq_validation_results
WHERE DATE(validation_timestamp) = CURRENT_DATE() - 1
GROUP BY table_name
HAVING dq_score < 0.95;
```

---

## Post-Deployment Validation

### Validation Checklist

- [ ] Unity Catalog catalog and schemas exist
- [ ] Bronze tables created and populated
- [ ] Silver tables created with SCD2 tracking
- [ ] Gold tables created with metrics
- [ ] Metadata configuration tables populated
- [ ] Agent notebooks execute successfully
- [ ] Scheduled job created and runs successfully
- [ ] Monitoring alerts configured

### Smoke Tests

#### Test 1: End-to-End Pipeline

```python
print("Running Bronze...")
bronze_result = dbutils.notebook.run("/Repos/.../pipelines/Bronze_Pipeline", 1800)
print("Running Silver...")
silver_result = dbutils.notebook.run("/Repos/.../pipelines/Silver_Pipeline_Metadata", 3600)
print("Running Gold...")
gold_result = dbutils.notebook.run("/Repos/.../pipelines/Gold_Pipeline", 1800)
print("✓ End-to-end pipeline completed!")
```

#### Test 2: Data Quality

```sql
SELECT table_name, 
       COUNT(*) AS total_checks,
       ROUND(100.0 * SUM(CASE WHEN validation_result = 'PASS' THEN 1 ELSE 0 END) / COUNT(*), 2) AS pass_rate
FROM pc_insurance.dq.dq_validation_results
WHERE DATE(validation_timestamp) = CURRENT_DATE()
GROUP BY table_name;
-- Expected: pass_rate > 95%
```

---

## Rollback Procedures

### Scenario 1: Pipeline Failure

```sql
-- Restore table to previous version
RESTORE TABLE pc_insurance.silver.policy_dim TO VERSION AS OF <version>;
```

### Scenario 2: Configuration Error

```sql
-- Restore metadata configuration
RESTORE TABLE pc_insurance.reference.silver_transformation_config 
TO TIMESTAMP AS OF '<timestamp>';
```

---

## Troubleshooting

### Issue 1: Unity Catalog Permissions

**Error**: `PERMISSION_DENIED`

**Solution**:
```sql
GRANT USE CATALOG ON CATALOG pc_insurance TO `user@company.com`;
```

### Issue 2: Pipeline Timeout

**Solution**:
```python
# Increase timeout
dbutils.notebook.run(notebook_path, timeout_seconds=7200)
```

---

## Deployment Timeline

| Phase | Duration |
|-------|----------|
| Prerequisites | 1-2 days |
| Environment Setup | 2-4 hours |
| Schema Creation | 15 minutes |
| Agent Setup | 20 minutes |
| MCP App Deployment | 10 minutes |
| Pipeline Deployment | 30 minutes |
| Job Orchestration | 20 minutes |
| Monitoring Setup | 15 minutes |
| Validation | 30 minutes |
| **Total** | **~2.5 hours** (after prerequisites) |

---

## Deployment Sign-Off

### Pre-Deployment Approval
- [ ] Deployment plan reviewed by: _____________________ Date: _______
- [ ] Infrastructure ready: _____________________ Date: _______

### Post-Deployment Approval
- [ ] Deployment completed: _____________________ Date: _______
- [ ] Validation passed: _____________________ Date: _______

---

**Document Version**: 2.0  
**Last Updated**: 2026-09-25  
**Owner**: Data Engineering Team
