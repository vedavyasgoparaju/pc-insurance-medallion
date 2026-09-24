# P&C Insurance Medallion - Operations Runbook

## Overview

This runbook provides operational procedures for running, monitoring, and troubleshooting the P&C Insurance Medallion data platform.

---

## Table of Contents

1. [Daily Operations](#daily-operations)
2. [Pipeline Execution](#pipeline-execution)
3. [Monitoring & Alerts](#monitoring--alerts)
4. [Troubleshooting](#troubleshooting)
5. [Data Quality Checks](#data-quality-checks)
6. [Incident Response](#incident-response)
7. [Maintenance Procedures](#maintenance-procedures)
8. [Emergency Contacts](#emergency-contacts)

---

## Daily Operations

### Morning Checklist

1. **Check Pipeline Status**
   ```sql
   -- Check last run status
   SELECT 
     transformation_id,
     run_timestamp,
     status,
     execution_time_seconds
   FROM pc_insurance.reference.silver_load_audit
   WHERE DATE(run_timestamp) = CURRENT_DATE()
   ORDER BY run_timestamp DESC;
   ```

2. **Verify Data Freshness**
   ```sql
   -- Check latest data in Bronze layer
   SELECT 
     'policies_raw' AS table_name,
     MAX(ingestion_timestamp) AS latest_data
   FROM pc_insurance.bronze.policies_raw
   UNION ALL
   SELECT 
     'claims_raw',
     MAX(ingestion_timestamp)
   FROM pc_insurance.bronze.claims_raw;
   ```

3. **Review Data Quality Scores**
   ```sql
   -- Check DQ scores for yesterday
   SELECT 
     table_name,
     AVG(CASE WHEN validation_result = 'PASS' THEN 1.0 ELSE 0.0 END) AS dq_score
   FROM pc_insurance.dq.dq_validation_results
   WHERE DATE(validation_timestamp) = CURRENT_DATE() - 1
   GROUP BY table_name;
   ```

4. **Check Reconciliation Status**
   ```sql
   -- Check Silver reconciliation
   SELECT 
     transformation_id,
     recon_status,
     count_diff
   FROM pc_insurance.reference.silver_reconciliation
   WHERE DATE(recon_timestamp) = CURRENT_DATE()
   AND recon_status != 'PASS';
   ```

---

## Pipeline Execution

### Manual Pipeline Execution

#### Bronze Layer

```python
# Run Bronze pipeline
dbutils.notebook.run(
  "/Repos/vedavyas.goparaju/pc-insurance-medallion/pipelines/Bronze_Pipeline",
  timeout_seconds=3600,
  arguments={}
)
```

#### Silver Layer

```python
# Run Silver pipeline (metadata-driven)
dbutils.notebook.run(
  "/Repos/vedavyas.goparaju/pc-insurance-medallion/pipelines/Silver_Pipeline_Metadata",
  timeout_seconds=7200,
  arguments={}
)
```

#### Gold Layer

```python
# Run Gold pipeline
dbutils.notebook.run(
  "/Repos/vedavyas.goparaju/pc-insurance-medallion/pipelines/Gold_Pipeline",
  timeout_seconds=3600,
  arguments={}
)
```

### Scheduled Job Execution

**Job Name**: `PC_Insurance_MultiAgent_Pipeline`  
**Job ID**: `820361677269451`  
**Schedule**: Daily at 2:00 AM UTC

**Check Job Status**:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

job_id = 820361677269451
runs = w.jobs.list_runs(job_id=job_id, limit=10)

for run in runs:
    print(f"Run ID: {run.run_id}")
    print(f"Status: {run.state.life_cycle_state}")
    print(f"Start Time: {run.start_time}")
    print(f"End Time: {run.end_time}")
    print("-" * 80)
```

### Orchestrator Execution

```python
# Run full orchestrated pipeline
dbutils.notebook.run(
  "/Repos/vedavyas.goparaju/pc-insurance-medallion/pipelines/Orchestrator",
  timeout_seconds=10800,
  arguments={
    "run_mode": "full",
    "layers": "bronze,silver,gold"
  }
)
```

---

## Monitoring & Alerts

### Key Metrics to Monitor

1. **Pipeline Execution Time**
   - Bronze: < 30 minutes
   - Silver: < 60 minutes
   - Gold: < 30 minutes

2. **Data Quality Score**
   - Target: > 95%
   - Warning: < 95%
   - Critical: < 90%

3. **Reconciliation Status**
   - Target: 100% PASS
   - Warning: Any FAIL status

4. **Row Counts**
   - Monitor for unexpected drops or spikes

### Monitoring Queries

```sql
-- Pipeline execution time trend
SELECT 
  DATE(run_timestamp) AS run_date,
  transformation_id,
  AVG(execution_time_seconds) AS avg_execution_time,
  MAX(execution_time_seconds) AS max_execution_time
FROM pc_insurance.reference.silver_load_audit
WHERE run_timestamp >= CURRENT_DATE() - 7
GROUP BY DATE(run_timestamp), transformation_id
ORDER BY run_date DESC, transformation_id;

-- Data quality trend
SELECT 
  DATE(validation_timestamp) AS validation_date,
  table_name,
  COUNT(*) AS total_validations,
  SUM(CASE WHEN validation_result = 'PASS' THEN 1 ELSE 0 END) AS passed,
  ROUND(SUM(CASE WHEN validation_result = 'PASS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate
FROM pc_insurance.dq.dq_validation_results
WHERE validation_timestamp >= CURRENT_DATE() - 7
GROUP BY DATE(validation_timestamp), table_name
ORDER BY validation_date DESC, table_name;

-- Row count trend
SELECT 
  DATE(run_timestamp) AS run_date,
  transformation_id,
  source_row_count,
  target_row_count
FROM pc_insurance.reference.silver_load_audit
WHERE run_timestamp >= CURRENT_DATE() - 7
ORDER BY run_date DESC, transformation_id;
```

### Alert Conditions

| Alert | Condition | Severity | Action |
|-------|-----------|----------|--------|
| Pipeline Failure | status = 'FAILED' | Critical | Investigate immediately |
| DQ Score Low | dq_score < 0.90 | Critical | Review failed validations |
| DQ Score Warning | dq_score < 0.95 | Warning | Monitor trend |
| Reconciliation Fail | recon_status = 'FAIL' | High | Investigate count mismatch |
| Execution Time High | execution_time > 2x baseline | Warning | Check for performance issues |
| No Data | source_row_count = 0 | High | Check upstream systems |

---

## Troubleshooting

### Common Issues

#### 1. Pipeline Failure

**Symptoms**: Pipeline status = 'FAILED'

**Diagnosis**:
```sql
-- Get error details
SELECT 
  transformation_id,
  run_timestamp,
  error_message
FROM pc_insurance.reference.silver_load_audit
WHERE status = 'FAILED'
ORDER BY run_timestamp DESC
LIMIT 10;
```

**Resolution Steps**:
1. Check error message in audit table
2. Review notebook execution logs in Databricks UI
3. Verify source data availability
4. Check for schema changes
5. Validate Unity Catalog permissions
6. Re-run pipeline after fixing issue

#### 2. Data Quality Failures

**Symptoms**: DQ score < 95%

**Diagnosis**:
```sql
-- Get failed DQ checks
SELECT 
  table_name,
  column_name,
  validation_rule,
  failed_record_count,
  validation_timestamp
FROM pc_insurance.dq.dq_validation_results
WHERE validation_result = 'FAIL'
AND DATE(validation_timestamp) = CURRENT_DATE()
ORDER BY failed_record_count DESC;
```

**Resolution Steps**:
1. Identify which validation rules failed
2. Query source data to understand root cause
3. Determine if issue is data quality or validation rule
4. Coordinate with source system owners if needed
5. Update cleansing rules or validation thresholds if appropriate

#### 3. Reconciliation Mismatch

**Symptoms**: Source count ≠ Target count

**Diagnosis**:
```sql
-- Get reconciliation details
SELECT 
  transformation_id,
  source_count,
  target_count,
  count_diff,
  notes
FROM pc_insurance.reference.silver_reconciliation
WHERE recon_status = 'FAIL'
ORDER BY recon_timestamp DESC;
```

**Resolution Steps**:
1. Check for duplicate records in source
2. Verify deduplication logic
3. Check for SCD2 version proliferation
4. Review transformation logic
5. Manually reconcile a sample of records

#### 4. Performance Degradation

**Symptoms**: Execution time > 2x baseline

**Diagnosis**:
```python
# Check table statistics
spark.sql("DESCRIBE EXTENDED pc_insurance.silver.policy_dim").show(100, False)

# Check for small files
spark.sql("""
  SELECT 
    COUNT(*) as file_count,
    SUM(size) / 1024 / 1024 / 1024 as total_size_gb,
    AVG(size) / 1024 / 1024 as avg_file_size_mb
  FROM (
    DESCRIBE DETAIL pc_insurance.silver.policy_dim
  )
""").show()
```

**Resolution Steps**:
1. Run OPTIMIZE on affected tables
2. Check for partition skew
3. Review query execution plans
4. Consider Z-ordering on frequently filtered columns
5. Increase cluster size if needed

#### 5. Missing Data

**Symptoms**: Expected data not present in Bronze layer

**Diagnosis**:
```sql
-- Check ingestion timestamps
SELECT 
  MAX(ingestion_timestamp) AS latest_ingestion,
  COUNT(*) AS row_count
FROM pc_insurance.bronze.policies_raw
WHERE DATE(ingestion_timestamp) = CURRENT_DATE();
```

**Resolution Steps**:
1. Verify source system availability
2. Check network connectivity
3. Review ingestion job logs
4. Verify file arrival in landing zone
5. Contact source system team if needed

---

## Data Quality Checks

### Running DQ Checks Manually

```python
# Run DQ checks on a specific table
from pc_insurance.dq import run_dq_checks

result = run_dq_checks("pc_insurance.silver.policy_dim")
print(result)
```

### DQ Check Definitions

| Check | Rule | Threshold |
|-------|------|-----------|
| Completeness | NOT NULL on required fields | 100% |
| Validity | Status codes in valid list | 100% |
| Consistency | FK exists in parent table | > 99% |
| Accuracy | Loss ratio <= 2.0 | > 95% |
| Timeliness | Data < 24 hours old | 100% |

### Adding New DQ Checks

1. Define validation rule in `pc_insurance.dq` schema
2. Add to DQ check notebook
3. Update threshold in configuration
4. Test on sample data
5. Deploy to production

---

## Incident Response

### Severity Levels

| Level | Description | Response Time | Escalation |
|-------|-------------|---------------|------------|
| P1 - Critical | Production pipeline down | 15 minutes | Immediate |
| P2 - High | Data quality < 90% | 1 hour | If not resolved in 2 hours |
| P3 - Medium | Performance degradation | 4 hours | If not resolved in 8 hours |
| P4 - Low | Minor issues, warnings | Next business day | N/A |

### Incident Response Steps

1. **Acknowledge**
   - Log incident in tracking system
   - Notify stakeholders

2. **Assess**
   - Determine severity
   - Identify affected systems/data

3. **Contain**
   - Stop failing pipelines if needed
   - Prevent data corruption

4. **Investigate**
   - Gather logs and error messages
   - Identify root cause

5. **Resolve**
   - Implement fix
   - Test resolution
   - Re-run affected pipelines

6. **Communicate**
   - Update stakeholders
   - Document resolution

7. **Post-Mortem**
   - Write incident report
   - Identify preventive measures

---

## Maintenance Procedures

### Weekly Maintenance

**Every Monday at 1:00 AM UTC**

1. **VACUUM old versions**
   ```sql
   -- Vacuum Bronze tables (retain 7 days)
   VACUUM pc_insurance.bronze.policies_raw RETAIN 168 HOURS;
   VACUUM pc_insurance.bronze.claims_raw RETAIN 168 HOURS;
   VACUUM pc_insurance.bronze.premiums_raw RETAIN 168 HOURS;
   
   -- Vacuum Silver tables (retain 30 days)
   VACUUM pc_insurance.silver.policy_dim RETAIN 720 HOURS;
   VACUUM pc_insurance.silver.claim_dim RETAIN 720 HOURS;
   
   -- Vacuum Gold tables (retain 90 days)
   VACUUM pc_insurance.gold.loss_ratio_by_lob RETAIN 2160 HOURS;
   ```

2. **OPTIMIZE tables**
   ```sql
   -- Optimize frequently queried tables
   OPTIMIZE pc_insurance.silver.policy_dim ZORDER BY (policy_id, state);
   OPTIMIZE pc_insurance.silver.claim_fact ZORDER BY (claim_id, loss_date);
   OPTIMIZE pc_insurance.gold.loss_ratio_by_lob;
   ```

3. **Update table statistics**
   ```sql
   -- Analyze tables for query optimization
   ANALYZE TABLE pc_insurance.silver.policy_dim COMPUTE STATISTICS;
   ANALYZE TABLE pc_insurance.silver.claim_fact COMPUTE STATISTICS;
   ANALYZE TABLE pc_insurance.gold.loss_ratio_by_lob COMPUTE STATISTICS;
   ```

### Monthly Maintenance

**First Sunday of each month at 12:00 AM UTC**

1. **Review and archive old audit logs**
   ```sql
   -- Archive audit logs older than 90 days
   CREATE TABLE pc_insurance.reference.silver_load_audit_archive AS
   SELECT * FROM pc_insurance.reference.silver_load_audit
   WHERE run_timestamp < CURRENT_DATE() - 90;
   
   DELETE FROM pc_insurance.reference.silver_load_audit
   WHERE run_timestamp < CURRENT_DATE() - 90;
   ```

2. **Review metadata configurations**
   - Check for inactive transformations
   - Update execution orders if needed
   - Review and update cleansing rules

3. **Performance review**
   - Analyze execution time trends
   - Identify optimization opportunities
   - Review cluster sizing

### Quarterly Maintenance

**First Sunday of quarter at 12:00 AM UTC**

1. **Schema evolution review**
   - Document any schema changes
   - Update data dictionary
   - Update downstream dependencies

2. **Disaster recovery test**
   - Verify backups
   - Test restore procedures
   - Update DR documentation

3. **Security audit**
   - Review Unity Catalog permissions
   - Audit user access
   - Review PII masking effectiveness

---

## Emergency Contacts

### On-Call Rotation

| Role | Primary | Secondary |
|------|---------|-----------|
| Data Engineer | [Name] | [Name] |
| Platform Engineer | [Name] | [Name] |
| Data Architect | [Name] | [Name] |

### Escalation Path

1. On-call Data Engineer
2. Data Engineering Manager
3. VP of Data & Analytics
4. CTO

### External Contacts

| System | Contact | Phone | Email |
|--------|---------|-------|-------|
| Policy Admin System | [Name] | [Phone] | [Email] |
| Claims System | [Name] | [Phone] | [Email] |
| Databricks Support | Support Portal | - | support@databricks.com |

---

## Useful Commands

### Check Cluster Status
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

cluster_id = "your-cluster-id"
cluster = w.clusters.get(cluster_id)
print(f"Cluster State: {cluster.state}")
```

### Check Job Run History
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

job_id = 820361677269451
runs = w.jobs.list_runs(job_id=job_id, limit=20)

for run in runs:
    print(f"{run.run_id}: {run.state.life_cycle_state} - {run.start_time}")
```

### Check Table Lineage
```sql
-- View table lineage in Unity Catalog
DESCRIBE EXTENDED pc_insurance.gold.loss_ratio_by_lob;
```

### Export Audit Logs
```python
# Export audit logs to CSV
audit_df = spark.sql("""
  SELECT * FROM pc_insurance.reference.silver_load_audit
  WHERE run_timestamp >= CURRENT_DATE() - 7
""")

audit_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("/tmp/audit_logs")
```

---

## Appendix

### Glossary

- **SCD2**: Slowly Changing Dimension Type 2 - tracks historical changes
- **DQ**: Data Quality
- **LOB**: Line of Business
- **UW**: Underwriting
- **VACUUM**: Delta Lake command to remove old file versions
- **OPTIMIZE**: Delta Lake command to compact small files
- **Z-ORDER**: Delta Lake optimization technique for multi-dimensional clustering

### Related Documentation

- [Architecture Guide](./ARCHITECTURE.md)
- [Data Dictionary](./DATA_DICTIONARY.md)
- [Git Automation Guide](./Git_Automation_Guide.md)
- [Databricks Documentation](https://docs.databricks.com/)

---

**Version**: 2.0  
**Last Updated**: 2026-09-25  
**Owner**: Data Engineering Team  
**Review Frequency**: Quarterly
