# P&C Insurance Medallion - Agent Access Matrix

## Role-Based Access Control (RBAC) Summary

### Access Matrix

| Agent / Role | Bronze Layer | Silver Layer | Gold Layer | Reference Layer | DQ Layer | Capabilities |
|--------------|--------------|--------------|------------|-----------------|----------|--------------|
| **ARCHITECT** | SELECT | SELECT | SELECT | SELECT | SELECT | Design validation, architecture review |
| **DATA ENGINEER** | ALL | ALL | ALL | ALL | ALL + EXECUTE | Create/modify tables, implement pipelines |
| **ANALYST** | SELECT ✨ | SELECT ✨ | SELECT | SELECT | SELECT | Query all layers, cross-layer validation |
| **QA VALIDATOR** | SELECT | SELECT | SELECT | SELECT | EXECUTE | Run DQ checks, validate pipelines |
| **P&C DOMAIN EXPERT** | SELECT | SELECT | SELECT | SELECT | SELECT | Provide insurance domain context |
| **DEVOPS** | SELECT | SELECT | SELECT | MODIFY | SELECT | Manage deployments, update metadata |
| **DOCUMENTATION** | SELECT | SELECT | SELECT | SELECT | SELECT | Generate complete documentation |

✨ **NEW** - Access granted in this deployment

### Layer Descriptions

#### Bronze Layer (`pc_insurance.bronze`)
**Purpose**: Raw data ingestion from source systems  
**Tables**: agents_raw, claims_raw, customers_raw, policies_raw, premiums_raw  
**Access Reason**: Analysts need to see raw data for data quality validation and metric tracing

#### Silver Layer (`pc_insurance.silver`)
**Purpose**: Cleansed, conformed, and deduplicated data  
**Tables**: agent_dim, claim_dim, claim_fact, customer_dim, date_dim, policy_dim, premium_fact  
**Access Reason**: Analysts need to understand transformations and validate business logic

#### Gold Layer (`pc_insurance.gold`)
**Purpose**: Business-ready aggregated metrics and KPIs  
**Tables**: claim_frequency_severity, exposure_summary, loss_ratio_by_lob, premium_growth, retention_by_agent, uw_dashboard_summary  
**Access Reason**: Primary consumption layer for business users and analysts

#### Reference Layer (`pc_insurance.reference`)
**Purpose**: Configuration, metadata, and audit tables  
**Tables**: silver_transformation_config, silver_load_audit, silver_reconciliation  
**Access Reason**: All roles need visibility into pipeline configuration and execution history

#### DQ Layer (`pc_insurance.dq`)
**Purpose**: Data quality validation functions  
**Functions**: calculate_dq_score, validation functions  
**Access Reason**: QA and Data Engineers need to execute DQ checks; others need to view results

## Use Cases by Role

### ANALYST (Enhanced Access)

**Before**: Could only query Gold layer
```sql
-- Limited to Gold queries
SELECT * FROM pc_insurance.gold.loss_ratio_by_lob;
```

**After**: Can query and join across all layers
```sql
-- Cross-layer validation
SELECT 
  g.line_of_business,
  g.loss_ratio,
  COUNT(DISTINCT s.policy_id) as silver_policies,
  COUNT(DISTINCT b.policy_id) as bronze_policies,
  CASE 
    WHEN COUNT(DISTINCT s.policy_id) = COUNT(DISTINCT b.policy_id) 
    THEN 'Reconciled' 
    ELSE 'Discrepancy' 
  END as data_quality_status
FROM pc_insurance.gold.loss_ratio_by_lob g
LEFT JOIN pc_insurance.silver.policy_dim s 
  ON g.line_of_business = s.line_of_business
LEFT JOIN pc_insurance.bronze.policies_raw b 
  ON s.policy_id = b.policy_id
GROUP BY g.line_of_business, g.loss_ratio;
```

**New Capabilities**:
- ✅ Trace metrics back to source data
- ✅ Validate transformation logic
- ✅ Identify data quality issues
- ✅ Reconcile record counts across layers
- ✅ Understand data lineage

### DATA ENGINEER

**Responsibilities**:
- Create and modify tables in all layers
- Implement Bronze → Silver → Gold pipelines
- Embed data quality checks
- Maintain transformation logic
- Optimize query performance

**Example Operations**:
```sql
-- Create Silver table
CREATE TABLE pc_insurance.silver.new_dimension AS ...

-- Run DQ checks
SELECT pc_insurance.dq.calculate_dq_score(total_records, failed_records);

-- Update reference metadata
INSERT INTO pc_insurance.reference.silver_transformation_config VALUES (...);
```

### QA VALIDATOR

**Responsibilities**:
- Execute data quality validation checks
- Verify pipeline outputs
- Test transformation logic
- Validate business rules
- Report data quality metrics

**Example Operations**:
```sql
-- Run DQ validation
SELECT 
  table_name,
  pc_insurance.dq.calculate_dq_score(
    COUNT(*), 
    COUNT(*) FILTER (WHERE validation_failed)
  ) as dq_score
FROM pc_insurance.silver.policy_dim;

-- Reconciliation check
SELECT 
  'Bronze to Silver' as check,
  (SELECT COUNT(*) FROM pc_insurance.bronze.policies_raw) as bronze_count,
  (SELECT COUNT(*) FROM pc_insurance.silver.policy_dim) as silver_count,
  ABS((SELECT COUNT(*) FROM pc_insurance.bronze.policies_raw) - 
      (SELECT COUNT(*) FROM pc_insurance.silver.policy_dim)) as discrepancy;
```

### ARCHITECT

**Responsibilities**:
- Design layer schemas and data models
- Define data flow topology
- Establish governance policies
- Validate implementation against design
- Review performance and optimization

**Example Queries**:
```sql
-- Validate schema design
DESCRIBE TABLE pc_insurance.silver.policy_dim;

-- Check data distribution
SELECT 
  line_of_business,
  COUNT(*) as policy_count,
  AVG(annual_premium) as avg_premium
FROM pc_insurance.silver.policy_dim
GROUP BY line_of_business;

-- Verify SCD2 implementation
SELECT 
  policy_id,
  is_current,
  effective_from,
  effective_to
FROM pc_insurance.silver.policy_dim
WHERE policy_id = 'POL0000001'
ORDER BY effective_from;
```

## Permission Verification Queries

### Verify Your Access

Run these queries to verify your role's access:

```sql
-- Check Bronze access
SELECT 'Bronze Access' as layer, COUNT(*) as accessible_records 
FROM pc_insurance.bronze.agents_raw;

-- Check Silver access
SELECT 'Silver Access' as layer, COUNT(*) as accessible_records 
FROM pc_insurance.silver.agent_dim;

-- Check Gold access
SELECT 'Gold Access' as layer, COUNT(*) as accessible_records 
FROM pc_insurance.gold.loss_ratio_by_lob;

-- Check Reference access
SELECT 'Reference Access' as layer, COUNT(*) as accessible_records 
FROM pc_insurance.reference.silver_transformation_config;
```

Expected: All queries return counts without permission errors.

### Verify Cross-Layer Joins

```sql
-- Test cross-layer join capability
SELECT 
  'Cross-Layer Join Test' as test_name,
  COUNT(*) as joined_records
FROM pc_insurance.bronze.policies_raw b
INNER JOIN pc_insurance.silver.policy_dim s ON b.policy_id = s.policy_id
INNER JOIN pc_insurance.gold.loss_ratio_by_lob g ON s.line_of_business = g.line_of_business;
```

Expected: Returns count of successfully joined records.

## Security & Compliance

### Data Governance

- **Audit Logging**: All access logged in Unity Catalog audit logs
- **Row-Level Security**: Consider implementing for sensitive data subsets
- **Column Masking**: Consider masking PII fields (email, phone, SSN)
- **Data Classification**: Bronze/Silver may contain PII - handle accordingly

### Compliance Considerations

- **GDPR**: Ensure PII access is logged and justified
- **SOX**: Maintain separation of duties (read vs. write access)
- **NAIC**: Insurance-specific data retention and access requirements
- **Internal Policies**: Follow organization's data governance policies

## Monitoring & Auditing

### Access Monitoring

Track these metrics:
- Failed permission attempts by user/role
- Cross-layer query frequency and performance
- Data export activities
- Unusual access patterns

### Audit Queries

```sql
-- View recent access (requires admin privileges)
SELECT 
  user_identity,
  action_name,
  request_params,
  event_time
FROM system.access.audit
WHERE catalog_name = 'pc_insurance'
  AND event_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY event_time DESC;
```

## Support & Questions

- **Access Issues**: Contact Unity Catalog administrator
- **Role Questions**: Review this matrix or contact architecture team
- **New Requirements**: Submit access request with business justification
- **Documentation**: See `DEPLOYMENT_GUIDE.md` for detailed instructions

---

**Version**: 1.0  
**Last Updated**: 2026-09-24  
**Owner**: P&C Insurance Medallion Architecture Team  
**Review Frequency**: Quarterly
