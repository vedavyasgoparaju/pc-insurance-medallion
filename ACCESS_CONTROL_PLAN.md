# P&C Insurance Medallion - Access Control & Permissions Plan

## Overview
This document defines the Unity Catalog access control model for the P&C Insurance Medallion architecture.

## Current Issue
- **ANALYST** agent has access only to Gold layer tables
- **ANALYST** needs read access to Bronze and Silver layers for data validation
- All agents need proper permissions aligned with their roles

## Required SQL Grants (Execute as UC Admin)

```sql
-- Bronze layer access
GRANT USAGE ON SCHEMA pc_insurance.bronze TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.bronze TO `account users`;

-- Silver layer access
GRANT USAGE ON SCHEMA pc_insurance.silver TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.silver TO `account users`;

-- Reference layer access
GRANT USAGE ON SCHEMA pc_insurance.reference TO `account users`;
GRANT SELECT ON SCHEMA pc_insurance.reference TO `account users`;

-- DQ layer access
GRANT USAGE ON SCHEMA pc_insurance.dq TO `account users`;
```

## Genie Space Updates Required

### pc_analyst_genie
1. Navigate to Genie → Spaces → pc_analyst_genie
2. Edit Space → Data Sources
3. Add schemas: pc_insurance.bronze, pc_insurance.silver
4. Save and test

## Verification

```sql
SELECT 'bronze.agents_raw' as table_name, COUNT(*) as accessible 
FROM pc_insurance.bronze.agents_raw
UNION ALL
SELECT 'silver.agent_dim', COUNT(*) 
FROM pc_insurance.silver.agent_dim;
```

**Status**: Pending Manual Implementation  
**Date**: 2026-09-24
