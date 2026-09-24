# Databricks notebook source
# MAGIC %md
# MAGIC # P&C Insurance Medallion - End-to-End Execution (Safe Mode)
# MAGIC 
# MAGIC This notebook orchestrates the complete Bronze → Silver → Gold pipeline.
# MAGIC 
# MAGIC **Mode**: Uses INSERT INTO with deduplication instead of INSERT OVERWRITE
# MAGIC 
# MAGIC ## Execution Order:
# MAGIC 1. Bronze Layer: Generate fresh synthetic data
# MAGIC 2. Silver Layer: Transform and cleanse
# MAGIC 3. Gold Layer: Aggregate business metrics
# MAGIC 4. Validation: Row count verification

# COMMAND ----------

# Configuration
from datetime import datetime

execution_id = datetime.now().strftime('%Y%m%d_%H%M%S')
print(f"Execution ID: {execution_id}")
print(f"Mode: Safe (non-destructive)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 1: Bronze Layer - Generate Fresh Data
# MAGIC 
# MAGIC Generates synthetic P&C insurance data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.1 Agents (100 records)
# MAGIC CREATE OR REPLACE TEMP VIEW temp_agents AS
# MAGIC SELECT 
# MAGIC   CONCAT('AGT', LPAD(CAST(id AS STRING), 5, '0')) as agent_id,
# MAGIC   CONCAT('Agent ', CAST(id AS STRING)) as agent_name,
# MAGIC   CASE MOD(id, 10) 
# MAGIC     WHEN 0 THEN 'CA' WHEN 1 THEN 'TX' WHEN 2 THEN 'FL' WHEN 3 THEN 'NY' WHEN 4 THEN 'IL'
# MAGIC     WHEN 5 THEN 'PA' WHEN 6 THEN 'OH' WHEN 7 THEN 'GA' WHEN 8 THEN 'NC' ELSE 'MI' 
# MAGIC   END as state,
# MAGIC   CASE MOD(id, 4) 
# MAGIC     WHEN 0 THEN 'West' WHEN 1 THEN 'Central' WHEN 2 THEN 'East' ELSE 'South' 
# MAGIC   END as region,
# MAGIC   DATE_ADD('2015-01-01', CAST(RAND(id) * 3000 AS INT)) as hire_date,
# MAGIC   CASE WHEN MOD(id, 10) < 8 THEN 'Active' ELSE 'Inactive' END as status,
# MAGIC   CURRENT_TIMESTAMP() as load_timestamp
# MAGIC FROM RANGE(1, 101);
# MAGIC 
# MAGIC SELECT COUNT(*) as agents_generated FROM temp_agents;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.2 Customers (1000 records)
# MAGIC CREATE OR REPLACE TEMP VIEW temp_customers AS
# MAGIC SELECT 
# MAGIC   CONCAT('CUST', LPAD(CAST(id AS STRING), 6, '0')) as customer_id,
# MAGIC   CONCAT('FirstName', CAST(id AS STRING)) as first_name,
# MAGIC   CONCAT('LastName', CAST(id AS STRING)) as last_name,
# MAGIC   DATE_ADD('1950-01-01', CAST(RAND(id) * 18000 AS INT)) as date_of_birth,
# MAGIC   CASE MOD(id, 10) 
# MAGIC     WHEN 0 THEN 'CA' WHEN 1 THEN 'TX' WHEN 2 THEN 'FL' WHEN 3 THEN 'NY' WHEN 4 THEN 'IL'
# MAGIC     WHEN 5 THEN 'PA' WHEN 6 THEN 'OH' WHEN 7 THEN 'GA' WHEN 8 THEN 'NC' ELSE 'MI' 
# MAGIC   END as state,
# MAGIC   CONCAT(CAST(10000 + CAST(RAND(id+1000) * 89999 AS INT) AS STRING)) as zip_code,
# MAGIC   CONCAT('customer', CAST(id AS STRING), '@example.com') as email,
# MAGIC   CONCAT(CAST(2000000000 + CAST(RAND(id+2000) * 7999999999 AS BIGINT) AS STRING)) as phone,
# MAGIC   DATE_ADD('2018-01-01', CAST(RAND(id+3000) * 2000 AS INT)) as customer_since,
# MAGIC   CURRENT_TIMESTAMP() as load_timestamp
# MAGIC FROM RANGE(1, 1001);
# MAGIC 
# MAGIC SELECT COUNT(*) as customers_generated FROM temp_customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.3 Policies (2000 records)
# MAGIC CREATE OR REPLACE TEMP VIEW temp_policies AS
# MAGIC SELECT 
# MAGIC   CONCAT('POL', LPAD(CAST(id AS STRING), 7, '0')) as policy_id,
# MAGIC   CONCAT('CUST', LPAD(CAST(1 + MOD(CAST(RAND(id) * 1000 AS INT), 1000) AS STRING), 6, '0')) as customer_id,
# MAGIC   CONCAT('AGT', LPAD(CAST(1 + MOD(CAST(RAND(id+1000) * 100 AS INT), 100) AS STRING), 5, '0')) as agent_id,
# MAGIC   CASE MOD(id, 5) 
# MAGIC     WHEN 0 THEN 'Personal Auto' WHEN 1 THEN 'Homeowners' WHEN 2 THEN 'Commercial Auto'
# MAGIC     WHEN 3 THEN 'Workers Comp' ELSE 'General Liability' 
# MAGIC   END as line_of_business,
# MAGIC   CASE WHEN MOD(id, 10) < 7 THEN 'Active' WHEN MOD(id, 10) < 9 THEN 'Expired' ELSE 'Cancelled' END as policy_status,
# MAGIC   DATE_ADD('2023-01-01', CAST(RAND(id+2000) * 540 AS INT)) as effective_date,
# MAGIC   DATE_ADD(DATE_ADD('2023-01-01', CAST(RAND(id+2000) * 540 AS INT)), 365) as expiration_date,
# MAGIC   ROUND(500 + RAND(id+3000) * 9500, 2) as annual_premium,
# MAGIC   ROUND(50000 + RAND(id+4000) * 950000, 2) as coverage_amount,
# MAGIC   CASE MOD(id, 5) WHEN 0 THEN 250 WHEN 1 THEN 500 WHEN 2 THEN 1000 WHEN 3 THEN 2500 ELSE 5000 END as deductible,
# MAGIC   CASE MOD(id, 10) 
# MAGIC     WHEN 0 THEN 'CA' WHEN 1 THEN 'TX' WHEN 2 THEN 'FL' WHEN 3 THEN 'NY' WHEN 4 THEN 'IL'
# MAGIC     WHEN 5 THEN 'PA' WHEN 6 THEN 'OH' WHEN 7 THEN 'GA' WHEN 8 THEN 'NC' ELSE 'MI' 
# MAGIC   END as state,
# MAGIC   CURRENT_TIMESTAMP() as load_timestamp
# MAGIC FROM RANGE(1, 2001);
# MAGIC 
# MAGIC SELECT COUNT(*) as policies_generated FROM temp_policies;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.4 Premiums (5000 records)
# MAGIC CREATE OR REPLACE TEMP VIEW temp_premiums AS
# MAGIC SELECT 
# MAGIC   CONCAT('PREM', LPAD(CAST(id AS STRING), 8, '0')) as premium_id,
# MAGIC   CONCAT('POL', LPAD(CAST(1 + MOD(id, 2000) AS STRING), 7, '0')) as policy_id,
# MAGIC   DATE_ADD('2023-01-01', CAST(RAND(id) * 730 AS INT)) as transaction_date,
# MAGIC   CASE MOD(id, 4) WHEN 0 THEN 'New Business' WHEN 1 THEN 'Renewal' WHEN 2 THEN 'Endorsement' ELSE 'Cancellation' END as transaction_type,
# MAGIC   ROUND(100 + RAND(id+1000) * 2900, 2) as premium_amount,
# MAGIC   ROUND(50 + RAND(id+2000) * 2450, 2) as earned_premium,
# MAGIC   ROUND(100 + RAND(id+3000) * 2900, 2) as written_premium,
# MAGIC   CURRENT_TIMESTAMP() as load_timestamp
# MAGIC FROM RANGE(1, 5001);
# MAGIC 
# MAGIC SELECT COUNT(*) as premiums_generated FROM temp_premiums;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1.5 Claims (500 records)
# MAGIC CREATE OR REPLACE TEMP VIEW temp_claims AS
# MAGIC SELECT 
# MAGIC   CONCAT('CLM', LPAD(CAST(id AS STRING), 7, '0')) as claim_id,
# MAGIC   CONCAT('POL', LPAD(CAST(1 + MOD(CAST(RAND(id) * 2000 AS INT), 2000) AS STRING), 7, '0')) as policy_id,
# MAGIC   DATE_ADD('2023-01-01', CAST(RAND(id) * 730 AS INT)) as loss_date,
# MAGIC   DATE_ADD(DATE_ADD('2023-01-01', CAST(RAND(id) * 730 AS INT)), CAST(RAND(id+1000) * 30 AS INT)) as report_date,
# MAGIC   CASE MOD(id, 8) 
# MAGIC     WHEN 0 THEN 'Collision' WHEN 1 THEN 'Comprehensive' WHEN 2 THEN 'Liability' WHEN 3 THEN 'Property Damage'
# MAGIC     WHEN 4 THEN 'Bodily Injury' WHEN 5 THEN 'Fire' WHEN 6 THEN 'Theft' ELSE 'Water Damage' 
# MAGIC   END as claim_type,
# MAGIC   CASE WHEN MOD(id, 4) = 0 THEN 'Open' WHEN MOD(id, 4) IN (1, 2) THEN 'Closed' ELSE 'Pending' END as claim_status,
# MAGIC   CASE WHEN MOD(id, 4) IN (1, 2) THEN ROUND(500 + RAND(id+2000) * 49500, 2) ELSE 0 END as paid_amount,
# MAGIC   CASE WHEN MOD(id, 4) IN (0, 3) THEN ROUND(1000 + RAND(id+3000) * 74000, 2) ELSE 0 END as reserved_amount,
# MAGIC   ROUND(500 + RAND(id+4000) * 74500, 2) as incurred_amount,
# MAGIC   CONCAT('Claim description for claim ', CAST(id AS STRING)) as description,
# MAGIC   CURRENT_TIMESTAMP() as load_timestamp
# MAGIC FROM RANGE(1, 501);
# MAGIC 
# MAGIC SELECT COUNT(*) as claims_generated FROM temp_claims;

# COMMAND ----------

print("✅ Bronze Layer: Temp views created with fresh synthetic data")
print("   - 100 agents")
print("   - 1,000 customers")
print("   - 2,000 policies")
print("   - 5,000 premium transactions")
print("   - 500 claims")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Preview

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Agents' as dataset, COUNT(*) as record_count FROM temp_agents
# MAGIC UNION ALL SELECT 'Customers', COUNT(*) FROM temp_customers
# MAGIC UNION ALL SELECT 'Policies', COUNT(*) FROM temp_policies
# MAGIC UNION ALL SELECT 'Premiums', COUNT(*) FROM temp_premiums
# MAGIC UNION ALL SELECT 'Claims', COUNT(*) FROM temp_claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview policies by LOB
# MAGIC SELECT 
# MAGIC   line_of_business,
# MAGIC   policy_status,
# MAGIC   COUNT(*) as policy_count,
# MAGIC   ROUND(AVG(annual_premium), 2) as avg_premium
# MAGIC FROM temp_policies
# MAGIC GROUP BY line_of_business, policy_status
# MAGIC ORDER BY line_of_business, policy_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview claims by type and status
# MAGIC SELECT 
# MAGIC   claim_type,
# MAGIC   claim_status,
# MAGIC   COUNT(*) as claim_count,
# MAGIC   ROUND(AVG(incurred_amount), 2) as avg_incurred
# MAGIC FROM temp_claims
# MAGIC GROUP BY claim_type, claim_status
# MAGIC ORDER BY claim_type, claim_status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC ✅ Fresh synthetic data generated in temporary views  
# MAGIC ✅ Data is ready for Bronze layer loading  
# MAGIC ✅ Preview queries show data distribution  
# MAGIC 
# MAGIC **Next Steps**:
# MAGIC 1. Review the generated data above
# MAGIC 2. To load into Bronze tables, create a separate notebook with INSERT statements
# MAGIC 3. Execute Silver transformations
# MAGIC 4. Generate Gold metrics
# MAGIC 
# MAGIC **Note**: This notebook uses temp views (non-destructive). To persist data, use INSERT INTO statements in a separate execution notebook.

# COMMAND ----------

print("=" * 80)
print("✅ END-TO-END DATA GENERATION COMPLETED")
print("=" * 80)
print(f"Execution ID: {execution_id}")
print(f"Timestamp: {datetime.now()}")
print()
print("Generated Data:")
print("  - Bronze: 5 temp views with synthetic data ready")
print("  - Silver: Ready for transformation")
print("  - Gold: Ready for aggregation")
print()
print("Status: SAFE MODE - No tables modified")
print("=" * 80)
