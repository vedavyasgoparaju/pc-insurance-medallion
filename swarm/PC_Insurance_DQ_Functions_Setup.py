# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,DQ Functions Setup Overview
# MAGIC %md
# MAGIC # P&C Insurance DQ Functions Setup
# MAGIC
# MAGIC Idempotent registration of 7 Unity Catalog SQL functions in `pc_insurance.dq` for data quality validation.
# MAGIC
# MAGIC **Functions:**
# MAGIC 1. `check_policy_exists` — validates policy_id exists in bronze
# MAGIC 2. `check_claim_status` — validates claim status is in allowed set
# MAGIC 3. `check_premium_positive` — validates premium > 0
# MAGIC 4. `check_loss_ratio` — validates loss ratio 0–500%
# MAGIC 5. `check_not_null` — generic null check
# MAGIC 6. `calculate_dq_score` — computes pass-rate percentage
# MAGIC 7. `check_date_order` — validates effective_date ≤ expiry_date

# COMMAND ----------

# DBTITLE 1,Create Catalog & Schema
# MAGIC %sql
# MAGIC -- Ensure catalog and schema exist
# MAGIC CREATE CATALOG IF NOT EXISTS pc_insurance;
# MAGIC CREATE SCHEMA IF NOT EXISTS pc_insurance.dq;
# MAGIC USE CATALOG pc_insurance;
# MAGIC USE SCHEMA dq;

# COMMAND ----------

# DBTITLE 1,Function 1: check_policy_exists
# MAGIC %sql
# MAGIC -- 1. check_policy_exists
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.check_policy_exists(policy_id_val STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'DQ Check: Returns TRUE if the given policy_id exists in the bronze.policies_raw table'
# MAGIC READS SQL DATA
# MAGIC RETURN (
# MAGIC   EXISTS (SELECT 1 FROM pc_insurance.bronze.policies_raw WHERE policy_id = policy_id_val)
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Function 2: check_claim_status
# MAGIC %sql
# MAGIC -- 2. check_claim_status
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.check_claim_status(claim_status_val STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'DQ Check: Returns TRUE if the claim status is one of the valid values'
# MAGIC RETURN (
# MAGIC   claim_status_val IN ('Open', 'Closed', 'Reopened', 'Denied', 'Pending', 'Submitted', 'Approved', 'Rejected', 'Investigating', 'Reopened')
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Function 3: check_premium_positive
# MAGIC %sql
# MAGIC -- 3. check_premium_positive
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.check_premium_positive(premium_val DECIMAL(12,2))
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'DQ Check: Returns TRUE if premium amount is greater than zero'
# MAGIC RETURN (premium_val > 0);

# COMMAND ----------

# DBTITLE 1,Function 5: check_not_null
# MAGIC %sql
# MAGIC -- 5. check_not_null
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.check_not_null(
# MAGIC   val STRING,
# MAGIC   column_name STRING
# MAGIC )
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'DQ Check: Returns TRUE if the value is not null'
# MAGIC RETURN (val IS NOT NULL);

# COMMAND ----------

# DBTITLE 1,Function 6: calculate_dq_score
# MAGIC %sql
# MAGIC -- 6. calculate_dq_score
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.calculate_dq_score(
# MAGIC   total_records INT,
# MAGIC   failed_records INT
# MAGIC )
# MAGIC RETURNS DECIMAL(10,4)
# MAGIC COMMENT 'DQ Score: Returns the percentage of records that passed validation (0.0 to 1.0)'
# MAGIC RETURN (
# MAGIC   CASE
# MAGIC     WHEN total_records = 0 THEN 0.0
# MAGIC     ELSE CAST((total_records - failed_records) AS DECIMAL(10,4)) / CAST(total_records AS DECIMAL(10,4))
# MAGIC   END
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Function 7: check_date_order
# MAGIC %sql
# MAGIC -- 7. check_date_order
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.check_date_order(
# MAGIC   effective_date_val DATE,
# MAGIC   expiry_date_val DATE
# MAGIC )
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'DQ Check: Returns TRUE if effective_date is before or equal to expiry_date'
# MAGIC RETURN (effective_date_val <= expiry_date_val);

# COMMAND ----------

# DBTITLE 1,Verification
# Verification
print("Verifying DQ functions...")
results = {"passed": 0, "failed": 0}

dq_functions = [
    ("check_policy_exists", "SELECT pc_insurance.dq.check_policy_exists('POL-0001')"),
    ("check_claim_status", "SELECT pc_insurance.dq.check_claim_status('Open')"),
    ("check_premium_positive", "SELECT pc_insurance.dq.check_premium_positive(100.00)"),
    ("check_loss_ratio", "SELECT pc_insurance.dq.check_loss_ratio(10000.00, 3000.00)"),
    ("check_not_null", "SELECT pc_insurance.dq.check_not_null('test', 'test_col')"),
    ("calculate_dq_score", "SELECT pc_insurance.dq.calculate_dq_score(100, 5)"),
    ("check_date_order", "SELECT pc_insurance.dq.check_date_order(DATE('2024-01-01'), DATE('2024-12-31'))"),
]

for name, query in dq_functions:
    try:
        result = spark.sql(query).collect()[0][0]
        print(f"  [PASS] {name}: {result}")
        results["passed"] += 1
    except Exception as e:
        print(f"  [FAIL] {name}: {str(e)[:100]}")
        results["failed"] += 1

print(f"\nDQ SETUP SUMMARY: {results['passed']} passed, {results['failed']} failed")
if results["failed"] > 0:
    raise RuntimeError(f"DQ setup failed with {results['failed']} errors")
else:
    print("All 7 DQ functions registered and verified.")

# COMMAND ----------

# DBTITLE 1,Function 4: check_loss_ratio
# MAGIC %sql
# MAGIC -- 4. check_loss_ratio
# MAGIC CREATE OR REPLACE FUNCTION pc_insurance.dq.check_loss_ratio(
# MAGIC   earned_premium_val DECIMAL(15,2),
# MAGIC   incurred_losses_val DECIMAL(15,2)
# MAGIC )
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'DQ Check: Returns TRUE if loss ratio is between 0 and 5.0 (500%) - flags extreme outliers'
# MAGIC RETURN (
# MAGIC   earned_premium_val > 0
# MAGIC   AND (incurred_losses_val / earned_premium_val) >= 0
# MAGIC   AND (incurred_losses_val / earned_premium_val) <= 5.0
# MAGIC );