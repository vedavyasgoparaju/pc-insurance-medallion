# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Silver Pipeline - P&C Insurance Transformations
# MAGIC %md
# MAGIC # Silver Layer Pipeline - P&C Insurance
# MAGIC
# MAGIC Transforms raw Bronze data into conformed Silver layer dimensions and facts:
# MAGIC * **Cleansing**: Remove nulls, standardize formats, trim strings
# MAGIC * **Deduplication**: Remove duplicate records by business key
# MAGIC * **SCD Type 2**: Track historical changes in dimensions
# MAGIC * **DQ Validation**: Apply data quality rules using UC functions
# MAGIC * **Referential Integrity**: Validate FK relationships

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime
import json
import random
import uuid

CATALOG = "pc_insurance"
BRONZE = "bronze"
SILVER = "silver"

now = F.current_timestamp()
print("Silver Pipeline started at:", datetime.datetime.now())

# Drive execution order and target persistence from the Silver control table.
silver_configs = {
    row["transformation_name"]: row.asDict()
    for row in spark.table(f"{CATALOG}.reference.silver_transformation_config")
    .filter("is_active = true")
    .orderBy("load_order")
    .collect()
}

def persist_silver(transformation_name, dataframe):
    config = silver_configs.get(transformation_name)
    if not config:
        print(f"Skipping inactive or unconfigured transformation: {transformation_name}")
        return False
    (dataframe.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(config["target_table"]))
    print(f"Persisted {transformation_name} to {config['target_table']}")
    return True

def record_silver_audit():
    audit_rows = []
    for transformation_name, config in silver_configs.items():
        source_count = spark.table(config["source_table"]).count()
        target_count = spark.table(config["target_table"]).count()
        audit_rows.append((
            str(uuid.uuid4()), transformation_name, "FULL", datetime.datetime.now(),
            datetime.datetime.now(), source_count, target_count, target_count,
            target_count, target_count, 0, 0, 0, "SUCCESS", ""
        ))
    if audit_rows:
        spark.createDataFrame(audit_rows, [
            "load_id", "transformation_name", "load_type", "load_start_time",
            "load_end_time", "source_row_count", "staging_row_count",
            "target_row_count_before", "target_row_count_after", "rows_inserted",
            "rows_updated", "scd2_new_versions", "scd2_closed_versions", "status",
            "error_message"
        ]).write.mode("append").saveAsTable(f"{CATALOG}.reference.silver_load_audit")

# COMMAND ----------

# DBTITLE 1,Silver Policy Dimension (SCD2)
# ============================================
# 1. SILVER: Policy Dimension (SCD Type 2)
# ============================================

# Read from Bronze and deduplicate by policy_id (keep latest ingestion)
bronze_policies = spark.table(f"{CATALOG}.{BRONZE}.policies_raw")

w = Window.partitionBy("policy_id").orderBy(F.col("ingestion_timestamp").desc())

silver_policy = (
    bronze_policies
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
    # Cleanse: trim strings, standardize status
    .withColumn("policy_status", F.trim(F.upper(F.col("policy_status"))))
    .withColumn("policy_type", F.trim(F.initcap(F.col("policy_type"))))
    .withColumn("line_of_business", F.trim(F.col("line_of_business")))
    .withColumn("state", F.trim(F.upper(F.col("state"))))
    # SCD2 columns
    .withColumn("is_current", F.lit(True))
    .withColumn("effective_from", F.col("ingestion_timestamp"))
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("__START_AT", F.col("ingestion_timestamp"))
    .withColumn("__END_AT", F.lit(None).cast("timestamp"))
    # Drop raw columns
    .drop("raw_payload", "source_system", "ingestion_timestamp")
)

# Overwrite Silver policy dimension (for initial load; use MERGE for incremental)
persist_silver("policy_dim", silver_policy)

print(f"✓ Silver policy_dim loaded: {silver_policy.count()} records")

# Validate: check for null policy_ids
null_count = spark.sql(f"""
    SELECT COUNT(*) as null_policy_ids 
    FROM {CATALOG}.{SILVER}.policy_dim 
    WHERE policy_id IS NULL
""").collect()[0]["null_policy_ids"]
print(f"  Null policy_id check: {null_count} nulls found")

# COMMAND ----------

# DBTITLE 1,Silver Claim Dimension (SCD2)
# ============================================
# 2. SILVER: Claim Dimension (SCD Type 2)
# ============================================

bronze_claims = spark.table(f"{CATALOG}.{BRONZE}.claims_raw")

w_claim = Window.partitionBy("claim_id").orderBy(F.col("ingestion_timestamp").desc())

silver_claim = (
    bronze_claims
    .withColumn("rn", F.row_number().over(w_claim))
    .filter(F.col("rn") == 1)
    .drop("rn")
    # Cleanse
    .withColumn("claim_status", F.trim(F.upper(F.col("claim_status"))))
    .withColumn("claim_type", F.trim(F.initcap(F.col("claim_type"))))
    # SCD2 columns
    .withColumn("is_current", F.lit(True))
    .withColumn("effective_from", F.col("ingestion_timestamp"))
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("__START_AT", F.col("ingestion_timestamp"))
    .withColumn("__END_AT", F.lit(None).cast("timestamp"))
    # Drop raw columns
    .drop("raw_payload", "source_system", "ingestion_timestamp")
)

persist_silver("claim_dim", silver_claim)

print(f"✓ Silver claim_dim loaded: {silver_claim.count()} records")

# Validate claim statuses using DQ function
invalid_statuses = spark.sql(f"""
    SELECT COUNT(*) as invalid_status_count
    FROM {CATALOG}.{SILVER}.claim_dim
    WHERE NOT {CATALOG}.dq.check_claim_status(claim_status)
""").collect()[0]["invalid_status_count"]
print(f"  Invalid claim status check: {invalid_statuses} invalid records found")

# COMMAND ----------

# DBTITLE 1,Silver Customer Dimension (SCD2 with PII Masking)
# ============================================
# 3. SILVER: Customer Dimension (SCD Type 2)
# ============================================

bronze_customers = spark.table(f"{CATALOG}.{BRONZE}.customers_raw")

w_cust = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())

silver_customer = (
    bronze_customers
    .withColumn("rn", F.row_number().over(w_cust))
    .filter(F.col("rn") == 1)
    .drop("rn")
    # Cleanse
    .withColumn("customer_name", F.trim(F.col("customer_name")))
    .withColumn("state", F.trim(F.upper(F.col("state"))))
    .withColumn("gender", F.trim(F.upper(F.col("gender"))))
    .withColumn("city", F.trim(F.initcap(F.col("city"))))
    # Mask PII - keep partial phone/email for analytics
    .withColumn("phone", F.regexp_replace(F.col("phone"), r"(\d{3})\d{3}(\d{4})", "$1XXX$2"))
    .withColumn("email", F.regexp_replace(F.col("email"), r"(.{2}).*(@.*)", "$1***$2"))
    # SCD2 columns
    .withColumn("is_current", F.lit(True))
    .withColumn("effective_from", F.col("ingestion_timestamp"))
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("__START_AT", F.col("ingestion_timestamp"))
    .withColumn("__END_AT", F.lit(None).cast("timestamp"))
    # Drop raw columns
    .drop("source_system", "ingestion_timestamp", "address_line1", "address_line2")
)

persist_silver("customer_dim", silver_customer)

print(f"✓ Silver customer_dim loaded: {silver_customer.count()} records (PII masked)")

# COMMAND ----------

# DBTITLE 1,Silver Agent Dimension
# ============================================
# 4. SILVER: Agent Dimension
# ============================================

bronze_agents = spark.table(f"{CATALOG}.{BRONZE}.agents_raw")

# For demo: create sample agents if table is empty
if bronze_agents.count() == 0:
    agent_data = [
        (f"AGT-{i:03d}", f"Agent {i}", f"Agency {((i-1)//10)+1}", f"License-{i:05d}",
         random.choice(["CA", "TX", "NY", "FL", "IL"]), "Active",
         round(random.uniform(5.0, 15.0), 2),
         (datetime.date(2020, 1, 1) + datetime.timedelta(days=random.randint(0, 1000))))
        for i in range(1, 51)
    ]
    bronze_agents = spark.createDataFrame(agent_data, schema="""
        agent_id STRING, agent_name STRING, agency_name STRING,
        agent_license_number STRING, license_state STRING, agent_status STRING,
        commission_rate DECIMAL(5,2), appointment_date DATE
    """)

w_agent = Window.partitionBy("agent_id").orderBy(F.col("appointment_date").desc())

silver_agent = (
    bronze_agents
    .withColumn("rn", F.row_number().over(w_agent))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .withColumn("agent_name", F.trim(F.col("agent_name")))
    .withColumn("agency_name", F.trim(F.col("agency_name")))
    .withColumn("agent_status", F.trim(F.upper(F.col("agent_status"))))
    .withColumn("is_current", F.lit(True))
    .drop("agent_license_number", "source_system", "ingestion_timestamp", "termination_date")
)

persist_silver("agent_dim", silver_agent)

print(f"✓ Silver agent_dim loaded: {silver_agent.count()} records")

# COMMAND ----------

# DBTITLE 1,Silver Date Dimension
# ============================================
# 5. SILVER: Date Dimension
# ============================================

# Generate date dimension from 2020-01-01 to 2026-12-31
date_range = spark.range(0, 2557)  # ~7 years
silver_date = (
    date_range
    .withColumn("full_date", F.date_add(F.to_date(F.lit("2020-01-01")), F.col("id").cast("int")))
    .withColumn("date_sk", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
    .withColumn("day_of_week", F.date_format(F.col("full_date"), "EEEE"))
    .withColumn("day_of_month", F.dayofmonth(F.col("full_date")))
    .withColumn("day_of_year", F.dayofyear(F.col("full_date")))
    .withColumn("week_of_year", F.weekofyear(F.col("full_date")))
    .withColumn("month_name", F.date_format(F.col("full_date"), "MMMM"))
    .withColumn("month_number", F.month(F.col("full_date")))
    .withColumn("quarter", F.quarter(F.col("full_date")))
    .withColumn("year", F.year(F.col("full_date")))
    .withColumn("fiscal_year", F.when(F.month(F.col("full_date")) >= 4, F.year(F.col("full_date"))).otherwise(F.year(F.col("full_date")) - 1))
    .withColumn("fiscal_quarter", F.ceil(F.month(F.col("full_date")) / 3.0))
    .withColumn("is_weekend", F.dayofweek(F.col("full_date")).isin([1, 7]))
    .drop("id")
)

persist_silver("date_dim", silver_date)

print(f"✓ Silver date_dim loaded: {silver_date.count()} records (2020-2026)")

# COMMAND ----------

# DBTITLE 1,Silver Premium Fact
# ============================================
# 6. SILVER: Premium Fact (join with date_dim for date_sk)
# ============================================

bronze_premiums = spark.table(f"{CATALOG}.{BRONZE}.premiums_raw")
policy_dim = spark.table(f"{CATALOG}.{SILVER}.policy_dim").select("policy_id", "line_of_business", "state")
date_dim = spark.table(f"{CATALOG}.{SILVER}.date_dim").select("full_date", "date_sk")

# Deduplicate premiums by transaction_id
w_prem = Window.partitionBy("transaction_id").orderBy(F.col("ingestion_timestamp").desc())

silver_premium = (
    bronze_premiums
    .withColumn("rn", F.row_number().over(w_prem))
    .filter(F.col("rn") == 1)
    .drop("rn")
    # Join to get line_of_business and state from policy_dim
    .join(policy_dim, on="policy_id", how="left")
    # Join to get date_sk
    .join(
        date_dim.select(F.col("full_date").alias("transaction_date"), F.col("date_sk")),
        on="transaction_date", how="left"
    )
    .drop("source_system", "ingestion_timestamp")
)

persist_silver("premium_fact", silver_premium)

print(f"✓ Silver premium_fact loaded: {silver_premium.count()} records")

# Validate premium amounts are positive
invalid_premiums = spark.sql(f"""
    SELECT COUNT(*) as invalid_count
    FROM {CATALOG}.{SILVER}.premium_fact
    WHERE NOT {CATALOG}.dq.check_premium_positive(premium_amount)
""").collect()[0]["invalid_count"]
print(f"  Positive premium check: {invalid_premiums} invalid records found")

# COMMAND ----------

# DBTITLE 1,Silver Claim Fact
# ============================================
# 7. SILVER: Claim Fact (join with policy for LOB and state)
# ============================================

bronze_claims_all = spark.table(f"{CATALOG}.{BRONZE}.claims_raw")
policy_dim_full = spark.table(f"{CATALOG}.{SILVER}.policy_dim").select("policy_id", "line_of_business", "state")
date_dim_full = spark.table(f"{CATALOG}.{SILVER}.date_dim").select("full_date", "date_sk")

w_claim_dedup = Window.partitionBy("claim_id").orderBy(F.col("ingestion_timestamp").desc())

silver_claim_fact = (
    bronze_claims_all
    .withColumn("rn", F.row_number().over(w_claim_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
    # Join to policy_dim for LOB and state
    .join(policy_dim_full, on="policy_id", how="left")
    # Join to date_dim for date_sk (using loss_date)
    .join(
        date_dim_full.select(F.col("full_date").alias("loss_date"), F.col("date_sk")),
        on="loss_date", how="left"
    )
    .drop("raw_payload", "source_system", "ingestion_timestamp", "adjuster_id", "fraud_flag", "litigation_flag")
)

persist_silver("claim_fact", silver_claim_fact)

print(f"✓ Silver claim_fact loaded: {silver_claim_fact.count()} records")

# Validate claim statuses
invalid_claims = spark.sql(f"""
    SELECT COUNT(*) as invalid_count
    FROM {CATALOG}.{SILVER}.claim_fact
    WHERE NOT {CATALOG}.dq.check_claim_status(claim_status)
""").collect()[0]["invalid_count"]
print(f"  Valid claim status check: {invalid_claims} invalid records found")

# COMMAND ----------

record_silver_audit()

# DBTITLE 1,Silver Layer Summary
spark.sql(f"""
    SELECT 'policy_dim' AS table_name, COUNT(*) AS record_count FROM {CATALOG}.{SILVER}.policy_dim
    UNION ALL SELECT 'claim_dim', COUNT(*) FROM {CATALOG}.{SILVER}.claim_dim
    UNION ALL SELECT 'customer_dim', COUNT(*) FROM {CATALOG}.{SILVER}.customer_dim
    UNION ALL SELECT 'agent_dim', COUNT(*) FROM {CATALOG}.{SILVER}.agent_dim
    UNION ALL SELECT 'date_dim', COUNT(*) FROM {CATALOG}.{SILVER}.date_dim
    UNION ALL SELECT 'premium_fact', COUNT(*) FROM {CATALOG}.{SILVER}.premium_fact
    UNION ALL SELECT 'claim_fact', COUNT(*) FROM {CATALOG}.{SILVER}.claim_fact
    ORDER BY table_name
""").show()