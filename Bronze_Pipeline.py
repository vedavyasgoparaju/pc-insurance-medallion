# Databricks notebook source
# DBTITLE 1,Bronze Pipeline - P&C Insurance Data Ingestion
# MAGIC %md
# MAGIC # Bronze Layer Pipeline - P&C Insurance
# MAGIC
# MAGIC This notebook ingests raw P&C insurance data into the Bronze layer using:
# MAGIC * **Auto Loader** for streaming ingestion from cloud storage
# MAGIC * **Batch ingestion** for one-time loads
# MAGIC * **Data validation** with DQ functions
# MAGIC
# MAGIC ## Data Sources
# MAGIC * Policy Admin System → `bronze.policies_raw`
# MAGIC * Claims Management System → `bronze.claims_raw`
# MAGIC * Billing System → `bronze.premiums_raw`
# MAGIC * CRM → `bronze.customers_raw`
# MAGIC * Reference data → `bronze.agents_raw`, `bronze.coverage_codes_raw`
# MAGIC
# MAGIC ## Architecture Pattern
# MAGIC * **ELT**: Extract raw data, load as-is, transform later in Silver
# MAGIC * **Immutable**: Never update Bronze records, append only
# MAGIC * **Schema evolution**: Capture schema changes in raw_payload JSON column

# COMMAND ----------

# DBTITLE 1,Configuration
# Configuration for Bronze pipeline
import datetime

# Catalog and schema
CATALOG = "pc_insurance"
BRONZE_SCHEMA = "bronze"

# Source paths (example - adjust for your cloud storage)
SOURCE_BUCKET = "s3://your-bucket/pc-insurance/raw"
CHECKPOINT_PATH = "/tmp/checkpoints/bronze"

# Ingestion timestamp
ingestion_timestamp = datetime.datetime.now()

print(f"Bronze Pipeline Configuration")
print(f"Catalog: {CATALOG}")
print(f"Schema: {BRONZE_SCHEMA}")
print(f"Source: {SOURCE_BUCKET}")
print(f"Ingestion Time: {ingestion_timestamp}")

# COMMAND ----------

# DBTITLE 1,Sample Data Generation
# Generate sample P&C insurance data for demonstration
from pyspark.sql import functions as F
from pyspark.sql.types import *
import random

# Generate sample policies
num_policies = 1000

policies_data = [
    (
        f"POL-{i:06d}",
        f"PN-{i:08d}",
        random.choice(["Active", "Expired", "Cancelled", "Lapsed"]),
        random.choice(["Auto", "Home", "Property", "Commercial"]),
        random.choice(["Personal Auto", "Homeowners", "Commercial Property", "General Liability"]),
        f"CUST-{random.randint(1, 500):05d}",
        f"AGT-{random.randint(1, 50):03d}",
        (datetime.date(2023, 1, 1) + datetime.timedelta(days=random.randint(0, 365))),
        (datetime.date(2024, 1, 1) + datetime.timedelta(days=random.randint(0, 365))),
        round(random.uniform(500, 5000), 2),
        round(random.uniform(100000, 1000000), 2),
        round(random.uniform(500, 2000), 2),
        random.choice(["CA", "TX", "NY", "FL", "IL"]),
        f"T{random.randint(1, 20):02d}",
        random.randint(0, 5),
        None if random.random() > 0.2 else (datetime.date(2024, 1, 1) + datetime.timedelta(days=random.randint(0, 180))),
        None if random.random() > 0.2 else random.choice(["Non-payment", "Request", "Underwriting"]),
        "policy_admin_system",
        ingestion_timestamp,
        None
    )
    for i in range(1, num_policies + 1)
]

policies_df = spark.createDataFrame(policies_data, schema="""
    policy_id STRING,
    policy_number STRING,
    policy_status STRING,
    policy_type STRING,
    line_of_business STRING,
    customer_id STRING,
    agent_id STRING,
    effective_date DATE,
    expiry_date DATE,
    premium_amount DECIMAL(12,2),
    coverage_limit DECIMAL(12,2),
    deductible DECIMAL(10,2),
    state STRING,
    territory_code STRING,
    endorsement_count INT,
    cancellation_date DATE,
    cancellation_reason STRING,
    source_system STRING,
    ingestion_timestamp TIMESTAMP,
    raw_payload STRING
""")

print(f"Generated {policies_df.count()} sample policies")
policies_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Ingest Policies to Bronze
# Write policies to Bronze layer
policies_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.policies_raw")

print(f"✓ Ingested {policies_df.count()} policies to bronze.policies_raw")

# Verify
result = spark.sql(f"SELECT COUNT(*) as policy_count FROM {CATALOG}.{BRONZE_SCHEMA}.policies_raw")
result.show()

# COMMAND ----------

# DBTITLE 1,Generate Sample Claims Data
# Generate sample claims (about 30% of policies have claims)
num_claims = 300

claims_data = [
    (
        f"CLM-{i:06d}",
        f"CN-{i:08d}",
        f"POL-{random.randint(1, num_policies):06d}",
        f"CUST-{random.randint(1, 500):05d}",
        random.choice(["Auto", "Property", "Liability", "Workers Comp"]),
        random.choice(["Open", "Closed", "Reopened", "Denied", "Pending"]),
        (datetime.date(2023, 6, 1) + datetime.timedelta(days=random.randint(0, 365))),
        (datetime.date(2023, 6, 1) + datetime.timedelta(days=random.randint(0, 400))),
        None if random.random() > 0.6 else (datetime.date(2024, 1, 1) + datetime.timedelta(days=random.randint(0, 180))),
        round(random.uniform(1000, 50000), 2),
        round(random.uniform(500, 40000), 2),
        round(random.uniform(1000, 10000), 2),
        round(random.uniform(500, 5000), 2),
        round(random.uniform(0, 2000), 2),
        round(random.uniform(0, 5000), 2) if random.random() > 0.8 else 0,
        f"ADJ-{random.randint(1, 20):03d}",
        random.choice([True, False]),
        random.choice([True, False]),
        "claims_system",
        ingestion_timestamp,
        None
    )
    for i in range(1, num_claims + 1)
]

claims_df = spark.createDataFrame(claims_data, schema="""
    claim_id STRING,
    claim_number STRING,
    policy_id STRING,
    customer_id STRING,
    claim_type STRING,
    claim_status STRING,
    loss_date DATE,
    report_date DATE,
    close_date DATE,
    incurred_loss DECIMAL(12,2),
    paid_loss DECIMAL(12,2),
    reserved_amount DECIMAL(12,2),
    expense_amount DECIMAL(12,2),
    deductible_applied DECIMAL(10,2),
    subrogation_amount DECIMAL(12,2),
    adjuster_id STRING,
    fraud_flag BOOLEAN,
    litigation_flag BOOLEAN,
    source_system STRING,
    ingestion_timestamp TIMESTAMP,
    raw_payload STRING
""")

print(f"Generated {claims_df.count()} sample claims")
claims_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Ingest Claims to Bronze
# Write claims to Bronze layer
claims_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.claims_raw")

print(f"✓ Ingested {claims_df.count()} claims to bronze.claims_raw")

# Verify
result = spark.sql(f"SELECT COUNT(*) as claim_count FROM {CATALOG}.{BRONZE_SCHEMA}.claims_raw")
result.show()

# COMMAND ----------

# DBTITLE 1,Generate Sample Premiums Data
# Generate premium transactions (one or more per policy)
num_premiums = 1200

premiums_data = [
    (
        f"TRX-{i:08d}",
        f"POL-{random.randint(1, num_policies):06d}",
        f"CUST-{random.randint(1, 500):05d}",
        random.choice(["New Business", "Renewal", "Endorsement", "Cancel"]),
        (datetime.date(2023, 1, 1) + datetime.timedelta(days=random.randint(0, 500))),
        round(random.uniform(500, 5000), 2),
        round(random.uniform(500, 5000), 2),
        round(random.uniform(300, 4000), 2),
        round(random.uniform(100, 1000), 2),
        round(random.uniform(50, 500), 2),
        random.choice(["Annual", "Semi-annual", "Quarterly", "Monthly"]),
        random.choice(["Paid", "Pending", "Overdue"]),
        "billing_system",
        ingestion_timestamp
    )
    for i in range(1, num_premiums + 1)
]

premiums_df = spark.createDataFrame(premiums_data, schema="""
    transaction_id STRING,
    policy_id STRING,
    customer_id STRING,
    transaction_type STRING,
    transaction_date DATE,
    premium_amount DECIMAL(12,2),
    written_premium DECIMAL(12,2),
    earned_premium DECIMAL(12,2),
    unearned_premium DECIMAL(12,2),
    commission_amount DECIMAL(12,2),
    payment_frequency STRING,
    billing_status STRING,
    source_system STRING,
    ingestion_timestamp TIMESTAMP
""")

print(f"Generated {premiums_df.count()} premium transactions")
premiums_df.show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Ingest Premiums to Bronze
# Write premiums to Bronze layer
premiums_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.premiums_raw")

print(f"✓ Ingested {premiums_df.count()} premiums to bronze.premiums_raw")

# Verify
result = spark.sql(f"SELECT COUNT(*) as premium_count FROM {CATALOG}.{BRONZE_SCHEMA}.premiums_raw")
result.show()

# COMMAND ----------

# DBTITLE 1,Summary - Bronze Layer Ingestion Complete
# MAGIC %sql
# MAGIC -- Verify all Bronze tables have data
# MAGIC SELECT 'policies_raw' AS table_name, COUNT(*) AS record_count FROM pc_insurance.bronze.policies_raw
# MAGIC UNION ALL
# MAGIC SELECT 'claims_raw', COUNT(*) FROM pc_insurance.bronze.claims_raw
# MAGIC UNION ALL
# MAGIC SELECT 'premiums_raw', COUNT(*) FROM pc_insurance.bronze.premiums_raw
# MAGIC ORDER BY table_name;

# COMMAND ----------

