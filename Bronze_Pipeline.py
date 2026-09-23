# Databricks notebook source
# ============================================
# BRONZE PIPELINE - Auto Loader Ingestion from CSV Source Files
# ============================================
# 
# This notebook ingests P&C insurance data from CSV source files stored in
# a Unity Catalog volume using Databricks Auto Loader (cloudFiles).
# 
# Source Files (UC Volume: pc_insurance.reference.raw_sources):
#   policies/  <- Policy Admin System exports (policy_admin_system)
#   claims/    <- Claims Management System exports (claims_system)
#   premiums/  <- Billing System exports (billing_system)
#   customers/ <- CRM exports (crm)
#   agents/    <- Agent Admin Portal exports (agent_admin)
#
# Auto Loader monitors each directory for new files and incrementally loads
# them into Bronze Delta tables with ingestion metadata (source_system, 
# ingestion_timestamp).
# ============================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "pc_insurance"
BRONZE = "bronze"
VOLUME = "/Volumes/pc_insurance/reference/raw_sources"

# ============================================
# Source Schemas (matching CSV column structure)
# ============================================
schemas = {
    "policies": StructType([
        StructField("policy_id", StringType(), False), StructField("policy_number", StringType()),
        StructField("policy_status", StringType()), StructField("policy_type", StringType()),
        StructField("line_of_business", StringType()), StructField("customer_id", StringType()),
        StructField("agent_id", StringType()), StructField("effective_date", DateType()),
        StructField("expiry_date", DateType()), StructField("premium_amount", DecimalType(12,2)),
        StructField("coverage_limit", DecimalType(12,2)), StructField("deductible", DecimalType(10,2)),
        StructField("state", StringType()), StructField("territory_code", StringType()),
        StructField("endorsement_count", IntegerType()), StructField("cancellation_date", DateType()),
        StructField("cancellation_reason", StringType()),
    ]),
    "claims": StructType([
        StructField("claim_id", StringType(), False), StructField("claim_number", StringType()),
        StructField("policy_id", StringType()), StructField("customer_id", StringType()),
        StructField("claim_type", StringType()), StructField("claim_status", StringType()),
        StructField("loss_date", DateType()), StructField("report_date", DateType()),
        StructField("close_date", DateType()), StructField("incurred_loss", DecimalType(12,2)),
        StructField("paid_loss", DecimalType(12,2)), StructField("reserved_amount", DecimalType(12,2)),
        StructField("expense_amount", DecimalType(12,2)), StructField("deductible_applied", DecimalType(10,2)),
        StructField("subrogation_amount", DecimalType(12,2)), StructField("adjuster_id", StringType()),
        StructField("fraud_flag", BooleanType()), StructField("litigation_flag", BooleanType()),
    ]),
    "premiums": StructType([
        StructField("transaction_id", StringType(), False), StructField("policy_id", StringType()),
        StructField("customer_id", StringType()), StructField("transaction_type", StringType()),
        StructField("transaction_date", DateType()), StructField("premium_amount", DecimalType(12,2)),
        StructField("written_premium", DecimalType(12,2)), StructField("earned_premium", DecimalType(12,2)),
        StructField("unearned_premium", DecimalType(12,2)), StructField("commission_amount", DecimalType(12,2)),
        StructField("payment_frequency", StringType()), StructField("billing_status", StringType()),
    ]),
    "customers": StructType([
        StructField("customer_id", StringType(), False), StructField("customer_name", StringType()),
        StructField("customer_type", StringType()), StructField("date_of_birth", DateType()),
        StructField("gender", StringType()), StructField("address_line1", StringType()),
        StructField("address_line2", StringType()), StructField("city", StringType()),
        StructField("state", StringType()), StructField("zip_code", StringType()),
        StructField("phone", StringType()), StructField("email", StringType()),
        StructField("credit_score", IntegerType()), StructField("occupation", StringType()),
        StructField("annual_income", DecimalType(12,2)), StructField("years_with_company", IntegerType()),
    ]),
    "agents": StructType([
        StructField("agent_id", StringType(), False), StructField("agent_name", StringType()),
        StructField("agency_name", StringType()), StructField("agent_license_number", StringType()),
        StructField("license_state", StringType()), StructField("agent_status", StringType()),
        StructField("commission_rate", DecimalType(5,2)), StructField("appointment_date", DateType()),
    ]),
}

source_systems = {
    "policies": "policy_admin_system",
    "claims": "claims_system",
    "premiums": "billing_system",
    "customers": "crm",
    "agents": "agent_admin",
}

# ============================================
# Auto Loader Ingestion (batch mode via trigger(availableNow=True))
# ============================================
print("=== Bronze Pipeline: Auto Loader Ingestion ===")
print(f"Source: UC Volume {VOLUME}")
print(f"Target: {CATALOG}.{BRONZE}.*_raw tables")
print()

for source_name, schema in schemas.items():
    table_name = source_name + "_raw"
    source_dir = VOLUME + "/" + source_name + "/"
    full_table = CATALOG + "." + BRONZE + "." + table_name
    
    print(f"[{table_name}] Loading from {source_dir}")
    
    # Auto Loader: reads CSV files from source directory
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", VOLUME + "/_schemas/" + source_name)
        .option("header", "true")
        .schema(schema)
        .load(source_dir)
        .withColumn("source_system", F.lit(source_systems[source_name]))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )
    
    # Write to Bronze table (batch mode - processes available files and stops)
    query = (df.writeStream
        .format("delta")
        .option("mergeSchema", "true")
        .option("checkpointLocation", VOLUME + "/_checkpoints/" + source_name)
        .trigger(availableNow=True)
        .toTable(full_table)
    )
    
    query.awaitTermination()
    count = spark.sql("SELECT COUNT(*) FROM " + full_table).collect()[0][0]
    print(f"  -> {count} rows ingested from {source_systems[source_name]}")

print()
print("=== Bronze Pipeline Complete ===")
print("All source files ingested via Auto Loader.")
print("To add new data: drop CSV files into the source directories and re-run this notebook.")
