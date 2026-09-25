# Databricks notebook source
# ============================================
# BRONZE PIPELINE - Metadata-Driven Auto Loader Ingestion
# ============================================
# 
# ARCHITECTURE:
#   Source CSV files (UC Volume) → Auto Loader → Staging Tables → Promotion → Bronze Tables
#                                        ↑                         ↓
#                                   Config Table              Audit + Reconciliation
#
# METADATA TABLES:
#   pc_insurance.reference.bronze_ingestion_config  - Defines all sources, targets, schemas
#   pc_insurance.reference.bronze_load_audit         - Tracks every load execution
#   pc_insurance.reference.bronze_reconciliation     - Source-to-target row count validation
#
# STAGING TABLES:
#   pc_insurance.bronze.stg_policies, stg_claims, stg_premiums, stg_customers, stg_agents
#   Each has _load_id and _file_name metadata columns for traceability
#
# LOAD TYPES:
#   INITIAL    - Truncate target + staging, load all files, full refresh
#   INCREMENTAL - Truncate staging only, load new files, append to target
#
# PIPELINE FLOW (per source):
#   1. Read config → get source definition
#   2. Create audit record (load_id, start_time)
#   3. Truncate staging (and target if INITIAL)
#   4. Auto Loader: source dir → staging table
#   5. Count source files, staging rows, target before
#   6. Promote: staging → target (overwrite/append)
#   7. Count target after
#   8. Reconcile: staging_count vs (target_after - target_before)
#   9. Update audit + reconciliation records
# ============================================

import json, uuid
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "pc_insurance"
BRONZE = "bronze"
REF = "reference"
VOLUME = "/Volumes/pc_insurance/reference/raw_sources"

# ============================================
# Notebook Parameters (Widgets)
# ============================================
dbutils.widgets.dropdown("load_type", "INCREMENTAL", ["INITIAL", "INCREMENTAL"], "Load Type")
dbutils.widgets.text("source_filter", "", "Source Filter (comma-separated, empty=all)")

LOAD_TYPE = dbutils.widgets.get("load_type")
SOURCE_FILTER = dbutils.widgets.get("source_filter")

print(f"=== Bronze Pipeline: {LOAD_TYPE} Load ===")
print(f"Source filter: {SOURCE_FILTER if SOURCE_FILTER else 'ALL'}")
print()

# ============================================
# Helper: Parse schema JSON to StructType
# ============================================
def parse_schema(schema_json_str):
    """Convert JSON schema definition to Spark StructType."""
    schema_def = json.loads(schema_json_str)
    fields = []
    for f in schema_def["fields"]:
        name = f["name"]
        nullable = f.get("nullable", True)
        type_str = f["type"]
        
        if type_str == "string":
            dt = StringType()
        elif type_str == "date":
            dt = DateType()
        elif type_str == "boolean":
            dt = BooleanType()
        elif type_str == "integer":
            dt = IntegerType()
        elif type_str.startswith("decimal"):
            parts = type_str.replace("decimal(", "").replace(")", "").split(",")
            precision, scale = int(parts[0]), int(parts[1])
            dt = DecimalType(precision, scale)
        else:
            dt = StringType()
        
        fields.append(StructField(name, dt, nullable))
    return StructType(fields)

# ============================================
# Helper: Insert audit record
# ============================================
def insert_audit(load_id, source_name, load_type, start_time):
    spark.sql(f"""
        INSERT INTO {CATALOG}.{REF}.bronze_load_audit
        (load_id, source_name, load_type, load_start_time, status)
        VALUES ('{load_id}', '{source_name}', '{load_type}', timestamp('{start_time}'), 'RUNNING')
    """)

def update_audit(load_id, end_time, source_file_count, source_row_count, 
                 staging_row_count, target_before, target_after, 
                 rows_inserted, status, error_msg=""):
    error_escaped = error_msg.replace("'", "''")
    spark.sql(f"""
        UPDATE {CATALOG}.{REF}.bronze_load_audit
        SET load_end_time = timestamp('{end_time}'),
            source_file_count = {source_file_count},
            source_row_count = {source_row_count},
            staging_row_count = {staging_row_count},
            target_row_count_before = {target_before},
            target_row_count_after = {target_after},
            rows_inserted = {rows_inserted},
            status = '{status}',
            error_message = '{error_escaped}'
        WHERE load_id = '{load_id}'
    """)

def insert_reconciliation(recon_id, load_id, source_name, file_name, 
                          source_count, staging_count, target_count, 
                          expected_count, status, details=""):
    details_escaped = details.replace("'", "''")
    spark.sql(f"""
        INSERT INTO {CATALOG}.{REF}.bronze_reconciliation
        (reconciliation_id, load_id, source_name, source_file_name,
         source_row_count, staging_row_count, target_row_count,
         expected_target_count, match_status, mismatch_details, reconciled_at)
        VALUES ('{recon_id}', '{load_id}', '{source_name}', '{file_name}',
                {source_count}, {staging_count}, {target_count},
                {expected_count}, '{status}', '{details_escaped}', current_timestamp())
    """)

# ============================================
# Step 1: Read ingestion config
# ============================================
config_df = spark.table(f"{CATALOG}.{REF}.bronze_ingestion_config").filter("is_active = true")

if SOURCE_FILTER:
    source_list = [s.strip() for s in SOURCE_FILTER.split(",")]
    config_df = config_df.filter(F.col("source_name").isin(source_list))

configs = config_df.orderBy("load_order").collect()
print(f"Found {len(configs)} active sources to process")
print()

# ============================================
# Step 2: Process each source
# ============================================
results = []

for cfg in configs:
    source_name = cfg["source_name"]
    source_dir = cfg["source_directory"]
    target_table = cfg["target_table"]
    staging_table = cfg["staging_table"]
    source_system = cfg["source_system"]
    file_format = cfg["file_format"]
    schema_json = cfg["schema_json"]
    
    load_id = str(uuid.uuid4())
    start_time = datetime.now()
    
    print(f"--- [{source_name}] {LOAD_TYPE} load (ID: {load_id[:8]}...) ---")
    
    try:
        # Insert audit record
        insert_audit(load_id, source_name, LOAD_TYPE, start_time)
        
        # Parse schema from config
        schema = parse_schema(schema_json)
        
        # Step 2a: Truncate staging table
        spark.sql(f"TRUNCATE TABLE {staging_table}")
        print(f"  ✓ Staging truncated: {staging_table.split('.')[-1]}")
        
        # Step 2b: For INITIAL load, truncate target and clear Auto Loader checkpoints
        target_before = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
        if LOAD_TYPE == "INITIAL":
            spark.sql(f"TRUNCATE TABLE {target_table}")
            target_before = 0
            print(f"  ✓ Target truncated (INITIAL): {target_table.split('.')[-1]}")
            # Clear Auto Loader checkpoints so all source files are re-processed
            checkpoint_path = f"{VOLUME}/_checkpoints/{source_name}"
            try:
                dbutils.fs.rm(checkpoint_path, True)
                print(f"  ✓ Checkpoints cleared (INITIAL): {source_name}")
            except Exception:
                pass  # No checkpoint to clear yet
        
        # Step 2c: Count source files
        try:
            source_files = dbutils.fs.ls(source_dir)
            source_file_count = len([f for f in source_files if f.name.endswith(f".{file_format}")])
        except:
            source_file_count = 0
        
        # Step 2d: Auto Loader → Staging table (UC-compatible: _metadata.file_path)
        df = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", f"{VOLUME}/_schemas/{source_name}")
            .option("header", "true")
            .schema(schema)
            .load(source_dir)
            .withColumn("source_system", F.lit(source_system))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("_load_id", F.lit(load_id))
            .withColumn("_file_name", F.col("_metadata.file_path"))
        )
        
        query = (df.writeStream
            .format("delta")
            .option("mergeSchema", "true")
            .option("checkpointLocation", f"{VOLUME}/_checkpoints/{source_name}")
            .trigger(availableNow=True)
            .toTable(staging_table)
        )
        query.awaitTermination()
        
        # Step 2e: Count staging rows
        staging_count = spark.sql(f"SELECT COUNT(*) FROM {staging_table}").collect()[0][0]
        print(f"  ✓ Staging loaded: {staging_count} rows from {source_file_count} files")
        
        # Step 2f: Promote staging → target
        if LOAD_TYPE == "INITIAL":
            # Overwrite target with staging data (minus metadata columns)
            promote_df = spark.table(staging_table).drop("_load_id", "_file_name")
            promote_df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true").saveAsTable(target_table)
        else:
            # Append staging data to target (minus metadata columns)
            promote_df = spark.table(staging_table).drop("_load_id", "_file_name")
            promote_df.write.format("delta").mode("append").saveAsTable(target_table)
        
        # Step 2g: Count target after
        target_after = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
        rows_inserted = target_after - target_before
        print(f"  ✓ Target promoted: {target_before} → {target_after} ({rows_inserted} new)")
        
        # Step 2h: Reconciliation
        expected_count = target_before + staging_count
        if target_after == expected_count:
            match_status = "MATCH"
            mismatch = ""
        else:
            match_status = "MISMATCH"
            mismatch = f"Expected {expected_count}, got {target_after}"
        
        recon_id = str(uuid.uuid4())
        insert_reconciliation(recon_id, load_id, source_name, f"{source_file_count} files",
                             staging_count, staging_count, target_after, expected_count,
                             match_status, mismatch)
        print(f"  ✓ Reconciliation: {match_status}")
        
        # Step 2i: Update audit
        end_time = datetime.now()
        update_audit(load_id, end_time, source_file_count, staging_count,
                     staging_count, target_before, target_after, rows_inserted, "SUCCESS")
        
        duration = (end_time - start_time).total_seconds()
        print(f"  ✓ Audit recorded ({duration:.1f}s)")
        
        results.append((source_name, "SUCCESS", staging_count, rows_inserted, match_status))
        
    except Exception as e:
        end_time = datetime.now()
        update_audit(load_id, end_time, 0, 0, 0, 0, 0, 0, "FAILED", str(e))
        print(f"  ✗ FAILED: {str(e)[:100]}")
        results.append((source_name, "FAILED", 0, 0, "ERROR"))
    
    print()

# ============================================
# Step 3: Summary Report
# ============================================
print("=" * 60)
print(f"BRONZE PIPELINE SUMMARY ({LOAD_TYPE} LOAD)")
print("=" * 60)
print(f"{'Source':<12} {'Status':<10} {'Staged':>8} {'Inserted':>10} {'Recon':<10}")
print("-" * 60)
total_staged = 0
total_inserted = 0
all_match = True
for r in results:
    print(f"{r[0]:<12} {r[1]:<10} {r[2]:>8} {r[3]:>10} {r[4]:<10}")
    total_staged += r[2]
    total_inserted += r[3]
    if r[4] != "MATCH":
        all_match = False
print("-" * 60)
print(f"{'TOTAL':<12} {'':<10} {total_staged:>8} {total_inserted:>10}")
print()
if all_match:
    print("✅ All sources reconciled successfully!")
else:
    print("⚠ Some sources had reconciliation mismatches - check bronze_reconciliation table")

# Show recent audit records
print("\n--- Recent Load Audit ---")
spark.sql(f"""
    SELECT substring(load_id, 1, 13) as load_id, source_name, load_type, status,
           source_row_count, target_row_count_before, target_row_count_after, rows_inserted
    FROM {CATALOG}.{REF}.bronze_load_audit
    ORDER BY load_start_time DESC
    LIMIT 5
""").show(truncate=False)

print("--- Recent Reconciliation ---")
spark.sql(f"""
    SELECT source_name, match_status, source_row_count, 
           target_row_count, expected_target_count
    FROM {CATALOG}.{REF}.bronze_reconciliation
    ORDER BY reconciled_at DESC
    LIMIT 5
""").show(truncate=False)