# Databricks notebook source
# MAGIC %md
# MAGIC # P&C Insurance — Autonomy Infrastructure Setup
# MAGIC
# MAGIC **Purpose:** Creates the UC objects that enable autonomous swarm operations:
# MAGIC
# MAGIC 1. `pc_insurance.dq.dq_validation_results` — DQ validation result logging
# MAGIC 2. `pc_insurance.metadata.swarm_fix_history` — Swarm fix attempt tracking + circuit breaker
# MAGIC 3. `pc_insurance.metadata.health_monitor_log` — Health monitor event logging
# MAGIC 4. `pc_insurance.dq.pipeline_health_score` — Composite health score function
# MAGIC
# MAGIC **Idempotent:** All objects use `CREATE IF NOT EXISTS` / `CREATE OR REPLACE`.
# MAGIC
# MAGIC **Deployed by:** Job 1 (`PC_Insurance_Agent_Setup`) task `autonomy_infrastructure_setup`

# COMMAND ----------

# Configuration
CATALOG = "pc_insurance"

print(f"Setting up autonomy infrastructure in catalog: {CATALOG}")
print("=" * 70)

# COMMAND ----------

# 1. DQ Validation Results Table
# Stores individual DQ rule validation outcomes for trend analysis

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.dq.dq_validation_results (
        validation_id        STRING      NOT NULL,
        validation_timestamp TIMESTAMP   NOT NULL,
        table_name           STRING      NOT NULL,
        column_name          STRING,
        rule_name            STRING      NOT NULL,
        total_records        BIGINT,
        failed_records        BIGINT,
        dq_score             DOUBLE,
        pass_fail            STRING      NOT NULL,
        run_id               STRING,
        error_details        STRING
    ) USING DELTA
    PARTITIONED BY (table_name)
""")
print("✓ Created dq_validation_results table")

# COMMAND ----------

# 2. Swarm Fix History Table
# Tracks every autonomous fix attempt with circuit breaker support

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metadata.swarm_fix_history (
        fix_id                  STRING      NOT NULL,
        run_id                  STRING,
        trigger_source          STRING,
        error_class             STRING,
        error_message           STRING,
        affected_table          STRING,
        fix_applied             STRING,
        dq_score_before         DOUBLE,
        dq_score_after          DOUBLE,
        resolution_status       STRING      NOT NULL,
        fix_timestamp           TIMESTAMP   NOT NULL,
        swarm_duration_sec       DOUBLE,
        token_cost_usd          DOUBLE,
        rollback_performed      BOOLEAN,
        circuit_breaker_triggered BOOLEAN,
        post_mortem_path        STRING
    ) USING DELTA
    PARTITIONED BY (resolution_status)
""")

# Add circuit_breaker_triggered column if table pre-exists without it (migration)
try:
    spark.sql(f"ALTER TABLE {CATALOG}.metadata.swarm_fix_history ADD COLUMN circuit_breaker_triggered BOOLEAN")
except Exception:
    pass  # Column may already exist

print("✓ Created swarm_fix_history table")

# COMMAND ----------

# 3. Health Monitor Log Table
# Stores periodic health monitoring events from the Health Monitor job

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.metadata.health_monitor_log (
        log_id              STRING      NOT NULL,
        log_timestamp       TIMESTAMP   NOT NULL,
        health_score        DOUBLE,
        stale_table_count   INT,
        dq_pass_rate        DOUBLE,
        swarm_success_rate  DOUBLE,
        rollback_count_24h  INT,
        circuit_breaker_count_24h INT,
        alerts_json         STRING,
        freshness_json      STRING,
        dq_summary_json     STRING,
        swarm_summary_json  STRING
    ) USING DELTA
    PARTITIONED BY (DATE(log_timestamp))
""")
print("✓ Created health_monitor_log table")

# COMMAND ----------

# 4. Pipeline Health Score Function
# Composite score: DQ (40%) + Freshness (25%) + Reconciliation (20%) + Error Rate (15%)

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {CATALOG}.dq.pipeline_health_score(
        dq_score_val               DOUBLE,
        hours_since_last_ingestion  DOUBLE,
        recon_pass_count           BIGINT,
        recon_total_count          BIGINT,
        failed_runs_count          BIGINT,
        total_runs_count           BIGINT
    )
    RETURNS DOUBLE
    LANGUAGE SQL
    RETURN
        LEAST(1.0, GREATEST(0.0,
            -- DQ score weight: 40%
            (COALESCE(dq_score_val, 0.0) * 0.40) +
            -- Freshness weight: 25% (1.0 if <24h, 0.5 if 24-48h, 0.0 if >48h)
            (CASE WHEN hours_since_last_ingestion < 24 THEN 1.0
                  WHEN hours_since_last_ingestion < 48 THEN 0.5
                  ELSE 0.0 END * 0.25) +
            -- Reconciliation pass rate weight: 20%
            (CASE WHEN recon_total_count > 0 THEN CAST(recon_pass_count AS DOUBLE) / recon_total_count ELSE 0.0 END * 0.20) +
            -- Error rate weight: 15% (1.0 - failure rate)
            (CASE WHEN total_runs_count > 0 THEN (1.0 - CAST(failed_runs_count AS DOUBLE) / total_runs_count) ELSE 1.0 END * 0.15)
        ))
""")
print("✓ Created pipeline_health_score function")

# COMMAND ----------

# 5. Verification
print("=" * 70)
print("Autonomy Infrastructure Verification")
print("=" * 70)

# Verify tables
tables = [
    (f"{CATALOG}.dq.dq_validation_results", "DQ Validation Results"),
    (f"{CATALOG}.metadata.swarm_fix_history", "Swarm Fix History"),
    (f"{CATALOG}.metadata.health_monitor_log", "Health Monitor Log"),
]
for full_name, label in tables:
    schema_name = full_name.rsplit('.', 1)[0]
    table_name = full_name.rsplit('.', 1)[1]
    exists = spark.sql(f"SHOW TABLES IN {schema_name}").filter(f"tableName = '{table_name}'").count()
    status = "✓" if exists else "✗"
    print(f"  {status} {label}: {full_name}")

# Verify function
try:
    func_exists = spark.sql(f"SHOW FUNCTIONS IN {CATALOG}.dq").filter("function = 'pipeline_health_score'").count()
except Exception:
    func_exists = 0
status = "✓" if func_exists > 0 else "✗"
print(f"  {status} Pipeline Health Score Function: {CATALOG}.dq.pipeline_health_score")

# Test the function
test_score = spark.sql(f"""
    SELECT {CATALOG}.dq.pipeline_health_score(0.95, 2.0, 98, 100, 1, 100) AS healthy,
           {CATALOG}.dq.pipeline_health_score(0.50, 50.0, 40, 100, 30, 100) AS degraded
""").collect()[0]
print(f"\n  Test — Healthy pipeline score:  {test_score['healthy']}")
print(f"  Test — Degraded pipeline score:  {test_score['degraded']}")

print("=" * 70)
print("Autonomy infrastructure setup complete!")
print("=" * 70)
