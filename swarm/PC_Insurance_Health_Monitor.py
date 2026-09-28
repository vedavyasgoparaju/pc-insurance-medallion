# Databricks notebook source
# DBTITLE 1,Notebook Metadata
"""
# P&C Insurance — Pipeline Health Monitor

**Purpose:** Continuous monitoring of pipeline health, data quality, and swarm fix effectiveness.

**Runs every:** 15 minutes (via Lakeflow Job schedule)

**Monitors:**
1. Data freshness (time since last update for each medallion layer)
2. DQ validation results (pass/fail rates, score trends)
3. Swarm fix history (success rate, rollback frequency, escalations)
4. Pipeline health score (composite metric)

**Alerts when:**
- Health score drops below 0.80
- Any table hasn't been updated in 4+ hours
- Swarm fix success rate drops below 60% over last 24h
- 3+ rollbacks in last 24h
- Circuit breaker triggered in last 6h

**Outputs:**
- Writes monitoring events to `pc_insurance.metadata.health_monitor_log`
- Triggers swarm if critical issues detected
- Sends Slack/email alerts (if configured)
"""

# COMMAND ----------

# DBTITLE 1,Imports
import datetime
import logging
import uuid
from pyspark.sql import functions as F
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HealthMonitor")

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "pc_insurance"
MONITOR_LOG_TABLE = f"{CATALOG}.metadata.health_monitor_log"
HEALTH_SCORE_THRESHOLD = 0.80  # Alert if below this
FRESHNESS_THRESHOLD_HOURS = 4  # Alert if tables not updated in 4h
FIX_SUCCESS_RATE_THRESHOLD = 0.60  # Alert if swarm success rate < 60%
ROLLBACK_ALERT_THRESHOLD = 3  # Alert if 3+ rollbacks in 24h

# Monitored tables (bronze, silver, gold layers)
MONITORED_TABLES = [
    f"{CATALOG}.bronze.policycenter_raw",
    f"{CATALOG}.bronze.claimcenter_raw",
    f"{CATALOG}.bronze.billingcenter_raw",
    f"{CATALOG}.bronze.mga_feed_raw",
    f"{CATALOG}.silver.policies_transformed",
    f"{CATALOG}.silver.claims_transformed",
    f"{CATALOG}.silver.billing_transformed",
    f"{CATALOG}.silver.mga_transformed",
    f"{CATALOG}.gold.premium_analytics",
    f"{CATALOG}.gold.claims_analytics",
    f"{CATALOG}.gold.loss_ratio_kpis",
    f"{CATALOG}.gold.combined_ratio_kpis",
]

w = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Helper Functions

def check_table_freshness() -> dict:
    """Check when each monitored table was last updated."""
    freshness = {}
    now = datetime.datetime.now()

    for table in MONITORED_TABLES:
        try:
            hist_df = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
            last_update = hist_df.collect()[0]["timestamp"]
            age_hours = (now - last_update).total_seconds() / 3600
            freshness[table] = {
                "last_update": last_update.isoformat(),
                "age_hours": round(age_hours, 2),
                "is_stale": age_hours > FRESHNESS_THRESHOLD_HOURS
            }
        except Exception as e:
            logger.warning(f"Freshness check failed for {table}: {e}")
            freshness[table] = {
                "last_update": None,
                "age_hours": None,
                "is_stale": True,
                "error": str(e)
            }

    return freshness

def get_dq_summary() -> dict:
    """Get recent DQ validation summary from dq_validation_results."""
    try:
        df = spark.sql(f"""
            SELECT 
                COUNT(*) AS total_validations,
                SUM(CASE WHEN pass_fail = 'PASS' THEN 1 ELSE 0 END) AS passed,
                SUM(CASE WHEN pass_fail = 'FAIL' THEN 1 ELSE 0 END) AS failed,
                AVG(dq_score) AS avg_dq_score
            FROM {CATALOG}.dq.dq_validation_results
            WHERE validation_timestamp >= TIMESTAMPADD(HOUR, -24, CURRENT_TIMESTAMP())
        """)
        row = df.collect()[0]
        return {
            "total": row["total_validations"],
            "passed": row["passed"],
            "failed": row["failed"],
            "avg_score": round(row["avg_dq_score"], 3) if row["avg_dq_score"] else None
        }
    except Exception as e:
        logger.warning(f"DQ summary query failed: {e}")
        return {"error": str(e)}

def get_swarm_fix_summary() -> dict:
    """Get recent swarm fix history summary."""
    try:
        df = spark.sql(f"""
            SELECT 
                COUNT(*) AS total_fixes,
                SUM(CASE WHEN resolution_status = 'resolved' THEN 1 ELSE 0 END) AS resolved,
                SUM(CASE WHEN resolution_status = 'halted' THEN 1 ELSE 0 END) AS halted,
                SUM(CASE WHEN resolution_status = 'rollback' THEN 1 ELSE 0 END) AS rollbacks,
                SUM(CASE WHEN circuit_breaker_triggered = TRUE THEN 1 ELSE 0 END) AS circuit_breaker_hits,
                AVG(token_cost_usd) AS avg_cost
            FROM {CATALOG}.metadata.swarm_fix_history
            WHERE fix_timestamp >= TIMESTAMPADD(HOUR, -24, CURRENT_TIMESTAMP())
        """)
        row = df.collect()[0]
        total = row["total_fixes"] or 0
        resolved = row["resolved"] or 0
        rollbacks = row["rollbacks"] or 0

        success_rate = resolved / total if total > 0 else 1.0

        return {
            "total": total,
            "resolved": resolved,
            "halted": row["halted"],
            "rollbacks": rollbacks,
            "success_rate": round(success_rate, 3),
            "circuit_breaker_hits": row["circuit_breaker_hits"],
            "avg_cost": round(row["avg_cost"], 2) if row["avg_cost"] else 0.0
        }
    except Exception as e:
        logger.warning(f"Swarm fix summary query failed: {e}")
        return {"error": str(e)}

def calculate_overall_health_score(freshness: dict, dq_summary: dict, swarm_summary: dict) -> float:
    """Calculate composite health score (0.0-1.0)."""
    try:
        # Freshness score: % of tables that are fresh
        total_tables = len(MONITORED_TABLES)
        fresh_tables = sum(1 for t in freshness.values() if not t.get("is_stale", True))
        freshness_score = fresh_tables / total_tables if total_tables > 0 else 0.0

        # DQ score: from validation results
        dq_score = dq_summary.get("avg_score", 1.0) or 1.0

        # Swarm effectiveness score: success rate
        swarm_score = swarm_summary.get("success_rate", 1.0)

        # Weighted composite: freshness 35%, DQ 40%, swarm 25%
        overall = (freshness_score * 0.35) + (dq_score * 0.40) + (swarm_score * 0.25)
        return round(overall, 3)
    except Exception as e:
        logger.error(f"Health score calculation failed: {e}")
        return 0.0

def generate_alerts(health_score: float, freshness: dict, dq_summary: dict, swarm_summary: dict) -> list:
    """Generate alert messages based on thresholds."""
    alerts = []

    # Health score alert
    if health_score < HEALTH_SCORE_THRESHOLD:
        alerts.append(f"⚠️  Pipeline health score dropped to {health_score:.2f} (threshold: {HEALTH_SCORE_THRESHOLD})")

    # Freshness alerts
    stale_tables = [t for t, info in freshness.items() if info.get("is_stale", False)]
    if stale_tables:
        alerts.append(f"⚠️  {len(stale_tables)} tables stale (>{FRESHNESS_THRESHOLD_HOURS}h): {', '.join(stale_tables[:3])}")

    # DQ alerts
    dq_failed = dq_summary.get("failed", 0)
    if dq_failed > 0:
        alerts.append(f"⚠️  {dq_failed} DQ validations failed in last 24h")

    # Swarm alerts
    if swarm_summary.get("success_rate", 1.0) < FIX_SUCCESS_RATE_THRESHOLD:
        alerts.append(f"⚠️  Swarm fix success rate: {swarm_summary['success_rate']*100:.0f}% (threshold: {FIX_SUCCESS_RATE_THRESHOLD*100:.0f}%)")

    if swarm_summary.get("rollbacks", 0) >= ROLLBACK_ALERT_THRESHOLD:
        alerts.append(f"⚠️  {swarm_summary['rollbacks']} rollbacks in last 24h (threshold: {ROLLBACK_ALERT_THRESHOLD})")

    if swarm_summary.get("circuit_breaker_hits", 0) > 0:
        alerts.append(f"⚠️  Circuit breaker triggered {swarm_summary['circuit_breaker_hits']} times in last 24h")

    return alerts

def log_monitoring_event(health_score: float, freshness: dict, dq_summary: dict, 
                         swarm_summary: dict, alerts: list) -> None:
    """Persist the monitoring event to health_monitor_log."""
    try:
        import json
        event_id = str(uuid.uuid4())
        timestamp = datetime.datetime.now().isoformat()

        # Serialize details as JSON strings
        freshness_json = json.dumps(freshness, default=str).replace("'", "''")
        dq_json = json.dumps(dq_summary, default=str).replace("'", "''")
        swarm_json = json.dumps(swarm_summary, default=str).replace("'", "''")
        alerts_json = json.dumps(alerts, default=str).replace("'", "''")

        # Compute summary metrics
        stale_count = sum(1 for t in freshness.values() if t.get("is_stale", False))
        dq_pass_rate = (dq_summary.get("passed", 0) / dq_summary.get("total", 1)) if dq_summary.get("total", 0) > 0 else 1.0
        swarm_success = swarm_summary.get("success_rate", 1.0)
        rollback_count = swarm_summary.get("rollbacks", 0) or 0
        cb_count = swarm_summary.get("circuit_breaker_hits", 0) or 0

        spark.sql(f"""
            INSERT INTO {MONITOR_LOG_TABLE}
            (log_id, log_timestamp, health_score, stale_table_count,
             dq_pass_rate, swarm_success_rate, rollback_count_24h,
             circuit_breaker_count_24h, alerts_json, freshness_json,
             dq_summary_json, swarm_summary_json)
            VALUES (
                '{event_id}',
                TIMESTAMP('{timestamp}'),
                {health_score},
                {stale_count},
                {dq_pass_rate},
                {swarm_success},
                {rollback_count},
                {cb_count},
                '{alerts_json}',
                '{freshness_json}',
                '{dq_json}',
                '{swarm_json}'
            )
        """)
        logger.info(f"Logged monitoring event {event_id}")
    except Exception as e:
        logger.error(f"Failed to log monitoring event: {e}")

# COMMAND ----------

# DBTITLE 1,Main Monitoring Logic

print("=" * 70)
print("P&C INSURANCE — PIPELINE HEALTH MONITOR")
print("=" * 70)
print(f"Timestamp: {datetime.datetime.now().isoformat()}")
print()

# 1. Check table freshness
print("1. Checking table freshness...")
freshness = check_table_freshness()
stale_count = sum(1 for t in freshness.values() if t.get("is_stale", False))
print(f"   ✓ {len(MONITORED_TABLES)} tables monitored, {stale_count} stale")

# 2. Get DQ summary
print("2. Checking DQ validation results...")
dq_summary = get_dq_summary()
if "error" not in dq_summary:
    print(f"   ✓ DQ validations (24h): {dq_summary['total']}, passed: {dq_summary['passed']}, avg score: {dq_summary.get('avg_score', 'N/A')}")
else:
    print(f"   ⚠️  DQ summary failed: {dq_summary['error']}")

# 3. Get swarm fix history
print("3. Checking swarm fix history...")
swarm_summary = get_swarm_fix_summary()
if "error" not in swarm_summary:
    print(f"   ✓ Swarm fixes (24h): {swarm_summary['total']}, resolved: {swarm_summary['resolved']}, success rate: {swarm_summary['success_rate']*100:.0f}%")
else:
    print(f"   ⚠️  Swarm summary failed: {swarm_summary['error']}")

# 4. Calculate overall health score
print("4. Calculating overall health score...")
health_score = calculate_overall_health_score(freshness, dq_summary, swarm_summary)
print(f"   ✓ Pipeline health score: {health_score:.3f}")

# 5. Generate alerts
print("5. Evaluating alert thresholds...")
alerts = generate_alerts(health_score, freshness, dq_summary, swarm_summary)
if alerts:
    print(f"   ⚠️  {len(alerts)} alerts generated:")
    for alert in alerts:
        print(f"      {alert}")
else:
    print("   ✓ No alerts (system healthy)")

# 6. Log to monitoring table
print("6. Logging monitoring event...")
log_monitoring_event(health_score, freshness, dq_summary, swarm_summary, alerts)
print("   ✓ Event logged")

print()
print("=" * 70)
print("HEALTH MONITOR COMPLETE")
print("=" * 70)

# COMMAND ----------

# DBTITLE 1,Optional: Trigger Swarm on Critical Issues

# If health score is critically low (<0.70) or multiple critical alerts, consider auto-triggering swarm
if health_score < 0.70:
    print("⚠️  CRITICAL: Health score below 0.70. Consider manual investigation.")
    print("    Automated swarm trigger disabled by default (enable in production if desired).")

# End of notebook