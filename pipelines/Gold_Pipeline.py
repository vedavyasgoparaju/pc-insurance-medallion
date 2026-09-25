# Databricks notebook source


# COMMAND ----------

# DBTITLE 1,Gold Pipeline - P&C Insurance KPIs
# MAGIC %md
# MAGIC # Gold Layer Pipeline - P&C Insurance KPIs
# MAGIC
# MAGIC Builds business-ready aggregations from Silver layer:
# MAGIC * **Loss Ratio** = Incurred Losses / Earned Premium
# MAGIC * **Combined Ratio** = (Losses + Expenses) / Earned Premium
# MAGIC * **Claim Frequency** = Claim Count / Exposure Units
# MAGIC * **Claim Severity** = Incurred Losses / Claim Count
# MAGIC * **Retention Rate** = Renewed / (Renewed + Cancelled)
# MAGIC * **Premium Growth** = YoY premium change
# MAGIC
# MAGIC All Gold tables are at **monthly** and **quarterly** grain by line of business.

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
import datetime
import uuid

CATALOG = "pc_insurance"
SILVER = "silver"
GOLD = "gold"

now = F.current_timestamp()
print("Gold Pipeline started at:", datetime.datetime.now())

# Gold outputs, dimensions, formulas, and refresh order are controlled by metadata.
gold_configs = {
    row["metric_name"]: row.asDict()
    for row in spark.table(f"{CATALOG}.reference.gold_metric_config")
    .filter("is_active = true")
    .orderBy("load_order")
    .collect()
}

def persist_gold(metric_name, dataframe):
    config = gold_configs.get(metric_name)
    if not config:
        print(f"Skipping inactive or unconfigured metric: {metric_name}")
        return False
    (dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(config["output_table"]))
    print(f"Persisted {metric_name} to {config['output_table']}")
    return True

def record_gold_audit():
    rows = []
    for metric_name, config in gold_configs.items():
        source_count = 0
        for source in (config["source_tables"] or "").split(","):
            source = source.strip()
            if source:
                source_name = source if source.count(".") == 2 else f"{CATALOG}.{source}"
                source_count += spark.table(source_name).count()
        target_count = spark.table(config["output_table"]).count()
        rows.append((
            str(uuid.uuid4()), metric_name, config["output_table"],
            datetime.datetime.now(), datetime.datetime.now(), source_count,
            target_count, "SUCCESS", ""
        ))
    if rows:
        spark.createDataFrame(rows, [
            "load_id", "metric_name", "output_table", "load_start_time",
            "load_end_time", "source_row_count", "target_row_count", "status",
            "error_message"
        ]).write.mode("append").saveAsTable(f"{CATALOG}.reference.gold_load_audit")

# COMMAND ----------

# DBTITLE 1,Gold: Loss Ratio by LOB
# ============================================
# 1. GOLD: Loss Ratio by Line of Business (Quarterly)
# ============================================

premium_fact = spark.table(f"{CATALOG}.{SILVER}.premium_fact")
claim_fact = spark.table(f"{CATALOG}.{SILVER}.claim_fact")
date_dim = spark.table(f"{CATALOG}.{SILVER}.date_dim")
customer_dim = spark.table(f"{CATALOG}.{SILVER}.customer_dim")

# Aggregate earned premium by quarter and LOB
earned_premium_agg = (
    premium_fact
    .join(date_dim, premium_fact["transaction_date_id"] == date_dim["date_id"], how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business")
    )
    .agg(
        F.sum("net_premium").alias("earned_premium"),
        F.sum("premium_amount").alias("written_premium"),
        F.sum("commission_amount").alias("expense_amount"),
        F.countDistinct("policy_id").alias("policy_count")
    )
)

# Aggregate incurred losses by quarter and LOB (using loss_date)
incurred_loss_agg = (
    claim_fact
    .join(
        date_dim.select(F.col("date_id").alias("loss_date_id_dim"), F.col("year"), F.col("quarter")),
        claim_fact["loss_date_id"] == F.col("loss_date_id_dim"), how="left"
    )
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.col("line_of_business")
    )
    .agg(
        F.sum("incurred_amount").alias("incurred_losses"),
        F.sum("reserve_amount").alias("claim_expense"),
        F.countDistinct("claim_id").alias("claim_count")
    )
)

# Join premium and loss aggregations
gold_loss_ratio = (
    earned_premium_agg
    .join(incurred_loss_agg, on=["reporting_period", "line_of_business"], how="left")
    .fillna(0, subset=["incurred_losses", "claim_count", "claim_expense"])
    .withColumn("expense_amount", F.col("expense_amount") + F.coalesce(F.col("claim_expense"), F.lit(0)))
    .withColumn("loss_ratio", 
        F.when(F.col("earned_premium") > 0, F.col("incurred_losses") / F.col("earned_premium"))
        .otherwise(0))
    .withColumn("expense_ratio",
        F.when(F.col("earned_premium") > 0, F.col("expense_amount") / F.col("earned_premium"))
        .otherwise(0))
    .withColumn("combined_ratio", F.col("loss_ratio") + F.col("expense_ratio"))
    .withColumn("loaded_at", now)
    .select(
        "reporting_period", "period_type", "line_of_business",
        "earned_premium", "written_premium", "incurred_losses", "expense_amount",
        "loss_ratio", "expense_ratio", "combined_ratio",
        "policy_count", "claim_count", "loaded_at"
    )
)

persist_gold("loss_ratio_by_lob", gold_loss_ratio)

print(f"✓ Gold loss_ratio_by_lob loaded: {gold_loss_ratio.count()} records")
gold_loss_ratio.orderBy(F.desc("reporting_period")).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Gold: Claim Frequency & Severity
# ============================================
# 2. GOLD: Claim Frequency & Severity
# ============================================

# Calculate exposure (policy count as proxy for exposure units)
exposure_agg = (
    premium_fact
    .join(date_dim, premium_fact["transaction_date_id"] == date_dim["date_id"], how="left")
    .join(customer_dim.select("customer_id", "state"), on="customer_id", how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business"),
        F.col("state")
    )
    .agg(
        F.countDistinct("policy_id").alias("exposure_units"),
        F.sum("net_premium").alias("total_earned_premium")
    )
)

# Aggregate claim counts and losses
claim_freq_agg = (
    claim_fact
    .join(
        date_dim.select(F.col("date_id").alias("claim_date_id_dim"), F.col("year"), F.col("quarter")),
        claim_fact["loss_date_id"] == F.col("claim_date_id_dim"), how="left"
    )
    .join(customer_dim.select("customer_id", "state"), on="customer_id", how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.col("line_of_business"),
        F.col("state")
    )
    .agg(
        F.countDistinct("claim_id").alias("claim_count"),
        F.sum("incurred_amount").alias("incurred_losses"),
        F.avg("paid_amount").alias("avg_paid_loss"),
        F.avg("reserve_amount").alias("avg_reserved")
    )
)

gold_freq_severity = (
    exposure_agg
    .join(claim_freq_agg, on=["reporting_period", "line_of_business", "state"], how="left")
    .fillna(0, subset=["claim_count", "incurred_losses", "avg_paid_loss", "avg_reserved"])
    .withColumn("claim_frequency",
        F.when(F.col("exposure_units") > 0, F.col("claim_count") / F.col("exposure_units"))
        .otherwise(0))
    .withColumn("claim_severity",
        F.when(F.col("claim_count") > 0, F.col("incurred_losses") / F.col("claim_count"))
        .otherwise(0))
    .withColumn("loaded_at", now)
    .select(
        "reporting_period", "period_type", "line_of_business", "state",
        "exposure_units", "claim_count", "claim_frequency",
        "incurred_losses", "claim_severity", "avg_paid_loss", "avg_reserved", "loaded_at"
    )
)

persist_gold("claim_frequency_severity", gold_freq_severity)

print(f"✓ Gold claim_frequency_severity loaded: {gold_freq_severity.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: Retention by Agent
# ============================================
# 3. GOLD: Retention Rate by Agent
# ============================================

policy_dim = spark.table(f"{CATALOG}.{SILVER}.policy_dim")
agent_dim = spark.table(f"{CATALOG}.{SILVER}.agent_dim")
date_dim_for_policy = spark.table(f"{CATALOG}.{SILVER}.date_dim")

# Classify policies by status
retention_data = (
    policy_dim
    .join(agent_dim, on="agent_id", how="left")
        .join(date_dim_for_policy,
            policy_dim["effective_date"] == date_dim_for_policy["full_date"], how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("agent_id"),
        F.col("agent_name"),
        F.col("agent_name").alias("agency_name")
    )
    .agg(
        F.count("*").alias("total_policies"),
        F.sum(F.when(F.col("status") == "Active", 1).otherwise(0)).alias("active_policies"),
        F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled_policies"),
        F.sum(F.when(F.col("status") == "Active", 1).otherwise(0)).alias("new_business_policies"),
        F.sum("coverage_amount").alias("total_written_premium")
    )
    .withColumn("renewed_policies", F.col("active_policies") - F.col("new_business_policies"))
    .withColumn("retention_rate",
        F.when((F.col("renewed_policies") + F.col("cancelled_policies")) > 0,
               F.col("renewed_policies") / (F.col("renewed_policies") + F.col("cancelled_policies")))
        .otherwise(0))
    .withColumn("new_business_growth",
        F.when(F.col("total_policies") > 0, F.col("new_business_policies") / F.col("total_policies"))
        .otherwise(0))
    .withColumn("loaded_at", now)
    .select(
        "reporting_period", "period_type", "agent_id", "agent_name", "agency_name",
        "total_policies", "renewed_policies", "cancelled_policies",
        "new_business_policies", "retention_rate", "new_business_growth",
        "total_written_premium", "loaded_at"
    )
)

persist_gold("retention_by_agent", retention_data)

print(f"✓ Gold retention_by_agent loaded: {retention_data.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: Premium Growth
# ============================================
# 4. GOLD: Premium Growth Summary
# ============================================

gold_premium_growth = (
    premium_fact
    .join(date_dim, premium_fact["transaction_date_id"] == date_dim["date_id"], how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business"),
        F.col("transaction_type")
    )
    .agg(F.sum("premium_amount").alias("type_premium"))
    .groupBy("reporting_period", "period_type", "line_of_business")
    .pivot("transaction_type")
    .agg(F.sum("type_premium"))
    .fillna(0)
    .withColumnRenamed("New Business", "new_business_premium")
    .withColumnRenamed("Renewal", "renewal_premium")
    .withColumnRenamed("Endorsement", "endorsement_premium")
    .withColumnRenamed("Cancel", "cancelled_premium")
    .withColumn("total_written_premium", 
        F.col("new_business_premium") + F.col("renewal_premium") + 
        F.col("endorsement_premium") - F.col("cancelled_premium"))
    .withColumn("total_earned_premium", F.col("total_written_premium") * 0.85)  # approximation
    .withColumn("growth_rate", F.lit(0.0))  # placeholder - calculate YoY in production
    .withColumn("loaded_at", now)
)

# Select final columns (handle missing pivot columns gracefully)
from pyspark.sql.functions import col
cols = ["reporting_period", "period_type", "line_of_business"]
for c in ["new_business_premium", "renewal_premium", "endorsement_premium", "cancelled_premium"]:
    if c in gold_premium_growth.columns:
        cols.append(c)
    else:
        gold_premium_growth = gold_premium_growth.withColumn(c, F.lit(0))
        cols.append(c)
cols += ["total_written_premium", "total_earned_premium", "growth_rate", "loaded_at"]

gold_premium_growth = gold_premium_growth.select(*cols)

persist_gold("premium_growth", gold_premium_growth)

print(f"✓ Gold premium_growth loaded: {gold_premium_growth.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: Exposure Summary
# ============================================
# 5. GOLD: Exposure Summary
# ============================================

gold_exposure = (
    policy_dim
    .join(date_dim_for_policy,
          policy_dim["effective_date"] == date_dim_for_policy["full_date"], how="left")
    .join(customer_dim.select("customer_id", "state"), on="customer_id", how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business"),
        F.col("state")
    )
    .agg(
        F.count("*").alias("total_policies"),
        F.sum(F.when(F.col("status") == "Active", 1).otherwise(0)).alias("active_policies"),
        F.sum(F.when(F.col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled_policies"),
        F.sum("coverage_amount").alias("total_coverage_limit"),
        F.avg("coverage_amount").alias("avg_premium_per_policy"),
        F.sum(F.when(F.col("status") == "Active", 1).otherwise(0)).alias("total_earned_exposure")
    )
    .withColumn("loaded_at", now)
)

persist_gold("exposure_summary", gold_exposure)

print(f"✓ Gold exposure_summary loaded: {gold_exposure.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: UW Dashboard Summary
# ============================================
# 6. GOLD: Underwriting Dashboard Summary
# ============================================

# Comprehensive executive summary combining all KPIs
loss_ratio_tbl = spark.table(f"{CATALOG}.{GOLD}.loss_ratio_by_lob")
freq_sev_tbl = spark.table(f"{CATALOG}.{GOLD}.claim_frequency_severity")

# Aggregate claim stats by period and LOB
claim_stats = (
    freq_sev_tbl
    .groupBy("reporting_period", "period_type", "line_of_business")
    .agg(
        F.sum("claim_count").alias("total_claims"),
        F.sum("incurred_losses").alias("total_incurred_losses"),
        F.avg("claim_severity").alias("avg_claim_severity")
    )
)

gold_uw_summary = (
    loss_ratio_tbl
    .join(claim_stats, on=["reporting_period", "period_type", "line_of_business"], how="left")
    .fillna(0, subset=["total_claims", "total_incurred_losses", "avg_claim_severity"])
    .withColumn("new_policies", F.col("policy_count"))
    .withColumn("renewed_policies", F.lit(0))  # from retention table in production
    .withColumn("cancelled_policies", F.lit(0))
    .withColumn("open_claims", F.col("claim_count"))
    .withColumn("closed_claims", F.lit(0))
    .select(
        "reporting_period", "period_type", "line_of_business",
        F.col("earned_premium").alias("earned_premium"),
        F.col("written_premium").alias("written_premium"),
        F.col("incurred_losses"),
        F.col("expense_amount"),
        F.col("loss_ratio"),
        F.col("combined_ratio"),
        "new_policies", "renewed_policies", "cancelled_policies",
        "open_claims", "closed_claims", "avg_claim_severity", "loaded_at"
    )
)

persist_gold("uw_dashboard_summary", gold_uw_summary)

print(f"✓ Gold uw_dashboard_summary loaded: {gold_uw_summary.count()} records")
gold_uw_summary.orderBy(F.desc("reporting_period")).show(10, truncate=False)

# COMMAND ----------

record_gold_audit()

# DBTITLE 1,Gold Layer Summary
spark.sql(f"""
    SELECT 'loss_ratio_by_lob' AS table_name, COUNT(*) AS record_count FROM {CATALOG}.{GOLD}.loss_ratio_by_lob
    UNION ALL SELECT 'claim_frequency_severity', COUNT(*) FROM {CATALOG}.{GOLD}.claim_frequency_severity
    UNION ALL SELECT 'retention_by_agent', COUNT(*) FROM {CATALOG}.{GOLD}.retention_by_agent
    UNION ALL SELECT 'premium_growth', COUNT(*) FROM {CATALOG}.{GOLD}.premium_growth
    UNION ALL SELECT 'exposure_summary', COUNT(*) FROM {CATALOG}.{GOLD}.exposure_summary
    UNION ALL SELECT 'uw_dashboard_summary', COUNT(*) FROM {CATALOG}.{GOLD}.uw_dashboard_summary
    ORDER BY table_name
""").show()