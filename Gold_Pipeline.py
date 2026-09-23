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

CATALOG = "pc_insurance"
SILVER = "silver"
GOLD = "gold"

now = F.current_timestamp()
print("Gold Pipeline started at:", datetime.datetime.now())

# COMMAND ----------

# DBTITLE 1,Gold: Loss Ratio by LOB
# ============================================
# 1. GOLD: Loss Ratio by Line of Business (Quarterly)
# ============================================

premium_fact = spark.table(f"{CATALOG}.{SILVER}.premium_fact")
claim_fact = spark.table(f"{CATALOG}.{SILVER}.claim_fact")

# Aggregate earned premium by quarter and LOB
earned_premium_agg = (
    premium_fact
    .join(spark.table(f"{CATALOG}.{SILVER}.date_dim"), on="date_sk", how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business")
    )
    .agg(
        F.sum("earned_premium").alias("earned_premium"),
        F.sum("written_premium").alias("written_premium"),
        F.sum("commission_amount").alias("expense_amount"),
        F.countDistinct("policy_id").alias("policy_count")
    )
)

# Aggregate incurred losses by quarter and LOB (using loss_date)
incurred_loss_agg = (
    claim_fact
    .join(
        spark.table(f"{CATALOG}.{SILVER}.date_dim")
            .select(F.col("date_sk").alias("loss_date_sk"), F.col("year"), F.col("quarter")),
        claim_fact["date_sk"] == F.col("loss_date_sk"), how="left"
    )
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.col("line_of_business")
    )
    .agg(
        F.sum("incurred_loss").alias("incurred_losses"),
        F.sum("expense_amount").alias("claim_expense"),
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
        "earned_premium", "incurred_losses", "expense_amount",
        "loss_ratio", "expense_ratio", "combined_ratio",
        "policy_count", "claim_count", "loaded_at"
    )
)

(gold_loss_ratio.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.loss_ratio_by_lob"))

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
    .join(spark.table(f"{CATALOG}.{SILVER}.date_dim"), on="date_sk", how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business"),
        F.col("state")
    )
    .agg(
        F.countDistinct("policy_id").alias("exposure_units"),
        F.sum("earned_premium").alias("total_earned_premium")
    )
)

# Aggregate claim counts and losses
claim_freq_agg = (
    claim_fact
    .join(
        spark.table(f"{CATALOG}.{SILVER}.date_dim")
            .select(F.col("date_sk").alias("claim_date_sk"), F.col("year"), F.col("quarter")),
        claim_fact["date_sk"] == F.col("claim_date_sk"), how="left"
    )
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.col("line_of_business"),
        F.col("state")
    )
    .agg(
        F.countDistinct("claim_id").alias("claim_count"),
        F.sum("incurred_loss").alias("incurred_losses"),
        F.avg("paid_loss").alias("avg_paid_loss"),
        F.avg("reserved_amount").alias("avg_reserved")
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

(gold_freq_severity.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.claim_frequency_severity"))

print(f"✓ Gold claim_frequency_severity loaded: {gold_freq_severity.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: Retention by Agent
# ============================================
# 3. GOLD: Retention Rate by Agent
# ============================================

policy_dim = spark.table(f"{CATALOG}.{SILVER}.policy_dim")
agent_dim = spark.table(f"{CATALOG}.{SILVER}.agent_dim")

# Classify policies by status
retention_data = (
    policy_dim
    .join(agent_dim, on="agent_id", how="left")
    .join(spark.table(f"{CATALOG}.{SILVER}.date_dim"), 
          policy_dim["effective_date"] == spark.table(f"{CATALOG}.{SILVER}.date_dim")["full_date"], how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("agent_id"),
        F.col("agent_name"),
        F.col("agency_name")
    )
    .agg(
        F.count("*").alias("total_policies"),
        F.sum(F.when(F.col("policy_status") == "Active", 1).otherwise(0)).alias("active_policies"),
        F.sum(F.when(F.col("policy_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_policies"),
        F.sum(F.when(F.col("endorsement_count") == 0, 1).otherwise(0)).alias("new_business_policies"),
        F.sum("premium_amount").alias("total_written_premium")
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

(retention_data.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.retention_by_agent"))

print(f"✓ Gold retention_by_agent loaded: {retention_data.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: Premium Growth
# ============================================
# 4. GOLD: Premium Growth Summary
# ============================================

gold_premium_growth = (
    premium_fact
    .join(spark.table(f"{CATALOG}.{SILVER}.date_dim"), on="date_sk", how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business"),
        F.col("transaction_type")
    )
    .agg(F.sum("written_premium").alias("type_premium"))
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

(gold_premium_growth.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.premium_growth"))

print(f"✓ Gold premium_growth loaded: {gold_premium_growth.count()} records")

# COMMAND ----------

# DBTITLE 1,Gold: Exposure Summary
# ============================================
# 5. GOLD: Exposure Summary
# ============================================

gold_exposure = (
    policy_dim
    .join(spark.table(f"{CATALOG}.{SILVER}.date_dim"),
          policy_dim["effective_date"] == spark.table(f"{CATALOG}.{SILVER}.date_dim")["full_date"], how="left")
    .groupBy(
        F.concat_ws("-Q", F.col("year"), F.col("quarter")).alias("reporting_period"),
        F.lit("Quarterly").alias("period_type"),
        F.col("line_of_business"),
        F.col("state")
    )
    .agg(
        F.count("*").alias("total_policies"),
        F.sum(F.when(F.col("policy_status") == "Active", 1).otherwise(0)).alias("active_policies"),
        F.sum(F.when(F.col("policy_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_policies"),
        F.sum("coverage_limit").alias("total_coverage_limit"),
        F.avg("premium_amount").alias("avg_premium_per_policy"),
        F.sum(F.when(F.col("policy_status") == "Active", 1).otherwise(0)).alias("total_earned_exposure")
    )
    .withColumn("loaded_at", now)
)

(gold_exposure.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.exposure_summary"))

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

(gold_uw_summary.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{CATALOG}.{GOLD}.uw_dashboard_summary"))

print(f"✓ Gold uw_dashboard_summary loaded: {gold_uw_summary.count()} records")
gold_uw_summary.orderBy(F.desc("reporting_period")).show(10, truncate=False)

# COMMAND ----------

# DBTITLE 1,Gold Layer Summary
# MAGIC %sql
# MAGIC -- ============================================
# MAGIC -- Gold Layer Summary
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT 'loss_ratio_by_lob' AS table_name, COUNT(*) AS record_count FROM pc_insurance.gold.loss_ratio_by_lob
# MAGIC UNION ALL
# MAGIC SELECT 'claim_frequency_severity', COUNT(*) FROM pc_insurance.gold.claim_frequency_severity
# MAGIC UNION ALL
# MAGIC SELECT 'retention_by_agent', COUNT(*) FROM pc_insurance.gold.retention_by_agent
# MAGIC UNION ALL
# MAGIC SELECT 'premium_growth', COUNT(*) FROM pc_insurance.gold.premium_growth
# MAGIC UNION ALL
# MAGIC SELECT 'exposure_summary', COUNT(*) FROM pc_insurance.gold.exposure_summary
# MAGIC UNION ALL
# MAGIC SELECT 'uw_dashboard_summary', COUNT(*) FROM pc_insurance.gold.uw_dashboard_summary
# MAGIC ORDER BY table_name;