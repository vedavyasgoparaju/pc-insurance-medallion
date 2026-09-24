# Databricks notebook source
# MAGIC %md
# MAGIC # P&C Insurance Analyst Agent (Genie-Powered)
# MAGIC 
# MAGIC This notebook implements the Analyst Agent that answers business questions by querying Gold layer tables:
# MAGIC - Loss ratios by line of business
# MAGIC - Claim frequency and severity metrics
# MAGIC - Retention rates by agent
# MAGIC - Premium growth trends
# MAGIC - Exposure summaries
# MAGIC - Underwriting dashboard metrics
# MAGIC 
# MAGIC **Data Source**: Gold layer tables in `pc_insurance.gold` schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

import json
from typing import Dict, List, Any, Optional
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Agent configuration
AGENT_CONFIG = {
    "name": "P&C Business Analyst",
    "version": "1.0",
    "description": "Answers business questions using Gold layer KPI tables",
    "data_sources": [
        "pc_insurance.gold.loss_ratio_by_lob",
        "pc_insurance.gold.claim_frequency_severity",
        "pc_insurance.gold.retention_by_agent",
        "pc_insurance.gold.premium_growth",
        "pc_insurance.gold.exposure_summary",
        "pc_insurance.gold.uw_dashboard_summary"
    ],
    "capabilities": [
        "Loss ratio analysis by LOB",
        "Claim frequency and severity metrics",
        "Agent retention analysis",
        "Premium growth trends",
        "Exposure summaries",
        "Underwriting performance metrics"
    ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyst Query Functions

# COMMAND ----------

class PCAnalyst:
    """P&C Insurance Business Analyst Agent"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.gold_schema = "pc_insurance.gold"
    
    def get_loss_ratio_by_lob(self, lob: Optional[str] = None) -> Dict[str, Any]:
        """Get loss ratio metrics by line of business"""
        
        query = f"""
        SELECT 
            line_of_business,
            total_incurred_loss,
            total_earned_premium,
            loss_ratio,
            claim_count,
            avg_loss_per_claim
        FROM {self.gold_schema}.loss_ratio_by_lob
        """
        
        if lob:
            query += f" WHERE line_of_business = '{lob}'"
        
        query += " ORDER BY loss_ratio DESC"
        
        try:
            df = self.spark.sql(query)
            results = df.collect()
            
            return {
                "metric": "loss_ratio_by_lob",
                "filter": {"lob": lob} if lob else None,
                "row_count": len(results),
                "data": [row.asDict() for row in results]
            }
        except Exception as e:
            return {
                "metric": "loss_ratio_by_lob",
                "error": str(e),
                "data": []
            }
    
    def get_claim_frequency_severity(self, state: Optional[str] = None) -> Dict[str, Any]:
        """Get claim frequency and severity metrics"""
        
        query = f"""
        SELECT 
            state,
            policy_count,
            claim_count,
            claim_frequency,
            total_incurred_loss,
            avg_severity,
            pure_premium
        FROM {self.gold_schema}.claim_frequency_severity
        """
        
        if state:
            query += f" WHERE state = '{state}'"
        
        query += " ORDER BY claim_frequency DESC"
        
        try:
            df = self.spark.sql(query)
            results = df.collect()
            
            return {
                "metric": "claim_frequency_severity",
                "filter": {"state": state} if state else None,
                "row_count": len(results),
                "data": [row.asDict() for row in results]
            }
        except Exception as e:
            return {
                "metric": "claim_frequency_severity",
                "error": str(e),
                "data": []
            }
    
    def get_retention_by_agent(self, agent_id: Optional[str] = None) -> Dict[str, Any]:
        """Get retention metrics by agent"""
        
        query = f"""
        SELECT 
            agent_id,
            agent_name,
            total_policies,
            renewed_policies,
            retention_rate,
            total_premium,
            avg_premium_per_policy
        FROM {self.gold_schema}.retention_by_agent
        """
        
        if agent_id:
            query += f" WHERE agent_id = '{agent_id}'"
        
        query += " ORDER BY retention_rate DESC"
        
        try:
            df = self.spark.sql(query)
            results = df.collect()
            
            return {
                "metric": "retention_by_agent",
                "filter": {"agent_id": agent_id} if agent_id else None,
                "row_count": len(results),
                "data": [row.asDict() for row in results]
            }
        except Exception as e:
            return {
                "metric": "retention_by_agent",
                "error": str(e),
                "data": []
            }
    
    def get_premium_growth(self) -> Dict[str, Any]:
        """Get premium growth trends"""
        
        query = f"""
        SELECT 
            year_month,
            written_premium,
            earned_premium,
            policy_count,
            avg_premium_per_policy,
            mom_growth_rate,
            yoy_growth_rate
        FROM {self.gold_schema}.premium_growth
        ORDER BY year_month DESC
        """
        
        try:
            df = self.spark.sql(query)
            results = df.collect()
            
            return {
                "metric": "premium_growth",
                "row_count": len(results),
                "data": [row.asDict() for row in results]
            }
        except Exception as e:
            return {
                "metric": "premium_growth",
                "error": str(e),
                "data": []
            }
    
    def get_exposure_summary(self, state: Optional[str] = None) -> Dict[str, Any]:
        """Get exposure summary metrics"""
        
        query = f"""
        SELECT 
            state,
            line_of_business,
            policy_count,
            total_coverage_limit,
            total_premium,
            avg_coverage_per_policy,
            avg_premium_per_policy
        FROM {self.gold_schema}.exposure_summary
        """
        
        if state:
            query += f" WHERE state = '{state}'"
        
        query += " ORDER BY total_coverage_limit DESC"
        
        try:
            df = self.spark.sql(query)
            results = df.collect()
            
            return {
                "metric": "exposure_summary",
                "filter": {"state": state} if state else None,
                "row_count": len(results),
                "data": [row.asDict() for row in results]
            }
        except Exception as e:
            return {
                "metric": "exposure_summary",
                "error": str(e),
                "data": []
            }
    
    def get_uw_dashboard(self) -> Dict[str, Any]:
        """Get underwriting dashboard summary"""
        
        query = f"""
        SELECT 
            line_of_business,
            state,
            policy_count,
            total_premium,
            total_incurred_loss,
            loss_ratio,
            claim_count,
            claim_frequency,
            avg_severity
        FROM {self.gold_schema}.uw_dashboard_summary
        ORDER BY loss_ratio DESC
        """
        
        try:
            df = self.spark.sql(query)
            results = df.collect()
            
            return {
                "metric": "uw_dashboard_summary",
                "row_count": len(results),
                "data": [row.asDict() for row in results]
            }
        except Exception as e:
            return {
                "metric": "uw_dashboard_summary",
                "error": str(e),
                "data": []
            }
    
    def answer_business_question(self, question: str) -> Dict[str, Any]:
        """Route business questions to appropriate metrics"""
        
        question_lower = question.lower()
        
        # Route based on keywords
        if "loss ratio" in question_lower:
            lob = self._extract_lob(question)
            return self.get_loss_ratio_by_lob(lob)
        
        elif "frequency" in question_lower or "severity" in question_lower:
            state = self._extract_state(question)
            return self.get_claim_frequency_severity(state)
        
        elif "retention" in question_lower or "renewal" in question_lower:
            return self.get_retention_by_agent()
        
        elif "premium growth" in question_lower or "growth" in question_lower:
            return self.get_premium_growth()
        
        elif "exposure" in question_lower:
            state = self._extract_state(question)
            return self.get_exposure_summary(state)
        
        elif "dashboard" in question_lower or "summary" in question_lower:
            return self.get_uw_dashboard()
        
        else:
            return {
                "error": "Could not determine which metric to query",
                "suggestion": "Try asking about: loss ratios, claim frequency/severity, retention, premium growth, exposure, or dashboard summary",
                "question": question
            }
    
    def _extract_lob(self, question: str) -> Optional[str]:
        """Extract line of business from question"""
        lobs = ["Auto", "Property", "Liability", "Workers Comp", "Commercial"]
        for lob in lobs:
            if lob.lower() in question.lower():
                return lob
        return None
    
    def _extract_state(self, question: str) -> Optional[str]:
        """Extract state from question"""
        # Simple extraction - could be enhanced
        states = ["CA", "TX", "FL", "NY", "IL"]
        for state in states:
            if state in question.upper():
                return state
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Agent

# COMMAND ----------

# Initialize the analyst
spark = SparkSession.builder.getOrCreate()
analyst = PCAnalyst(spark)

print(f"Analyst Agent initialized")
print(f"Configuration: {json.dumps(AGENT_CONFIG, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Interface Functions

# COMMAND ----------

def query_kpi(metric_name: str, **filters) -> Dict[str, Any]:
    """
    Query a specific KPI metric
    
    Args:
        metric_name: Name of the metric (e.g., 'loss_ratio_by_lob')
        **filters: Optional filters (e.g., lob='Auto', state='CA')
        
    Returns:
        Dictionary with metric data
    """
    if metric_name == "loss_ratio_by_lob":
        return analyst.get_loss_ratio_by_lob(filters.get('lob'))
    elif metric_name == "claim_frequency_severity":
        return analyst.get_claim_frequency_severity(filters.get('state'))
    elif metric_name == "retention_by_agent":
        return analyst.get_retention_by_agent(filters.get('agent_id'))
    elif metric_name == "premium_growth":
        return analyst.get_premium_growth()
    elif metric_name == "exposure_summary":
        return analyst.get_exposure_summary(filters.get('state'))
    elif metric_name == "uw_dashboard":
        return analyst.get_uw_dashboard()
    else:
        return {"error": f"Unknown metric: {metric_name}"}

def ask_analyst(question: str) -> Dict[str, Any]:
    """
    Ask the analyst a business question
    
    Args:
        question: Natural language business question
        
    Returns:
        Dictionary with answer and data
    """
    return analyst.answer_business_question(question)

def list_metrics() -> List[str]:
    """List available metrics"""
    return AGENT_CONFIG["data_sources"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget-Based Interactive Interface

# COMMAND ----------

# Create widgets
dbutils.widgets.text("business_question", "", "Business Question")
dbutils.widgets.dropdown("metric", "auto", ["auto", "loss_ratio_by_lob", "claim_frequency_severity", "retention_by_agent", "premium_growth", "exposure_summary", "uw_dashboard"], "Metric")

# COMMAND ----------

# Get widget values
question = dbutils.widgets.get("business_question")
metric = dbutils.widgets.get("metric")

if question:
    print("=" * 80)
    print(f"Processing: {question}")
    print("=" * 80)
    
    if metric == "auto":
        result = ask_analyst(question)
    else:
        result = query_kpi(metric)
    
    print(f"\nResult:\n{json.dumps(result, indent=2, default=str)}")
else:
    print("Please enter a business question in the widget above and run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Status

# COMMAND ----------

def get_agent_status() -> Dict[str, Any]:
    """Get current agent status"""
    return {
        "agent_name": AGENT_CONFIG["name"],
        "version": AGENT_CONFIG["version"],
        "status": "active",
        "data_sources": AGENT_CONFIG["data_sources"],
        "capabilities": AGENT_CONFIG["capabilities"]
    }

# Display status
status = get_agent_status()
print(json.dumps(status, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export for Orchestration

# COMMAND ----------

dbutils.notebook.exit(json.dumps({
    "status": "success",
    "agent": "Analyst_Genie_Agent",
    "message": "Analyst Agent initialized and ready",
    "capabilities": AGENT_CONFIG["capabilities"]
}))
