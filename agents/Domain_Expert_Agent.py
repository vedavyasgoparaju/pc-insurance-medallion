# Databricks notebook source
# MAGIC %md
# MAGIC # P&C Insurance Domain Expert Agent
# MAGIC 
# MAGIC This notebook implements the Domain Expert Agent that answers questions about P&C insurance domain knowledge:
# MAGIC - Policy lifecycle and administration
# MAGIC - Claims processing and reserving
# MAGIC - Underwriting principles
# MAGIC - Loss ratios and combined ratios
# MAGIC - Frequency and severity metrics
# MAGIC - Retention and renewal patterns
# MAGIC - NAIC requirements and regulatory compliance
# MAGIC 
# MAGIC **Data Source**: Unity Catalog Volume `pc_insurance.reference.pc_domain_docs`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

import os
import json
from pathlib import Path
from typing import Dict, List, Any, Optional

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog volume path for P&C domain documents
DOMAIN_DOCS_VOLUME = "/Volumes/pc_insurance/reference/pc_domain_docs/"

# Agent configuration
AGENT_CONFIG = {
    "name": "P&C Domain Expert",
    "version": "1.0",
    "description": "Expert in Property & Casualty insurance domain knowledge",
    "capabilities": [
        "Policy lifecycle and administration",
        "Claims processing and reserving",
        "Underwriting principles and risk assessment",
        "Loss ratios and combined ratios",
        "Frequency and severity analysis",
        "Retention and renewal patterns",
        "NAIC requirements and compliance"
    ]
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Domain Knowledge Base Access

# COMMAND ----------

class PCDomainExpert:
    """P&C Insurance Domain Expert Agent"""
    
    def __init__(self, docs_path: str):
        self.docs_path = docs_path
        self.knowledge_base = self._load_knowledge_base()
    
    def _load_knowledge_base(self) -> Dict[str, str]:
        """Load all domain documents from the volume"""
        knowledge = {}
        
        if not os.path.exists(self.docs_path):
            print(f"Warning: Domain docs path does not exist: {self.docs_path}")
            return knowledge
        
        # Recursively load all text and markdown files
        for root, dirs, files in os.walk(self.docs_path):
            for file in files:
                if file.endswith(('.txt', '.md', '.pdf', '.doc', '.docx')):
                    file_path = os.path.join(root, file)
                    try:
                        # For text-based files, read content
                        if file.endswith(('.txt', '.md')):
                            with open(file_path, 'r', encoding='utf-8') as f:
                                content = f.read()
                                knowledge[file] = content
                        else:
                            # For binary files, just note their existence
                            knowledge[file] = f"[Binary file: {file_path}]"
                    except Exception as e:
                        print(f"Error reading {file_path}: {str(e)}")
        
        print(f"Loaded {len(knowledge)} documents from knowledge base")
        return knowledge
    
    def search_knowledge(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """Search knowledge base for relevant information"""
        query_lower = query.lower()
        results = []
        
        # Simple keyword-based search
        for doc_name, content in self.knowledge_base.items():
            if isinstance(content, str) and not content.startswith("[Binary file"):
                # Calculate relevance score based on keyword matches
                score = 0
                content_lower = content.lower()
                
                # Split query into keywords
                keywords = [w for w in query_lower.split() if len(w) > 3]
                
                for keyword in keywords:
                    score += content_lower.count(keyword)
                
                if score > 0:
                    results.append({
                        "document": doc_name,
                        "score": score,
                        "content": content[:500] + "..." if len(content) > 500 else content
                    })
        
        # Sort by score and return top_k
        results.sort(key=lambda x: x["score"], reverse=True)
        return results[:top_k]
    
    def answer_question(self, question: str) -> Dict[str, Any]:
        """Answer a P&C insurance domain question"""
        
        # Search knowledge base
        relevant_docs = self.search_knowledge(question)
        
        if not relevant_docs:
            return {
                "question": question,
                "answer": "I don't have specific information about this topic in my knowledge base. Please provide more context or rephrase your question.",
                "sources": [],
                "confidence": "low"
            }
        
        # Generate answer based on relevant documents
        answer_parts = []
        sources = []
        
        for doc in relevant_docs:
            answer_parts.append(f"From {doc['document']}:\n{doc['content']}\n")
            sources.append(doc['document'])
        
        return {
            "question": question,
            "answer": "\n".join(answer_parts),
            "sources": sources,
            "confidence": "high" if len(relevant_docs) >= 2 else "medium",
            "documents_searched": len(self.knowledge_base)
        }
    
    def get_topic_overview(self, topic: str) -> Dict[str, Any]:
        """Get an overview of a specific P&C insurance topic"""
        
        topic_keywords = {
            "loss_ratio": ["loss ratio", "incurred loss", "earned premium", "loss cost"],
            "combined_ratio": ["combined ratio", "expense ratio", "underwriting profit"],
            "claims": ["claim", "loss", "adjuster", "settlement", "reserve"],
            "underwriting": ["underwriting", "risk assessment", "pricing", "selection"],
            "policy": ["policy", "coverage", "endorsement", "renewal", "cancellation"],
            "reserving": ["reserve", "ibnr", "case reserve", "loss development"],
            "retention": ["retention", "renewal", "lapse", "persistency"],
            "frequency_severity": ["frequency", "severity", "claim count", "average loss"]
        }
        
        # Find matching topic
        topic_lower = topic.lower().replace(" ", "_")
        keywords = topic_keywords.get(topic_lower, [topic.lower()])
        
        # Search using topic keywords
        query = " ".join(keywords)
        return self.answer_question(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Agent

# COMMAND ----------

# Initialize the domain expert
domain_expert = PCDomainExpert(DOMAIN_DOCS_VOLUME)

print(f"Domain Expert Agent initialized")
print(f"Configuration: {json.dumps(AGENT_CONFIG, indent=2)}")
print(f"Knowledge base size: {len(domain_expert.knowledge_base)} documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Interface Functions

# COMMAND ----------

def ask_domain_expert(question: str) -> Dict[str, Any]:
    """
    Ask the P&C Domain Expert a question
    
    Args:
        question: Natural language question about P&C insurance
        
    Returns:
        Dictionary with answer, sources, and confidence level
    """
    return domain_expert.answer_question(question)

def get_topic_info(topic: str) -> Dict[str, Any]:
    """
    Get information about a specific P&C insurance topic
    
    Args:
        topic: Topic name (e.g., 'loss_ratio', 'claims', 'underwriting')
        
    Returns:
        Dictionary with topic overview and sources
    """
    return domain_expert.get_topic_overview(topic)

def list_capabilities() -> Dict[str, Any]:
    """List the agent's capabilities"""
    return AGENT_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget-Based Interactive Interface

# COMMAND ----------

# Create widgets for interactive use
dbutils.widgets.text("question", "", "Ask a Question")
dbutils.widgets.dropdown("query_type", "question", ["question", "topic_overview"], "Query Type")

# COMMAND ----------

# Get widget values
user_question = dbutils.widgets.get("question")
query_type = dbutils.widgets.get("query_type")

if user_question:
    print("=" * 80)
    print(f"Processing: {user_question}")
    print("=" * 80)
    
    if query_type == "question":
        result = ask_domain_expert(user_question)
    else:
        result = get_topic_info(user_question)
    
    print(f"\nAnswer:\n{result['answer']}")
    print(f"\nSources: {', '.join(result['sources'])}")
    print(f"Confidence: {result['confidence']}")
else:
    print("Please enter a question in the widget above and run this cell.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Status and Health Check

# COMMAND ----------

def get_agent_status() -> Dict[str, Any]:
    """Get current agent status"""
    return {
        "agent_name": AGENT_CONFIG["name"],
        "version": AGENT_CONFIG["version"],
        "status": "active",
        "knowledge_base_size": len(domain_expert.knowledge_base),
        "knowledge_base_path": DOMAIN_DOCS_VOLUME,
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
    "agent": "Domain_Expert_Agent",
    "message": "Domain Expert Agent initialized and ready",
    "capabilities": AGENT_CONFIG["capabilities"]
}))
