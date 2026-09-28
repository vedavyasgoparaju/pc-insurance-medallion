# Autonomy Enhancements - September 2026

**Version:** 1.0  
**Date:** 2026-09-29  
**Status:** Deployed

---

## Overview

This document describes the new autonomy features added to the PC Insurance Medallion Architecture to enable self-healing, knowledge-based fixes, automated rollbacks, and health monitoring.

---

## New UC Artifacts

### 1. pc_insurance.dq.dq_validation_results

**Purpose**: Stores individual DQ rule validation outcomes for trend analysis

**Partitioning**: By table_name

### 2. pc_insurance.metadata.swarm_fix_history

**Purpose**: Tracks every autonomous fix attempt with circuit breaker support

**NEW COLUMN**: circuit_breaker_triggered (BOOLEAN)

### 3. pc_insurance.metadata.health_monitor_log

**Purpose**: Tracks pipeline health over time for alerting

**Key metrics**: health_score, stale_table_count, dq_pass_rate, swarm_success_rate, rollback/circuit-breaker counts

### 4. pc_insurance.dq.pipeline_health_score (function)

**Formula**: DQ (40%) + Freshness (25%) + Reconciliation (20%) + Error Rate (15%)

---

## Swarm Autonomy Enhancements

### 1. Circuit Breaker
- Prevents infinite fix loops
- Halts swarm after 3 failed attempts on same error in 6h

### 2. Fix Knowledge Base
- Learns from past successful fixes
- Queries swarm_fix_history for similar resolutions

### 3. RollbackManager
- Automated Delta RESTORE if DQ score drops >10% post-fix

### 4. DependencyChecker
- Verifies downstream table freshness after upstream fix

---

## Health Monitor Job

**Job ID**: 88172905444926
**Schedule**: Every 6 hours
**Composite Health Score**: 0.0-1.0 weighted by DQ, freshness, swarm success

---

## Job 1 Enhancement

**New Task**: autonomy_infrastructure_setup
- Depends on: swarm_setup, dq_functions_setup
- Creates all 4 autonomy UC artifacts

---

## Key Changes

1. Swarm notebook: Circuit breaker, knowledge base, rollback manager, dependency checker
2. Health monitor: Fixed logging schema
3. All pc_insurance_dev references updated to pc_insurance
4. New autonomy setup notebook
