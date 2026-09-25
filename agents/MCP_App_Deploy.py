# Databricks notebook source
# MAGIC %md
# MAGIC # MCP App Deploy - pc-insurance-workspace-actions
# MAGIC
# MAGIC Deploys the MCP app (`pc-insurance-workspace-actions`) that serves as the `workspace-actions` tool on the Supervisor Agent.
# MAGIC
# MAGIC This notebook is **idempotent** — it checks the app status and only takes action if needed:
# MAGIC - **STOPPED** → start the app, then deploy
# MAGIC - **RUNNING** → deploy the latest source
# MAGIC - **STARTING/STOPPING** → poll until stable, then deploy
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - App source files must exist at the configured source path
# MAGIC - Databricks CLI must be available (pre-installed on job clusters)
# MAGIC
# MAGIC ## Part of Job 1 (PC_Insurance_Agent_Setup)
# MAGIC Runs as a task before `supervisor_agent_setup` so the MCP app is running before the Supervisor Agent registers it as a tool.

# COMMAND ----------

# ============================================
# Configuration
# ============================================
import subprocess
import json
import time
import sys

APP_NAME = "pc-insurance-workspace-actions"
SOURCE_PATH = "/Workspace/Users/vedavyas.goparaju@gmail.com/pc-insurance-workspace-actions"

print(f"App: {APP_NAME}")
print(f"Source: {SOURCE_PATH}")

# ============================================
# Helper: run databricks CLI command
# ============================================
def run_cli(args, timeout=300):
    """Run a databricks CLI command and return (returncode, stdout, stderr)."""
    cmd = ["databricks"] + args
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return result.returncode, result.stdout.strip(), result.stderr.strip()

def get_app_status():
    """Get app status as JSON dict. Returns None if app doesn't exist."""
    rc, stdout, stderr = run_cli(["apps", "get", APP_NAME, "--output", "JSON"], timeout=30)
    if rc == 0:
        return json.loads(stdout)
    return None

def deploy_app():
    """Deploy the app from the source path."""
    rc, stdout, stderr = run_cli(["apps", "deploy", APP_NAME, "--source-path", SOURCE_PATH], timeout=600)
    if rc == 0:
        print(f"  Deploy succeeded")
        return True
    else:
        print(f"  Deploy failed (rc={rc}): {stderr[:300]}")
        return False

print("Configuration loaded.")

# COMMAND ----------

# ============================================
# Step 1: Check App Status (State-First Deploy Sequence)
# ============================================

app_info = get_app_status()

if app_info is None:
    print(f"ERROR: App '{APP_NAME}' not found. Create it first:")
    print(f"  databricks apps create {APP_NAME} --source-path {SOURCE_PATH}")
    raise Exception(f"App {APP_NAME} does not exist")

app_state = app_info.get("app_status", {}).get("state", "UNKNOWN")
compute_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")
print(f"App status: {app_state}")
print(f"Compute status: {compute_state}")

# ============================================
# Step 2: Handle State Transitions
# ============================================

if app_state == "RUNNING" and compute_state == "ACTIVE":
    # Already running -- deploy directly
    print("\nApp is RUNNING. Deploying latest source...")
    deploy_app()

elif app_state == "STOPPED":
    # Start the app first, then deploy
    print("\nApp is STOPPED. Starting...")
    rc, stdout, stderr = run_cli(["apps", "start", APP_NAME, "--timeout", "20m", "--output", "JSON"], timeout=1200)
    if rc != 0:
        print(f"  Start failed: {stderr[:300]}")
        raise Exception(f"Failed to start app: {stderr[:200]}")
    print("  App started.")
    time.sleep(5)
    print("  Deploying source...")
    deploy_app()

elif app_state in ("STARTING", "STOPPING"):
    # Poll until stable
    print(f"\nApp is {app_state}. Polling for stable state...")
    max_polls = 20
    for i in range(max_polls):
        time.sleep(15)
        info = get_app_status()
        if info is None:
            raise Exception("App disappeared during polling")
        state = info.get("app_status", {}).get("state", "UNKNOWN")
        print(f"  Poll {i+1}/{max_polls}: {state}")
        if state == "RUNNING":
            print("  App is now RUNNING. Deploying...")
            deploy_app()
            break
        elif state == "STOPPED":
            print("  App stopped. Starting...")
            rc, stdout, stderr = run_cli(["apps", "start", APP_NAME, "--timeout", "20m", "--output", "JSON"], timeout=1200)
            if rc == 0:
                time.sleep(5)
                deploy_app()
            break
    else:
        raise Exception(f"App did not stabilize after {max_polls} polls (last state: {state})")

else:
    print(f"\nUnexpected app state: {app_state}")
    raise Exception(f"Cannot deploy in state: {app_state}")

print("\n✓ Deploy step complete.")

# COMMAND ----------

# ============================================
# Step 3: Verify App is Running
# ============================================

print("\n" + "=" * 60)
print("MCP APP DEPLOYMENT VERIFICATION")
print("=" * 60)

# Wait a moment for deployment to settle
time.sleep(10)

app_info = get_app_status()
if app_info is None:
    print(f"ERROR: App '{APP_NAME}' not found after deploy!")
    raise Exception("App disappeared after deploy")

app_state = app_info.get("app_status", {}).get("state", "UNKNOWN")
compute_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")
deployment = app_info.get("active_deployment", {})
deploy_status = deployment.get("status", {}).get("state", "UNKNOWN")

print(f"  App name: {APP_NAME}")
print(f"  App status: {app_state}")
print(f"  Compute status: {compute_state}")
print(f"  Deployment status: {deploy_status}")
print(f"  Source path: {deployment.get('source_code_path', 'N/A')}")
print(f"  App URL: {app_info.get('url', 'N/A')}")

if app_state == "RUNNING" and deploy_status == "SUCCEEDED":
    print("\n✓ MCP App deployed successfully!")
    print(f"  Endpoint: {app_info.get('url', 'N/A')}/mcp")
else:
    print(f"\n⚠ App state: {app_state}, deployment: {deploy_status}")
    print("  App may still be starting. Check status manually.")

print("\n" + "=" * 60)
print("MCP APP DEPLOY COMPLETE")
print("=" * 60)