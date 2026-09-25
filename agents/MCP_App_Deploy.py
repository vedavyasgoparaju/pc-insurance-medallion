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
# MAGIC - App must already be created (via `databricks apps create` or UI)
# MAGIC
# MAGIC ## Part of Job 1 (PC_Insurance_Agent_Setup)
# MAGIC Runs as a task before `supervisor_agent_setup` so the MCP app is running before the Supervisor Agent registers it as a tool.

# COMMAND ----------

# ============================================
# Configuration & REST API Helpers
# ============================================
import urllib.request
import urllib.error
import json
import time
import os

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
host = w.config.host

APP_NAME = "pc-insurance-workspace-actions"
SOURCE_PATH = "/Workspace/Users/vedavyas.goparaju@gmail.com/pc-insurance-workspace-actions"

print(f"App: {APP_NAME}")
print(f"Source: {SOURCE_PATH}")

# Get auth headers from SDK (works on job clusters, serverless, and local)
auth_headers = w.config.authenticate()
HEADERS = {**auth_headers, "Content-Type": "application/json"}
print(f"Auth headers: {list(auth_headers.keys())}")

def api_request(method, path, body=None):
    """Helper for REST API calls to the Apps API."""
    url = f"{host}/api/2.0/apps/{path}"
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=HEADERS, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=60)
        return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        raise Exception(f"{e.code}: {error_body[:300]}")

def get_app_status():
    """Get app status as JSON dict. Returns None if app doesn't exist."""
    try:
        return api_request("GET", APP_NAME)
    except Exception as e:
        print(f"  Status check error: {str(e)[:200]}")
        return None

def start_app():
    """Start the app."""
    try:
        result = api_request("POST", f"{APP_NAME}/start")
        print(f"  Start: {result.get('status', {}).get('state', 'UNKNOWN')}")
        return True
    except Exception as e:
        print(f"  Start failed: {str(e)[:200]}")
        return False

def deploy_app():
    """Deploy the app from the source path via REST API."""
    deploy_body = {
        "source_code_path": SOURCE_PATH,
        "mode": "SNAPSHOT"
    }
    try:
        result = api_request("POST", f"{APP_NAME}/deployments", deploy_body)
        deployment_id = result.get("deployment_id", "N/A")
        print(f"  Deploy succeeded (deployment_id: {deployment_id})")
        return True
    except Exception as e:
        print(f"  Deploy failed: {str(e)[:200]}")
        return False

print("Configuration loaded.")

# COMMAND ----------

# ============================================
# Step 1: Check App Status (State-First Deploy Sequence)
# ============================================

app_info = get_app_status()

if app_info is None:
    print(f"ERROR: App '{APP_NAME}' not found.")
    raise Exception(f"App {APP_NAME} does not exist")

app_state = app_info.get("app_status", {}).get("state", "UNKNOWN")
compute_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")
print(f"App status: {app_state}")
print(f"Compute status: {compute_state}")

# ============================================
# Step 2: Handle State Transitions
# ============================================

if app_state == "RUNNING" and compute_state == "ACTIVE":
    print("\nApp is RUNNING. Deploying latest source...")
    deploy_app()

elif app_state == "STOPPED":
    print("\nApp is STOPPED. Starting...")
    if start_app():
        print("  Waiting for app to become active...")
        time.sleep(10)
        print("  Deploying source...")
        deploy_app()
    else:
        raise Exception("Failed to start app")

elif app_state in ("STARTING", "STOPPING"):
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
            if start_app():
                time.sleep(10)
                deploy_app()
            break
    else:
        raise Exception(f"App did not stabilize after {max_polls} polls")

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

if app_state == "RUNNING":
    print("\n✓ MCP App deployed successfully!")
    print(f"  Endpoint: {app_info.get('url', 'N/A')}/mcp")
else:
    print(f"\n⚠ App state: {app_state}, deployment: {deploy_status}")
    print("  App may still be starting. Check status manually.")

print("\n" + "=" * 60)
print("MCP APP DEPLOY COMPLETE")
print("=" * 60)