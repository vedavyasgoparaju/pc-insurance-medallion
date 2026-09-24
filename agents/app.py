"""
MCP Server for P&C Insurance Workspace Actions

This Databricks App serves as an MCP (Model Context Protocol) server that
the Supervisor Agent can call to apply changes to the Databricks workspace.

Tools exposed:
1.  execute_sql        - Execute any SQL statement
2.  create_table       - Create a UC table from DDL
3.  write_notebook     - Write a notebook to workspace
4.  run_notebook       - Run a notebook as a one-time job
5.  git_commit         - Commit and push to GitHub
6.  run_dq_checks      - Run all DQ functions on a table
7.  get_table_schema   - Get schema of a UC table
8.  query_table        - Run a SELECT query
9.  update_file        - Write/update a file in workspace
10. insert_config_row  - Insert a row into a config table
"""

import os
import json
import base64
import time
import subprocess
from mcp.server.fastmcp import FastMCP
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat, Language
from databricks.sdk.service.sql import StatementState

# Initialize Databricks SDK client (uses app service principal auth)
w = WorkspaceClient()

# Configuration from environment
CATALOG_NAME = os.environ.get("CATALOG_NAME", "pc_insurance")
GIT_REPO_PATH = os.environ.get("GIT_REPO_PATH", "/Repos/vedavyas.goparaju/pc-insurance-medallion")
SQL_WAREHOUSE_ID = os.environ.get("SQL_WAREHOUSE_ID", "")

# DQ functions to run for run_dq_checks
DQ_FUNCTIONS = [
    "check_not_null",
    "check_claim_status",
    "check_premium_positive",
    "check_date_order",
    "check_policy_exists",
    "check_loss_ratio",
    "calculate_dq_score",
]

# Initialize MCP server
mcp = FastMCP("pc-insurance-workspace-actions")


def _get_warehouse_id() -> str:
    """Get the SQL warehouse ID from env var or auto-detect."""
    global SQL_WAREHOUSE_ID
    if SQL_WAREHOUSE_ID:
        return SQL_WAREHOUSE_ID
    # Auto-detect: list warehouses and pick first running one
    for ep in w.warehouses.list():
        if ep.state == "RUNNING":
            SQL_WAREHOUSE_ID = ep.id
            return ep.id
    # If none running, list any
    for ep in w.warehouses.list():
        SQL_WAREHOUSE_ID = ep.id
        return ep.id
    raise RuntimeError("No SQL warehouse found. Set SQL_WAREHOUSE_ID env var.")


def _execute_sql_raw(sql: str, wait_timeout: str = "60s") -> dict:
    """Execute SQL via Statement Execution API and return raw result."""
    wh_id = _get_warehouse_id()
    result = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=wh_id,
        wait_timeout=wait_timeout,
        result_format="JSON_ARRAY",
    )
    # Check for errors
    if result.status and result.status.state == StatementState.FAILED:
        err_msg = ""
        if result.status.error:
            err_msg = result.status.error.message or "Unknown error"
        return {"success": False, "error": err_msg}
    # Extract data
    rows = []
    columns = []
    if result.result and result.result.data_array:
        rows = result.result.data_array
    if result.manifest and result.manifest.schema and result.manifest.schema.columns:
        columns = [c.name for c in result.manifest.schema.columns]
    return {
        "success": True,
        "columns": columns,
        "row_count": len(rows),
        "rows": rows[:100],  # Limit to 100 rows
    }


# ==================== MCP TOOLS ====================


@mcp.tool()
def execute_sql(sql: str) -> str:
    """Execute any SQL statement (DDL, DML, or query) on Databricks.

    Args:
        sql: The SQL statement to execute (CREATE TABLE, INSERT, SELECT, MERGE, etc.)

    Returns:
        JSON string with success status, columns, row count, and rows (for SELECT).
    """
    try:
        result = _execute_sql_raw(sql)
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def create_table(ddl: str) -> str:
    """Create a Unity Catalog table from a CREATE TABLE DDL statement.

    Args:
        ddl: The full CREATE TABLE DDL statement

    Returns:
        JSON string with success status.
    """
    try:
        result = _execute_sql_raw(ddl)
        if result["success"]:
            return json.dumps({"success": True, "message": "Table created successfully"})
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def write_notebook(path: str, content: str, language: str = "PYTHON") -> str:
    """Write a notebook to the Databricks workspace.

    Args:
        path: Full workspace path (e.g., /Workspace/Users/user/project/my_notebook)
        content: The notebook source code as a string
        language: Notebook language - PYTHON, SQL, SCALA, or R (default: PYTHON)

    Returns:
        JSON string with success status.
    """
    try:
        # Ensure parent directory exists
        parent = os.path.dirname(path)
        try:
            w.workspace.mkdirs(parent)
        except Exception:
            pass  # Directory may already exist

        # Map language string to SDK Language enum
        lang_map = {
            "PYTHON": Language.PYTHON,
            "SQL": Language.SQL,
            "SCALA": Language.SCALA,
            "R": Language.R,
        }
        lang = lang_map.get(language.upper(), Language.PYTHON)

        # Import as notebook source
        content_bytes = content.encode("utf-8")
        w.workspace.import_workspace(
            path=path,
            format=ImportFormat.SOURCE,
            language=lang,
            content=base64.b64encode(content_bytes).decode(),
            overwrite=True,
        )
        return json.dumps({"success": True, "message": f"Notebook written to {path}"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def run_notebook(notebook_path: str, params: str = "{}") -> str:
    """Run a notebook as a one-time job and return the results.

    Args:
        notebook_path: Full workspace path to the notebook (e.g., /Workspace/Users/user/project/pipeline)
        params: JSON string of notebook parameters (default: {})

    Returns:
        JSON string with job run status and output.
    """
    try:
        notebook_params = json.loads(params)
        # Submit a one-time job run
        run = w.jobs.submit(
            tasks=[
                {
                    "task_key": "run_notebook",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": notebook_params,
                    },
                }
            ]
        )
        run_id = run.run_id

        # Wait for completion (max 10 minutes)
        max_wait = 600
        waited = 0
        while waited < max_wait:
            run_state = w.jobs.get_run(run_id=run_id)
            state = run_state.state
            if state.life_cycle_state == "TERMINATED":
                if state.result_state == "SUCCESS":
                    # Try to get notebook output
                    try:
                        output = w.jobs.get_run_output(run_id=run_id)
                        return json.dumps({
                            "success": True,
                            "run_id": run_id,
                            "output": output.logs if hasattr(output, "logs") and output.logs else "Completed successfully",
                        })
                    except Exception:
                        return json.dumps({"success": True, "run_id": run_id, "output": "Completed successfully"})
                else:
                    return json.dumps({
                        "success": False,
                        "run_id": run_id,
                        "error": f"Job failed with state: {state.result_state}",
                    })
            time.sleep(5)
            waited += 5

        return json.dumps({"success": False, "run_id": run_id, "error": "Job timed out after 10 minutes"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def git_commit(commit_message: str, repo_path: str = GIT_REPO_PATH) -> str:
    """Commit and push changes to the Git repository.

    Args:
        commit_message: The commit message for the changes
        repo_path: Databricks Git folder path (default: /Repos/vedavyas.goparaju/pc-insurance-medallion)

    Returns:
        JSON string with commit status.
    ""
    try:
        # Convert Databricks repo path to filesystem path
        repo_fs_path = f"/Workspace{repo_path}"

        # Get the current branch using git CLI
        branch_result = subprocess.run(
            ["git", "-C", repo_fs_path, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True, text=True, timeout=30
        )
        if branch_result.returncode != 0:
            return json.dumps({"success": False, "error": f"Failed to get branch: {branch_result.stderr}"})
        branch = branch_result.stdout.strip()

        # Get the list of changed files using git CLI
        status_result = subprocess.run(
            ["git", "-C", repo_fs_path, "status", "--porcelain"],
            capture_output=True, text=True, timeout=30
        )
        if status_result.returncode != 0:
            return json.dumps({"success": False, "error": f"Failed to get status: {status_result.stderr}"})

        changes = []
        for line in status_result.stdout.strip().split("\n"):
            if line:
                action = line[:2].strip()
                path = line[3:]
                changes.append({"path": path, "action": action})

        if not changes:
            return json.dumps({"success": True, "message": "No changes to commit"})

        # Stage all changes
        add_result = subprocess.run(
            ["git", "-C", repo_fs_path, "add", "-A"],
            capture_output=True, text=True, timeout=60
        )
        if add_result.returncode != 0:
            return json.dumps({"success": False, "error": f"git add failed: {add_result.stderr}"})

        # Create commit
        commit_result = subprocess.run(
            ["git", "-C", repo_fs_path, "commit", "-m", commit_message],
            capture_output=True, text=True, timeout=60
        )
        if commit_result.returncode != 0:
            if "nothing to commit" in commit_result.stdout:
                return json.dumps({"success": True, "message": "No changes to commit"})
            return json.dumps({"success": False, "error": f"git commit failed: {commit_result.stderr}"})

        # Push to remote
        push_result = subprocess.run(
            ["git", "-C", repo_fs_path, "push", "origin", branch],
            capture_output=True, text=True, timeout=120
        )
        if push_result.returncode != 0:
            return json.dumps({"success": False, "error": f"git push failed: {push_result.stderr}", "commit": commit_result.stdout.strip()})

        return json.dumps({
            "success": True,
            "message": f"Committed and pushed {len(changes)} files to {branch}",
            "branch": branch,
            "files": [c["path"] for c in changes],
        })
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def run_dq_checks(table_name: str) -> str:
    """Run all data quality validation functions from pc_insurance.dq against a table.

    Args:
        table_name: Full table name (e.g., pc_insurance.bronze.policies_raw)

    Returns:
        JSON string with pass/fail for each DQ check.
    """
    try:
        results = {}
        overall_pass = True

        for func_name in DQ_FUNCTIONS:
            full_func = f"{CATALOG_NAME}.dq.{func_name}"
            try:
                # DQ functions accept table_name as parameter and return a boolean or score
                sql = f"SELECT {full_func}('{table_name}') AS result"
                result = _execute_sql_raw(sql, wait_timeout="120s")
                if result["success"] and result["rows"]:
                    val = result["rows"][0][0] if result["rows"][0] else None
                    if func_name == "calculate_dq_score":
                        results[func_name] = {"score": val}
                        overall_pass = overall_pass and (val is not None)
                    else:
                        passed = bool(val) if val is not None else False
                        results[func_name] = {"pass": passed, "value": val}
                        overall_pass = overall_pass and passed
                else:
                    results[func_name] = {"error": result.get("error", "No result")}
                    overall_pass = False
            except Exception as e:
                results[func_name] = {"error": str(e)}
                overall_pass = False

        results["overall_pass"] = overall_pass
        return json.dumps(results, indent=2)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def get_table_schema(table_name: str) -> str:
    """Get the schema (column names and types) of a Unity Catalog table.

    Args:
        table_name: Full table name (e.g., pc_insurance.bronze.policies_raw)

    Returns:
        JSON string with column names, types, and comments.
    """
    try:
        # Try SDK tables API first
        try:
            parts = table_name.split(".")
            if len(parts) == 3:
                catalog, schema, name = parts
            elif len(parts) == 2:
                catalog = "main"
                schema, name = parts
            else:
                catalog, schema, name = "main", "default", table_name

            info = w.tables.get(full_name=table_name)
            columns = []
            if info.columns:
                for col in info.columns:
                    columns.append({
                        "name": col.name,
                        "type": col.type_name,
                        "comment": col.comment or "",
                        "nullable": col.nullable if col.nullable is not None else True,
                    })
            return json.dumps({
                "success": True,
                "table": table_name,
                "columns": columns,
            }, indent=2)
        except Exception:
            # Fallback: use DESCRIBE TABLE
            sql = f"DESCRIBE TABLE {table_name}"
            result = _execute_sql_raw(sql)
            if result["success"]:
                cols = []
                for row in result["rows"]:
                    if row and row[0] and not row[0].startswith("#"):
                        cols.append({"name": row[0], "type": row[1] if len(row) > 1 else "", "comment": row[2] if len(row) > 2 else ""})
                return json.dumps({"success": True, "table": table_name, "columns": cols}, indent=2)
            return json.dumps(result)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def query_table(sql: str) -> str:
    """Run a SELECT query and return the results as JSON.

    Args:
        sql: A SELECT query to execute (e.g., SELECT * FROM pc_insurance.bronze.policies_raw LIMIT 10)

    Returns:
        JSON string with columns, row count, and rows.
    """
    try:
        # Basic validation - only allow SELECT
        sql_stripped = sql.strip().upper()
        if not sql_stripped.startswith("SELECT") and not sql_stripped.startswith("WITH"):
            return json.dumps({"success": False, "error": "Only SELECT or WITH queries are allowed"})
        result = _execute_sql_raw(sql, wait_timeout="120s")
        return json.dumps(result, indent=2)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def update_file(file_path: str, content: str) -> str:
    """Write or update a file in the Databricks workspace.

    Args:
        file_path: Full workspace path (e.g., /Workspace/Users/user/project/README.md)
        content: The file content as a string

    Returns:
        JSON string with success status.
    """
    try:
        # Ensure parent directory exists
        parent = os.path.dirname(file_path)
        try:
            w.workspace.mkdirs(parent)
        except Exception:
            pass

        content_bytes = content.encode("utf-8")
        w.workspace.import_workspace(
            path=file_path,
            format=ImportFormat.AUTO,
            content=base64.b64encode(content_bytes).decode(),
            overwrite=True,
        )
        return json.dumps({"success": True, "message": f"File written to {file_path}"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


@mcp.tool()
def insert_config_row(table_name: str, columns: str) -> str:
    """Insert a row into a metadata config table (e.g., bronze_ingestion_config).

    Args:
        table_name: Full table name (e.g., pc_insurance.reference.bronze_ingestion_config)
        columns: JSON string mapping column names to values (e.g., {"source_name": "policies", "source_format": "csv"})

    Returns:
        JSON string with success status.
    """
    try:
        col_map = json.loads(columns)
        if not col_map:
            return json.dumps({"success": False, "error": "No columns provided"})

        col_names = list(col_map.keys())
        col_values = []
        for v in col_map.values():
            if isinstance(v, str):
                col_values.append(f"'{v.replace(chr(39), chr(39)+chr(39))}'")
            elif v is None:
                col_values.append("NULL")
            elif isinstance(v, bool):
                col_values.append("true" if v else "false")
            elif isinstance(v, (int, float)):
                col_values.append(str(v))
            else:
                col_values.append(f"'{str(v)}'")

        sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(col_values)})"
        result = _execute_sql_raw(sql)
        if result["success"]:
            return json.dumps({"success": True, "message": f"Row inserted into {table_name}"})
        return json.dumps(result)
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


# ==================== ENTRY POINT ====================


if __name__ == "__main__":
    mcp.run(transport="sse")