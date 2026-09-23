"""Execute a validated multi-agent change plan in Databricks."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
from pathlib import PurePosixPath
from typing import Any

from databricks.sdk import WorkspaceClient

DEFAULT_ROOTS = (
    "/Users/vedavyas.goparaju@gmail.com/InsuranceModel",
    "/Users/vedavyas.goparaju@gmail.com/Supervisor_Agent_Setup",
)


def _allowed_roots() -> tuple[str, ...]:
    configured = os.getenv("EXECUTION_ALLOWED_ROOTS")
    return tuple(configured.split(",")) if configured else DEFAULT_ROOTS


def _workspace_path(value: str) -> str:
    path = str(PurePosixPath(value))
    if not path.startswith("/") or ".." in PurePosixPath(path).parts:
        raise ValueError(f"Invalid workspace path: {value}")
    if not any(path == root or path.startswith(root.rstrip("/") + "/") for root in _allowed_roots()):
        raise ValueError(f"Path is outside EXECUTION_ALLOWED_ROOTS: {value}")
    return path


def _single_statement(sql: str) -> str:
    statement = sql.strip()
    if not statement or ";" in statement:
        raise ValueError("SQL must contain exactly one non-empty statement")
    if re.match(r"^(DROP|GRANT|REVOKE)\b", statement, re.IGNORECASE):
        raise ValueError("Destructive or privilege-changing SQL is not allowed")
    return statement


def _write_file(client: WorkspaceClient, operation: dict[str, Any]) -> dict[str, Any]:
    path = _workspace_path(operation["path"])
    content = operation["content"].encode()
    client.workspace.mkdirs(str(PurePosixPath(path).parent))
    client.api_client.do(
        "POST",
        "/api/2.0/workspace/import",
        body={
            "path": path,
            "format": operation.get("format", "AUTO"),
            "language": operation.get("language", "PYTHON").upper(),
            "content": base64.b64encode(content).decode(),
            "overwrite": bool(operation.get("overwrite", False)),
        },
    )
    return {"operation": "write_workspace_file", "path": path}


def _execute_sql(client: WorkspaceClient, operation: dict[str, Any]) -> dict[str, Any]:
    warehouse_id = operation.get("warehouse_id") or os.environ.get("SQL_WAREHOUSE_ID")
    if not warehouse_id:
        raise ValueError("warehouse_id or SQL_WAREHOUSE_ID is required")
    result = client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=_single_statement(operation["sql"]),
        wait_timeout=operation.get("wait_timeout", "50s"),
    )
    state = result.status.state.value if result.status and result.status.state else "UNKNOWN"
    if state == "FAILED":
        message = result.status.error.message if result.status.error else "SQL execution failed"
        raise RuntimeError(message)
    return {"operation": "execute_sql", "state": state}


def _run_notebook(client: WorkspaceClient, operation: dict[str, Any]) -> dict[str, Any]:
    notebook_path = _workspace_path(operation["notebook_path"])
    run = client.jobs.submit(
        tasks=[
            {
                "task_key": "plan_notebook",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": operation.get("parameters", {}),
                },
            }
        ]
    )
    return {"operation": "run_notebook", "notebook_path": notebook_path, "run_id": run.run_id}


def _git_commit(client: WorkspaceClient, operation: dict[str, Any]) -> dict[str, Any]:
    repo_path = _workspace_path(operation["repo_path"])
    message = operation["commit_message"].strip()
    if not message:
        raise ValueError("commit_message cannot be empty")
    repo = client.repos.get(repo_path)
    status = client.git.get_status(repo_path=repo_path)
    changes = [{"path": change.path, "action": str(change.action)} for change in (status.changes or [])]
    if changes:
        client.git.create_commit(
            repo_path=repo_path,
            branch=repo.branch or "main",
            commit_message=message,
            changes=changes,
        )
    return {"operation": "git_commit", "repo_path": repo_path, "changed_files": len(changes)}


def execute(plan: dict[str, Any], client: WorkspaceClient | None = None) -> list[dict[str, Any]]:
    if plan.get("version") != 1:
        raise ValueError("Only execution plan version 1 is supported")
    operations = plan.get("operations")
    if not isinstance(operations, list) or not operations:
        raise ValueError("Plan must contain a non-empty operations list")

    client = client or WorkspaceClient()
    handlers = {
        "write_workspace_file": _write_file,
        "execute_sql": _execute_sql,
        "run_notebook": _run_notebook,
        "git_commit": _git_commit,
    }
    results = []
    for operation in operations:
        operation_type = operation.get("type")
        if operation_type not in handlers:
            raise ValueError(f"Unsupported operation: {operation_type}")
        results.append(handlers[operation_type](client, operation))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", help="Path to a local JSON execution plan")
    parser.add_argument("--plan-json", help="Inline JSON execution plan supplied by a Job parameter")
    args = parser.parse_args()
    if bool(args.plan) == bool(args.plan_json):
        parser.error("provide exactly one of --plan or --plan-json")
    if args.plan_json:
        plan = json.loads(args.plan_json)
    else:
        with open(args.plan, encoding="utf-8") as plan_file:
            plan = json.load(plan_file)
    print(json.dumps({"success": True, "results": execute(plan)}, indent=2))


if __name__ == "__main__":
    main()
