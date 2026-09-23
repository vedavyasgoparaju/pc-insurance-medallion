"""Autonomously convert a user request into and run an execution plan."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from databricks.sdk import WorkspaceClient

from plan_executor import _single_statement, _workspace_path

SUPERVISOR_ENDPOINT = os.getenv("SUPERVISOR_ENDPOINT", "mas-0bcacd94-endpoint")
WAREHOUSE_ID = os.getenv("SQL_WAREHOUSE_ID", "670b9d31fd290bb2")
WORKSPACE_ROOT = os.getenv("WORKSPACE_ROOT", "/Users/vedavyas.goparaju@gmail.com/InsuranceModel")
REPO_PATH = os.getenv("REPO_PATH", "/Repos/vedavyas.goparaju/pc-insurance-medallion")
REQUEST_TABLE = "pc_insurance.reference.agent_requests"


def _supervisor_text(response: dict[str, Any]) -> str:
    texts = []
    for item in response.get("output", []):
        for content in item.get("content", []):
            if content.get("type") == "output_text":
                texts.append(content.get("text", ""))
    if not texts:
        raise RuntimeError("Supervisor returned no plan text")
    return "\n".join(texts).strip()


def _parse_plan(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    plan = json.loads(cleaned)
    if plan.get("version") != 1 or not isinstance(plan.get("operations"), list) or not plan["operations"]:
        raise ValueError("Supervisor returned an invalid execution plan")
    allowed = {"write_workspace_file", "execute_sql", "run_notebook", "git_commit"}
    for operation in plan["operations"]:
        operation_type = operation.get("type")
        if operation_type not in allowed:
            raise ValueError(f"Unsupported planned operation: {operation_type}")
        if operation_type == "write_workspace_file":
            _workspace_path(operation["path"])
            if not operation.get("content"):
                raise ValueError("Planned workspace write has no content")
        elif operation_type == "execute_sql":
            operation.setdefault("warehouse_id", WAREHOUSE_ID)
            _single_statement(operation["sql"])
        elif operation_type == "run_notebook":
            _workspace_path(operation["notebook_path"])
        elif operation_type == "git_commit":
            _workspace_path(operation["repo_path"])
            if not operation.get("commit_message", "").strip():
                raise ValueError("Planned Git commit has no message")
    return plan


def _request_plan(client: WorkspaceClient, request: str) -> dict[str, Any]:
    prompt = f"""Convert this request into an execution plan for the P&C Insurance Medallion project.
Return ONLY valid JSON with this shape: {{\"version\":1,\"request_id\":\"...\",\"operations\":[...]}}.
Allowed operation types are write_workspace_file, execute_sql, run_notebook, and git_commit.
Use workspace paths below {WORKSPACE_ROOT} and use this Databricks Repo for commits: {REPO_PATH}.
For any request that changes code, schemas, pipelines, configuration, or documentation, always include these final steps in the plan:
1. Ask the Documentation specialist to update the affected README, runbook, or architecture documentation and include those file writes.
2. Ask the QA specialist to define and run relevant validation before committing.
3. Ask the DevOps specialist to prepare a descriptive Git commit operation as the final operation, using repo_path {REPO_PATH}.
Do not include a Git commit for read-only questions or failed validation. Do not call tools. Do not return Markdown or explanations.
User request: {request}"""
    response = client._api_client.do(
        "POST",
        f"/serving-endpoints/{SUPERVISOR_ENDPOINT}/invocations",
        body={"input": [{"role": "user", "content": prompt}]},
    )
    return _parse_plan(_supervisor_text(response))


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _execute_control_sql(client: WorkspaceClient, statement: str) -> None:
    result = client.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s",
    )
    if result.status and result.status.state and result.status.state.value == "FAILED":
        message = result.status.error.message if result.status.error else "Control SQL failed"
        raise RuntimeError(message)


def _pending_requests(client: WorkspaceClient) -> list[dict[str, str]]:
    result = client.statement_execution.execute_statement(
        statement=f"SELECT request_id, request_text FROM {REQUEST_TABLE} WHERE status = 'PENDING' ORDER BY submitted_at LIMIT 10",
        warehouse_id=WAREHOUSE_ID,
        wait_timeout="50s",
    )
    rows = result.result.data_array if result.result and result.result.data_array else []
    return [{"request_id": row[0], "request_text": row[1]} for row in rows]


def _set_request_status(client: WorkspaceClient, request_id: str, status: str, plan: dict[str, Any] | None = None, execution_run_id: int | None = None, error: str = "") -> None:
    values = [f"status = '{_sql_literal(status)}'"]
    if plan is not None:
        values.append(f"plan_json = '{_sql_literal(json.dumps(plan, separators=(',', ':')))}'")
    if execution_run_id is not None:
        values.append(f"execution_run_id = {execution_run_id}")
    if error:
        values.append(f"error_message = '{_sql_literal(error[:4000])}'")
    values.append("updated_at = current_timestamp()")
    _execute_control_sql(client, f"UPDATE {REQUEST_TABLE} SET {', '.join(values)} WHERE request_id = '{_sql_literal(request_id)}'")


def _process_request(client: WorkspaceClient, request_id: str, request: str, execution_job_id: int) -> dict[str, Any]:
    _set_request_status(client, request_id, "RUNNING")
    try:
        plan = _request_plan(client, request)
        _set_request_status(client, request_id, "PLANNED", plan=plan)
        run = client.jobs.run_now(
            job_id=execution_job_id,
            job_parameters={"execution_plan": json.dumps(plan, separators=(",", ":"))},
        )
        execution = _wait_for_run(client, run.run_id)
        if execution.get("result_state") != "SUCCESS":
            raise RuntimeError(f"Execution Job failed: {execution}")
        _set_request_status(client, request_id, "SUCCEEDED", execution_run_id=run.run_id)
        return {"request_id": request_id, "success": True, "plan": plan, "execution": execution}
    except Exception as exc:
        _set_request_status(client, request_id, "FAILED", error=str(exc))
        return {"request_id": request_id, "success": False, "error": str(exc)}


def _wait_for_run(client: WorkspaceClient, run_id: int) -> dict[str, Any]:
    for _ in range(120):
        run = client.jobs.get_run(run_id=run_id)
        life_cycle = run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else "UNKNOWN"
        result = run.state.result_state.value if run.state and run.state.result_state else None
        if life_cycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            return {"run_id": run_id, "life_cycle_state": life_cycle, "result_state": result}
        time.sleep(5)
    return {"run_id": run_id, "life_cycle_state": "TIMEOUT"}


def main() -> None:
    global SUPERVISOR_ENDPOINT, WAREHOUSE_ID, WORKSPACE_ROOT, REPO_PATH
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", default="")
    parser.add_argument("--execution-job-id", type=int, required=True)
    parser.add_argument("--supervisor-endpoint", default=SUPERVISOR_ENDPOINT)
    parser.add_argument("--warehouse-id", default=WAREHOUSE_ID)
    parser.add_argument("--workspace-root", default=WORKSPACE_ROOT)
    parser.add_argument("--repo-path", default=REPO_PATH)
    parser.add_argument("--allowed-roots", default=os.getenv("EXECUTION_ALLOWED_ROOTS", ""))
    args = parser.parse_args()

    SUPERVISOR_ENDPOINT = args.supervisor_endpoint
    WAREHOUSE_ID = args.warehouse_id
    WORKSPACE_ROOT = args.workspace_root
    REPO_PATH = args.repo_path
    if args.allowed_roots:
        os.environ["EXECUTION_ALLOWED_ROOTS"] = args.allowed_roots

    client = WorkspaceClient()
    if args.request.strip():
        result = {"success": True, "direct_request": _request_plan(client, args.request)}
        plan = result["direct_request"]
        run = client.jobs.run_now(
            job_id=args.execution_job_id,
            job_parameters={"execution_plan": json.dumps(plan, separators=(",", ":"))},
        )
        execution = _wait_for_run(client, run.run_id)
        result["execution"] = execution
        result["success"] = execution.get("result_state") == "SUCCESS"
        if not result["success"]:
            raise RuntimeError(f"Execution Job failed: {execution}")
    else:
        result = {"success": True, "processed": [_process_request(client, item["request_id"], item["request_text"], args.execution_job_id) for item in _pending_requests(client)]}
        result["success"] = all(item["success"] for item in result["processed"])
        if not result["success"]:
            raise RuntimeError("One or more queued agent requests failed")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
