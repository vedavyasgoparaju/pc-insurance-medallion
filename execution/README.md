# Multi-agent execution plans

The Supervisor and specialist agents produce a versioned JSON plan. A Databricks Job runs `plan_executor.py` under a dedicated service principal. The Supervisor does not receive direct workspace write permissions.

## Supported operations

- `write_workspace_file`: writes only below `EXECUTION_ALLOWED_ROOTS`.
- `execute_sql`: runs one statement and rejects `DROP`, `GRANT`, and `REVOKE`.
- `run_notebook`: submits an approved notebook path as a one-time job.
- `git_commit`: commits the current changes in an approved repository.

## Local validation

```bash
.venv/bin/python -m py_compile execution/plan_executor.py
```

Set `EXECUTION_ALLOWED_ROOTS` and `SQL_WAREHOUSE_ID` in the execution Job environment. The example plan is intentionally a sandbox plan and should be reviewed before deployment.
