# Git Automation Workflow

## Overview

The Git Automation Workflow automatically commits and pushes changes to the Git repository after successful pipeline execution and validation. This ensures that all workspace changes are synchronized with the remote repository without manual intervention.

## Architecture

### Components

1. **Git_Automation.py** - Core automation notebook with retry logic and error handling
2. **Orchestrator.py** - Integrated with Git automation as final step
3. **git_auto_push_workflow.yml** - Databricks workflow configuration for scheduled/triggered execution

### Workflow Flow

```
Pipeline Execution
       ↓
Validation Success
       ↓
Git Automation (Git_Automation.py)
       ↓
   ┌─────────────────┐
   │ 1. Health Check │ ← Verify repo state, remote config, branch
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │ 2. Check Changes│ ← git status --porcelain
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │ 3. Stage All    │ ← git add -A
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │ 4. Commit       │ ← git commit -m "message"
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │ 5. Push (retry) │ ← git push origin main (with retries)
   └────────┬────────┘
            ↓
      Success/Failure
```

## Features

### 1. Repository Health Checks
- Validates Git repository exists and is initialized
- Checks remote repository configuration
- Verifies current branch is valid (not detached HEAD)

### 2. Change Detection
- Uses `git status --porcelain` for reliable change detection
- Lists all modified, added, and deleted files
- Skips commit if no changes (configurable)

### 3. Automatic Staging
- Stages all changes with `git add -A`
- Includes new files, modifications, and deletions

### 4. Smart Commit Creation
- Auto-generates commit messages with timestamp
- Supports custom commit messages
- Optional author attribution

### 5. Push with Retry Logic
- Attempts push up to 3 times (configurable)
- 5-second delay between retries
- Handles network connectivity issues gracefully

### 6. Comprehensive Logging
- Detailed step-by-step execution logs
- Clear success/failure indicators
- Actionable error messages

## Usage

### Basic Usage (in Orchestrator)

```python
# At the end of Orchestrator.py
result = dbutils.notebook.run(
    "./Git_Automation",
    timeout_seconds=300
)
```

### With Custom Parameters

```python
result = dbutils.notebook.run(
    "./Git_Automation",
    timeout_seconds=300,
    arguments={
        "commit_message": "feat: added new Gold KPI for loss ratio by region",
        "push_enabled": "true"
    }
)
```

### Direct Function Call (in Git_Automation.py)

```python
# Import and call the main function
result = auto_commit_and_push(
    commit_message="fix: corrected SCD2 logic in Silver pipeline",
    author="Data Engineer <engineer@company.com>",
    push_enabled=True
)

# Check result
if result['success']:
    print(f"✅ Changes committed and pushed: {result['commit_hash']}")
else:
    print(f"❌ Failed: {result['messages']}")
```

## Configuration

### Environment Variables

Set these in Git_Automation.py:

```python
# Repository path
REPO_PATH = "/Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion"

# Retry configuration
MAX_PUSH_RETRIES = 3
RETRY_DELAY_SECONDS = 5
```

### Function Parameters

#### `auto_commit_and_push()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `commit_message` | str | Auto-generated | Custom commit message |
| `author` | str | None | Author string (e.g., "Name <email>") |
| `skip_if_no_changes` | bool | True | Skip commit when no changes detected |
| `push_enabled` | bool | True | Enable/disable push to remote |

## Return Value

The `auto_commit_and_push()` function returns a dictionary:

```python
{
    'timestamp': '2026-09-24T10:30:00',
    'success': True,
    'repo_health': 'Repository healthy, on branch: main',
    'changes_detected': True,
    'staged': True,
    'committed': True,
    'commit_hash': 'a1b2c3d',
    'pushed': True,
    'messages': [
        'Health check: Repository healthy, on branch: main',
        'All changes staged successfully',
        'Commit created: a1b2c3d',
        'Successfully pushed to origin/main'
    ]
}
```

## Error Handling

### Network Connectivity Issues

If push fails due to network issues:

1. Changes are committed locally
2. Workflow logs the failure
3. Manual push can be done via Databricks Repos UI

### Repository Issues

If repository health check fails:

- Workflow stops before making changes
- Error message indicates the specific issue
- Manual intervention required

### No Changes Detected

If no changes are detected:

- Workflow skips commit (default behavior)
- Can be overridden with `skip_if_no_changes=False`

## Databricks Workflow Integration

### Standalone Workflow

Deploy the workflow:

```bash
databricks bundle deploy -t dev
```

Run manually:

```bash
databricks jobs run-now --job-id <job-id>
```

### Scheduled Execution

Uncomment the schedule block in `git_auto_push_workflow.yml`:

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM
  timezone_id: "America/New_York"
  pause_status: "UNPAUSED"
```

### Triggered After Pipeline

Chain workflows:

```yaml
tasks:
  - task_key: run_pipeline
    notebook_task:
      notebook_path: "/Repos/.../Orchestrator"
  
  - task_key: git_push
    depends_on:
      - task_key: run_pipeline
    notebook_task:
      notebook_path: "/Repos/.../Git_Automation"
```

## Compliance with Mandatory Change Completion Policy

This Git automation ensures compliance with the project's **Mandatory Change Completion Policy**:

✅ **Automatic Commit**: Changes are committed after validation
✅ **Documentation Updates**: Included in staged changes
✅ **Validation Integration**: Runs only after successful validation
✅ **Audit Trail**: Detailed commit messages and logs
✅ **No Manual Steps**: Fully automated workflow

## Troubleshooting

### Issue: "Could not resolve host: github.com"

**Cause**: Network connectivity from execution environment to GitHub

**Solutions**:
1. Use Databricks Repos UI to push manually
2. Configure Databricks to use a different network path
3. Use Databricks Git credentials for authentication

### Issue: "Not a git repository"

**Cause**: Repository not initialized or path incorrect

**Solutions**:
1. Verify `REPO_PATH` in Git_Automation.py
2. Ensure Databricks Repo is properly cloned
3. Check Git folder exists: `/Workspace/Repos/<user>/<repo>/.git`

### Issue: "No remote repository configured"

**Cause**: Git remote not set up

**Solutions**:
```bash
cd /Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion
git remote add origin https://github.com/<user>/<repo>.git
```

### Issue: "Nothing to commit"

**Cause**: No changes detected (expected behavior)

**Action**: No action needed - workflow skips commit

## Best Practices

1. **Commit Messages**: Use descriptive, conventional commit messages
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation
   - `chore:` for maintenance
   - `auto:` for automated commits

2. **Testing**: Test Git automation in dev environment before production

3. **Monitoring**: Set up email notifications for failures

4. **Backup**: Keep backup files until changes are verified in remote

5. **Rollback**: Use Git history to rollback if needed

## Security Considerations

- Git credentials managed by Databricks
- No credentials stored in code
- Uses Databricks-managed authentication
- Audit trail maintained in Git history

## Maintenance

### Regular Tasks

- Review automated commit messages for clarity
- Monitor push success rate
- Update retry configuration based on network reliability
- Clean up old backup files after successful pushes

### Updates

To update the Git automation:

1. Modify `Git_Automation.py`
2. Test in development environment
3. Deploy to production via bundle
4. Monitor first few automated runs

## Support

For issues or questions:

- **Architecture**: ARCHITECT agent
- **Implementation**: DATA ENGINEER agent
- **Deployment**: DEVOPS agent
- **Documentation**: DOCUMENTATION agent

## Version History

- **v1.0** (2026-09-24): Initial implementation
  - Core automation functions
  - Retry logic for push
  - Repository health checks
  - Integration with Orchestrator
  - Databricks workflow configuration

---

**Last Updated**: 2026-09-24
**Maintained By**: DevOps Team
**Status**: Production Ready
