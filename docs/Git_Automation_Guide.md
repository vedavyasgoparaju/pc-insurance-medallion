# Git Automation Workflow

## Overview

The Git Automation Workflow automatically commits and pushes changes to the Git repository after successful pipeline execution and validation. This ensures that all workspace changes are synchronized with the remote repository without manual intervention.

## Architecture

### Components

1. **app/app.py** - MCP server (`pc-insurance-workspace-actions`) with subprocess git CLI (git add, commit, push)
2. **pipelines/Orchestrator.py** - Pipeline orchestrator (can trigger git via MCP app)
3. **app/app.yaml** - Databricks App configuration for the MCP server

### Workflow Flow

```
Pipeline Execution
       ↓
Validation Success
       ↓
MCP App (app/app.py — pc-insurance-workspace-actions)
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
   │ 3. Stage All    │ ← git add -A (subprocess)
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │ 4. Commit       │ ← git commit -m "message" (subprocess)
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │ 5. Push (retry) │ ← git push origin main (subprocess, with retries)
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

### Basic Usage (via MCP App)

The git automation is handled by the `pc-insurance-workspace-actions` MCP app. The Supervisor Agent routes git/file/SQL execution requests to it:

```python
# The MCP app exposes tools that the Supervisor Agent can call
# Git operations are executed via subprocess CLI:
#   subprocess.run(["git", "add", "-A"], cwd=repo_path)
#   subprocess.run(["git", "commit", "-m", commit_message], cwd=repo_path)
#   subprocess.run(["git", "push", "origin", "main"], cwd=repo_path)
```

### With Custom Parameters

The MCP app accepts parameters via MCP tool calls:

```python
# Parameters passed through MCP protocol:
# - commit_message: Custom commit message (default: auto-generated with timestamp)
# - push_enabled: Enable/disable push (default: true)
# - repo_path: Repository path (default: configured in app)
```

### Direct App invocation

To restart or check the MCP app:

```bash
# Check app status
databricks apps get pc-insurance-workspace-actions

# View app logs
databricks apps logs pc-insurance-workspace-actions

# Restart the app
databricks apps stop pc-insurance-workspace-actions
databricks apps start pc-insurance-workspace-actions
```

## Configuration

### Environment Variables

Set these in `app/app.py`:

```python
# Repository path
REPO_PATH = "/Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion"

# Retry configuration
MAX_PUSH_RETRIES = 3
RETRY_DELAY_SECONDS = 5
```

### MCP Tool Parameters

The MCP app exposes git operations as tools. Parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `commit_message` | str | Auto-generated | Custom commit message |
| `push_enabled` | bool | True | Enable/disable push to remote |
| `repo_path` | str | Configured | Override repository path |
| `skip_if_no_changes` | bool | True | Skip commit when no changes detected |

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

## Databricks App Integration

### MCP App Deployment

The `pc-insurance-workspace-actions` MCP app runs as a Databricks App:

```bash
# Deploy the app
databricks apps deploy pc-insurance-workspace-actions

# Check status
databricks apps get pc-insurance-workspace-actions

# View logs
databricks apps logs pc-insurance-workspace-actions
```

### Supervisor Agent Integration

The MCP app is registered as the 8th tool in the Supervisor Agent. The Supervisor routes git/file/SQL execution requests to it via MCP protocol. No separate job or workflow needed — the MCP app is always available when running.

### Triggered After Pipeline

The Supervisor Agent can orchestrate git commits after pipeline completion by routing the request to the MCP app tool.

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
1. Verify `REPO_PATH` in `app/app.py`
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

**Action**: No action needed - MCP app skips commit

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

1. Modify `app/app.py` in the repo
2. Commit and push changes to GitHub
3. Redeploy the MCP app: `databricks apps deploy pc-insurance-workspace-actions`
4. Monitor first few automated runs via app logs

## Support

For issues or questions:

- **Architecture**: Architect agent
- **Implementation**: Data Engineer agent
- **Deployment**: DevOps agent (guidance only)
- **Git execution**: Workspace-Actions MCP app
- **Documentation**: Documentation agent

## Version History

- **v2.0** (2026-09-25): MCP App migration
  - Replaced `Git_Automation.py` with MCP app `app/app.py`
  - Uses subprocess git CLI (robust, no SDK dependency)
  - Integrated as 8th tool in Supervisor Agent
  - Removed `git_auto_push_workflow.yml` (no longer needed)
- **v1.0** (2026-09-24): Initial implementation
  - Core automation functions (Git_Automation.py — now deprecated)
  - Retry logic for push
  - Repository health checks
  - Integration with Orchestrator

---

**Last Updated**: 2026-09-25
**Maintained By**: DevOps Team
**Status**: Production Ready
