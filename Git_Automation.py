# Databricks notebook source
# MAGIC %md
# MAGIC # Git Automation Workflow
# MAGIC 
# MAGIC Automatically commits and pushes changes to Git after successful pipeline execution and validation.
# MAGIC 
# MAGIC **Usage:**
# MAGIC - Call `auto_commit_and_push()` at the end of your pipeline
# MAGIC - Requires: Git repository initialized and remote configured
# MAGIC - Validates changes before committing
# MAGIC - Includes retry logic for push failures

# COMMAND ----------

import subprocess
import os
from datetime import datetime
from typing import Tuple, Dict, Any
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Git repository path (configured for Databricks Repos)
REPO_PATH = "/Workspace/Repos/vedavyas.goparaju/pc-insurance-medallion"

# Retry configuration for push operations
MAX_PUSH_RETRIES = 3
RETRY_DELAY_SECONDS = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def run_git_command(command: list, cwd: str = REPO_PATH) -> Tuple[bool, str, str]:
    """
    Execute a git command and return success status and output.
    
    Args:
        command: List of command arguments (e.g., ['git', 'status'])
        cwd: Working directory for the command
        
    Returns:
        Tuple of (success: bool, stdout: str, stderr: str)
    """
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=30
        )
        success = result.returncode == 0
        return success, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out after 30 seconds"
    except Exception as e:
        return False, "", str(e)

# COMMAND ----------

def get_git_status() -> Dict[str, Any]:
    """
    Get the current git repository status.
    
    Returns:
        Dictionary with status information
    """
    success, stdout, stderr = run_git_command(['git', 'status', '--porcelain'])
    
    if not success:
        return {
            'has_changes': False,
            'error': stderr,
            'files': []
        }
    
    # Parse porcelain output
    files = []
    for line in stdout.strip().split('\n'):
        if line:
            status = line[:2].strip()
            filename = line[3:]
            files.append({'status': status, 'file': filename})
    
    return {
        'has_changes': len(files) > 0,
        'files': files,
        'error': None
    }

# COMMAND ----------

def check_repo_health() -> Tuple[bool, str]:
    """
    Check if the Git repository is in a healthy state.
    
    Returns:
        Tuple of (is_healthy: bool, message: str)
    """
    # Check if directory exists
    if not os.path.exists(REPO_PATH):
        return False, f"Repository path does not exist: {REPO_PATH}"
    
    # Check if it's a git repository
    success, stdout, stderr = run_git_command(['git', 'rev-parse', '--git-dir'])
    if not success:
        return False, f"Not a git repository: {stderr}"
    
    # Check if remote is configured
    success, stdout, stderr = run_git_command(['git', 'remote', '-v'])
    if not success or not stdout.strip():
        return False, "No remote repository configured"
    
    # Check current branch
    success, stdout, stderr = run_git_command(['git', 'branch', '--show-current'])
    if not success:
        return False, f"Cannot determine current branch: {stderr}"
    
    branch = stdout.strip()
    if not branch:
        return False, "Not on any branch (detached HEAD)"
    
    return True, f"Repository healthy, on branch: {branch}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Git Automation Functions

# COMMAND ----------

def stage_all_changes() -> Tuple[bool, str]:
    """
    Stage all changes in the repository.
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    success, stdout, stderr = run_git_command(['git', 'add', '-A'])
    
    if success:
        return True, "All changes staged successfully"
    else:
        return False, f"Failed to stage changes: {stderr}"

# COMMAND ----------

def create_commit(message: str, author: str = None) -> Tuple[bool, str, str]:
    """
    Create a git commit with the given message.
    
    Args:
        message: Commit message
        author: Optional author string (e.g., "Name <email>")
        
    Returns:
        Tuple of (success: bool, commit_hash: str, message: str)
    """
    # Build commit command
    cmd = ['git', 'commit', '-m', message]
    if author:
        cmd.extend(['--author', author])
    
    success, stdout, stderr = run_git_command(cmd)
    
    if success:
        # Extract commit hash from output
        success_hash, hash_stdout, _ = run_git_command(['git', 'rev-parse', 'HEAD'])
        commit_hash = hash_stdout.strip()[:7] if success_hash else "unknown"
        return True, commit_hash, f"Commit created: {commit_hash}"
    else:
        # Check if there's nothing to commit
        if "nothing to commit" in stderr or "nothing to commit" in stdout:
            return True, "", "No changes to commit"
        return False, "", f"Failed to create commit: {stderr}"

# COMMAND ----------

def push_to_remote(branch: str = None, retries: int = MAX_PUSH_RETRIES) -> Tuple[bool, str]:
    """
    Push commits to remote repository with retry logic.
    
    Args:
        branch: Branch name to push (default: current branch)
        retries: Number of retry attempts
        
    Returns:
        Tuple of (success: bool, message: str)
    """
    import time
    
    # Get current branch if not specified
    if not branch:
        success, stdout, stderr = run_git_command(['git', 'branch', '--show-current'])
        if not success:
            return False, f"Cannot determine current branch: {stderr}"
        branch = stdout.strip()
    
    # Try pushing with retries
    for attempt in range(1, retries + 1):
        print(f"Push attempt {attempt}/{retries}...")
        
        success, stdout, stderr = run_git_command(['git', 'push', 'origin', branch])
        
        if success:
            return True, f"Successfully pushed to origin/{branch}"
        
        # Check if it's a network error that might be retryable
        if "Could not resolve host" in stderr or "Failed to connect" in stderr:
            if attempt < retries:
                print(f"Network error, retrying in {RETRY_DELAY_SECONDS} seconds...")
                time.sleep(RETRY_DELAY_SECONDS)
                continue
        
        # Non-retryable error or last attempt
        return False, f"Push failed after {attempt} attempts: {stderr}"
    
    return False, f"Push failed after {retries} attempts"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Automation Function

# COMMAND ----------

def auto_commit_and_push(
    commit_message: str = None,
    author: str = None,
    skip_if_no_changes: bool = True,
    push_enabled: bool = True
) -> Dict[str, Any]:
    """
    Automatically commit and push changes to Git.
    
    This is the main function to call at the end of your pipeline.
    
    Args:
        commit_message: Custom commit message (auto-generated if None)
        author: Optional author string
        skip_if_no_changes: If True, skip commit when no changes detected
        push_enabled: If False, only commit locally without pushing
        
    Returns:
        Dictionary with operation results and status
    """
    result = {
        'timestamp': datetime.now().isoformat(),
        'success': False,
        'repo_health': None,
        'changes_detected': False,
        'staged': False,
        'committed': False,
        'commit_hash': None,
        'pushed': False,
        'messages': []
    }
    
    # Step 1: Check repository health
    print("=" * 80)
    print("GIT AUTOMATION WORKFLOW")
    print("=" * 80)
    print("\nStep 1: Checking repository health...")
    
    is_healthy, health_msg = check_repo_health()
    result['repo_health'] = health_msg
    result['messages'].append(f"Health check: {health_msg}")
    
    if not is_healthy:
        result['messages'].append(f"❌ Repository health check failed: {health_msg}")
        print(f"❌ {health_msg}")
        return result
    
    print(f"✅ {health_msg}")
    
    # Step 2: Check for changes
    print("\nStep 2: Checking for changes...")
    status = get_git_status()
    
    if status['error']:
        result['messages'].append(f"❌ Failed to get git status: {status['error']}")
        print(f"❌ Failed to get git status: {status['error']}")
        return result
    
    result['changes_detected'] = status['has_changes']
    
    if not status['has_changes']:
        if skip_if_no_changes:
            result['success'] = True
            result['messages'].append("✅ No changes detected, skipping commit")
            print("✅ No changes detected, skipping commit")
            return result
        else:
            result['messages'].append("⚠️ No changes detected, but proceeding anyway")
            print("⚠️ No changes detected")
    else:
        print(f"✅ Found {len(status['files'])} changed file(s):")
        for file_info in status['files'][:10]:  # Show first 10
            print(f"   {file_info['status']} {file_info['file']}")
        if len(status['files']) > 10:
            print(f"   ... and {len(status['files']) - 10} more")
    
    # Step 3: Stage changes
    print("\nStep 3: Staging changes...")
    stage_success, stage_msg = stage_all_changes()
    result['staged'] = stage_success
    result['messages'].append(stage_msg)
    
    if not stage_success:
        print(f"❌ {stage_msg}")
        return result
    
    print(f"✅ {stage_msg}")
    
    # Step 4: Create commit
    print("\nStep 4: Creating commit...")
    
    if not commit_message:
        commit_message = f"auto: pipeline execution completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    commit_success, commit_hash, commit_msg = create_commit(commit_message, author)
    result['committed'] = commit_success
    result['commit_hash'] = commit_hash
    result['messages'].append(commit_msg)
    
    if not commit_success:
        print(f"❌ {commit_msg}")
        return result
    
    print(f"✅ {commit_msg}")
    
    # Step 5: Push to remote (if enabled)
    if push_enabled:
        print("\nStep 5: Pushing to remote...")
        push_success, push_msg = push_to_remote()
        result['pushed'] = push_success
        result['messages'].append(push_msg)
        
        if push_success:
            print(f"✅ {push_msg}")
            result['success'] = True
        else:
            print(f"❌ {push_msg}")
            print("\n⚠️ Changes are committed locally but not pushed to remote.")
            print("   You can manually push using the Databricks Repos UI.")
    else:
        result['success'] = True
        result['messages'].append("Push skipped (push_enabled=False)")
        print("\n⚠️ Push skipped (push_enabled=False)")
    
    print("\n" + "=" * 80)
    print("WORKFLOW COMPLETE")
    print("=" * 80)
    
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Example 1: Basic usage (auto-commit and push)
# result = auto_commit_and_push()
# print(json.dumps(result, indent=2))

# Example 2: Custom commit message
# result = auto_commit_and_push(
#     commit_message="feat: added new Gold KPI for loss ratio by region"
# )

# Example 3: Commit only, no push
# result = auto_commit_and_push(
#     commit_message="wip: work in progress",
#     push_enabled=False
# )

# Example 4: With custom author
# result = auto_commit_and_push(
#     commit_message="fix: corrected SCD2 logic in Silver pipeline",
#     author="Data Engineer <engineer@company.com>"
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integration with Orchestrator
# MAGIC 
# MAGIC Add this to the end of your Orchestrator.py:
# MAGIC 
# MAGIC ```python
# MAGIC # Import the Git automation module
# MAGIC dbutils.notebook.run("./Git_Automation", timeout_seconds=300)
# MAGIC 
# MAGIC # Call auto-commit after successful pipeline execution
# MAGIC result = auto_commit_and_push(
# MAGIC     commit_message=f"auto: {pipeline_name} completed successfully"
# MAGIC )
# MAGIC 
# MAGIC if not result['success']:
# MAGIC     print("Warning: Git automation failed, but pipeline completed successfully")
# MAGIC     print("Manual push may be required")
# MAGIC ```
