# Databricks notebook source
# MAGIC %md
# MAGIC # Git Sync Helper
# MAGIC 
# MAGIC Quick Git operations for Databricks Repos. Run cells as needed.
# MAGIC 
# MAGIC | Command | Action |
# MAGIC |---------|--------|
# MAGIC | `sync` | Pull latest from remote |
# MAGIC | `push` | Commit all + push |
# MAGIC | `status` | Show branch & changes |

# COMMAND ----------

# Configuration - Update these for your repo
REPO_PATH = "/Repos/shahab@example.com/Sustainable-Building-Energy-Benchmarking-Pipeline"
DEFAULT_BRANCH = "main"

# COMMAND ----------

import subprocess
import os

def run_git(cmd: str, repo_path: str = REPO_PATH) -> str:
    """Run a git command in the repo directory."""
    result = subprocess.run(
        f"cd {repo_path} && git {cmd}",
        shell=True,
        capture_output=True,
        text=True
    )
    output = result.stdout + result.stderr
    return output.strip()

def status():
    """Show repo status."""
    branch = run_git("branch --show-current")
    status = run_git("status --short")
    remote = run_git("remote -v | head -1")
    
    print(f"📍 Branch: {branch}")
    print(f"🔗 Remote: {remote.split()[1] if remote else 'not set'}")
    if status:
        print(f"📝 Changes:\n{status}")
    else:
        print("✅ Working tree clean")

def sync():
    """Pull latest changes from remote."""
    print("⬇️  Pulling latest changes...")
    result = run_git(f"pull origin {DEFAULT_BRANCH}")
    print(result if result else "✅ Already up to date")

def push(message: str = None):
    """Commit all changes and push."""
    # Check for changes
    status = run_git("status --short")
    if not status:
        print("✅ Nothing to commit")
        return
    
    print(f"📝 Changes to commit:\n{status}\n")
    
    # Stage all
    run_git("add -A")
    
    # Commit
    if not message:
        from datetime import datetime
        message = f"Databricks update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    
    result = run_git(f'commit -m "{message}"')
    print(result)
    
    # Push
    print("\n⬆️  Pushing...")
    result = run_git(f"push origin {DEFAULT_BRANCH}")
    print(result if result else "✅ Pushed successfully")

def switch_branch(branch: str, create: bool = False):
    """Switch to a branch, optionally creating it."""
    if create:
        result = run_git(f"checkout -b {branch}")
    else:
        result = run_git(f"checkout {branch}")
    print(result)

def reset_hard(confirm: bool = False):
    """⚠️ Reset to remote state (destructive!)."""
    if not confirm:
        print("⚠️  This will discard ALL local changes!")
        print("Run: reset_hard(confirm=True)")
        return
    
    run_git("fetch origin")
    result = run_git(f"reset --hard origin/{DEFAULT_BRANCH}")
    print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Actions
# MAGIC Uncomment and run the action you need:

# COMMAND ----------

# Show current status
status()

# COMMAND ----------

# # Pull latest changes (sync)
# sync()

# COMMAND ----------

# # Commit and push all changes
# push("Updated medallion notebook")

# COMMAND ----------

# # Switch branch
# switch_branch("feature/my-branch", create=True)

# COMMAND ----------

# # ⚠️ Reset to remote (discards local changes!)
# reset_hard(confirm=True)
