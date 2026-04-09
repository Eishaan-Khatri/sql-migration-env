"""
FastAPI application for the SQL Migration Environment.

Uses create_app() from the OpenEnv framework to auto-generate all
standard endpoints (/reset, /step, /state, /ws, /health, /schema).
Additionally defines three hackathon-required custom endpoints:
/tasks, /grader, /baseline.

Usage:
    uvicorn server.app:app --host 0.0.0.0 --port 7860
"""

import os
import subprocess
import sys
from typing import Any, Dict, List, Optional

from fastapi import Body

# Support both in-repo and standalone imports
try:
    from openenv.core.env_server.http_server import create_app
    from ..models import MigrationAction, MigrationObservation
    from .environment import DbMigrationEnvironment
except ImportError:
    from openenv.core.env_server.http_server import create_app
    from models import MigrationAction, MigrationObservation
    from server.environment import DbMigrationEnvironment


# Get task name from environment variable (default to column-restructure)
DEFAULT_TASK = os.getenv("MIGRATION_TASK", "column-restructure")


# Factory function for per-session environment creation
def create_migration_environment():
    """Factory function that creates DbMigrationEnvironment instances."""
    return DbMigrationEnvironment(task_name=DEFAULT_TASK)


# Create the FastAPI app using OpenEnv's create_app factory
# This auto-generates: /reset, /step, /state, /ws, /health, /schema, /docs
app = create_app(
    create_migration_environment,
    MigrationAction,
    MigrationObservation,
    env_name="sql_migration_env",
)


# =============================================================================
# Custom Hackathon Endpoints
# =============================================================================

from fastapi.responses import HTMLResponse


@app.get("/", response_class=HTMLResponse)
async def root():
    """Root endpoint — returns a status page for the HF Space UI."""
    return """<!DOCTYPE html>
<html>
<head>
    <title>SQL Migration Agent -- OpenEnv</title>
    <style>
        body { font-family: monospace; background: #0d1117; color: #e6edf3; padding: 40px; }
        h1 { color: #58a6ff; } h2 { color: #79c0ff; }
        .ok { color: #3fb950; } .endpoint { color: #d2a8ff; }
        pre { background: #161b22; padding: 12px; border-radius: 6px; }
        a { color: #58a6ff; }
        .easy { color: #3fb950; } .medium { color: #d29922; } .hard { color: #f85149; }
    </style>
</head>
<body>
    <h1>SQL Schema Migration Agent</h1>
    <p class="ok">Server running -- OpenEnv hackathon environment (7 tasks)</p>
    <h2>API Endpoints</h2>
    <pre>
<span class="endpoint">POST /reset</span>   -- Start a new migration episode
<span class="endpoint">POST /step</span>    -- Execute a SQL action
<span class="endpoint">GET  /state</span>   -- Current environment state
<span class="endpoint">GET  /tasks</span>   -- List all 7 tasks
<span class="endpoint">POST /grader</span>  -- Run grader on all tasks
<span class="endpoint">GET  /health</span>  -- Health check
<span class="endpoint">GET  /docs</span>    -- Interactive API documentation
    </pre>
    <h2>Tasks (2 Easy / 3 Medium / 2 Hard)</h2>
    <pre>
<span class="easy">1. column-restructure      (Easy)   -- Merge first_name + last_name -> full_name</span>
<span class="easy">2. soft-delete-restoration  (Easy)   -- Restore deleted products from deletion_log</span>
<span class="medium">3. table-normalization      (Medium) -- Normalize purchases -> customers + orders + FK</span>
<span class="medium">4. schema-version-merge     (Medium) -- Merge v1/v2 product tables with coercion</span>
<span class="medium">5. multi-entity-extraction  (Medium) -- 3NF decomposition with invalid data routing</span>
<span class="hard">6. cascade-migration        (Hard)   -- 4-table FK cascade, type coercion, orphan audit</span>
<span class="hard">7. dual-source-consolidation(Hard)   -- 6->4 table merge, cross-system email dedup</span>
    </pre>
    <p><a href="/docs">Open API Docs</a> | <a href="/tasks">View Tasks</a> | <a href="/health">Health Check</a></p>
</body>
</html>"""


@app.get("/tasks")
async def list_tasks() -> Dict[str, Any]:
    """
    List all available migration tasks and the action schema.

    Returns JSON with task definitions and action schema for automated validation.
    """
    # Import seeds to dynamically build task list
    try:
        from .. import seeds as _seeds
    except ImportError:
        import seeds as _seeds

    task_list = []
    for name, cfg in _seeds.TASKS.items():
        task_list.append({
            "name": name,
            "description": cfg["description"],
            "difficulty": cfg["difficulty"],
            "max_steps": cfg.get("max_steps", 20),
        })

    return {
        "tasks": task_list,
        "action_schema": {
            "sql_command": "string -- The SQL statement to execute",
            "reasoning": "string -- Explanation of the action (optional)",
            "submit_final": "boolean -- Set true when migration is complete (default: false)",
        },
    }


@app.post("/grader")
async def grade_task(
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """
    Grade a task or all tasks.

    Accepts: {"task_name": "column-restructure"} or {} for all tasks.
    Returns per-task grader scores after running the environment's internal scorer.
    """
    task_name = body.get("task_name", None)

    try:
        from .. import seeds as _seeds
    except ImportError:
        import seeds as _seeds

    tasks_to_grade = [task_name] if task_name else list(_seeds.TASKS.keys())

    results = {}
    for t in tasks_to_grade:
        try:
            env = DbMigrationEnvironment(task_name=t)
            obs = env.reset()
            results[t] = {
                "initial_score": obs.migration_progress,
                "grader_functional": True,
                "reward_range": [0.01, 0.99],
                "max_steps": _seeds.TASKS[t].get("max_steps", 20),
            }
            env.close()
        except Exception as e:
            results[t] = {
                "initial_score": 0.01,
                "grader_functional": False,
                "error": str(e),
            }

    return {
        "tasks": results,
        "status": "graded",
    }


@app.post("/baseline")
async def run_baseline(
    body: Dict[str, Any] = Body(default={}),
) -> Dict[str, Any]:
    """
    Run the baseline inference script and return its output.

    Triggers inference.py as a subprocess with a 1200-second timeout.
    Returns captured stdout for evaluation.
    """
    try:
        result = subprocess.run(
            [sys.executable, "inference.py"],
            capture_output=True,
            text=True,
            timeout=1200,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            env=os.environ.copy(),  # Explicitly inherit env vars (HF Space secrets)
        )
        return {
            "status": "completed",
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode,
        }
    except subprocess.TimeoutExpired as e:
        return {
            "status": "timeout",
            "stdout": e.stdout.decode() if e.stdout else "",
            "stderr": e.stderr.decode() if e.stderr else "",
            "returncode": -1,
        }
    except Exception as e:
        return {
            "status": "error",
            "stdout": "",
            "stderr": str(e),
            "returncode": -1,
        }


def main():
    """Entry point for direct execution."""
    import uvicorn

    port = int(os.getenv("PORT", "7860"))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
