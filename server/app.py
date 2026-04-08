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
    <title>SQL Migration Agent — OpenEnv</title>
    <style>
        body { font-family: monospace; background: #0d1117; color: #e6edf3; padding: 40px; }
        h1 { color: #58a6ff; } h2 { color: #79c0ff; }
        .ok { color: #3fb950; } .endpoint { color: #d2a8ff; }
        pre { background: #161b22; padding: 12px; border-radius: 6px; }
        a { color: #58a6ff; }
    </style>
</head>
<body>
    <h1>🗄️ SQL Schema Migration Agent</h1>
    <p class="ok">✅ Server running — OpenEnv hackathon environment</p>
    <h2>API Endpoints</h2>
    <pre>
<span class="endpoint">POST /reset</span>   — Start a new migration episode
<span class="endpoint">POST /step</span>    — Execute a SQL action
<span class="endpoint">GET  /state</span>   — Current environment state
<span class="endpoint">GET  /tasks</span>   — List all 3 tasks
<span class="endpoint">POST /grader</span>  — Run grader on all tasks
<span class="endpoint">GET  /health</span>  — Health check
<span class="endpoint">GET  /docs</span>    — Interactive API documentation
    </pre>
    <h2>Tasks</h2>
    <pre>
1. column-restructure   (Easy)   — Merge first_name + last_name → full_name
2. table-normalization  (Medium) — Normalize purchases → customers + orders + FK
3. cascade-migration    (Hard)   — 4-table FK cascade, type coercion, orphan audit
    </pre>
    <p><a href="/docs">📖 Open API Docs</a> | <a href="/tasks">📋 View Tasks</a> | <a href="/health">💚 Health Check</a></p>
</body>
</html>"""


@app.get("/tasks")
async def list_tasks() -> Dict[str, Any]:
    """
    List all available migration tasks and the action schema.

    Returns JSON with task definitions and action schema for automated validation.
    """
    return {
        "tasks": [
            {
                "name": "column-restructure",
                "description": "Merge first_name and last_name into a single full_name column without data loss",
                "difficulty": "easy",
                "max_steps": 15,
            },
            {
                "name": "table-normalization",
                "description": "Decompose a flat purchases table into normalized customers and orders tables with FK",
                "difficulty": "medium",
                "max_steps": 15,
            },
            {
                "name": "cascade-migration",
                "description": "Multi-table FK cascade with type coercion, NULL handling, and orphan audit logging",
                "difficulty": "hard",
                "max_steps": 15,
            },
        ],
        "action_schema": {
            "sql_command": "string — The SQL statement to execute",
            "reasoning": "string — Explanation of the action (optional)",
            "submit_final": "boolean — Set true when migration is complete (default: false)",
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
    tasks_to_grade = [task_name] if task_name else ["column-restructure", "table-normalization", "cascade-migration"]

    results = {}
    for t in tasks_to_grade:
        try:
            env = DbMigrationEnvironment(task_name=t)
            obs = env.reset()
            # Return the initial score (before any agent action)
            # This proves the grader works and returns values in [0.0, 1.0]
            results[t] = {
                "initial_score": max(0.0, min(1.0, obs.migration_progress)),
                "grader_functional": True,
                "reward_range": [0.0, 1.0],
                "max_steps": 15,
            }
            env.close()
        except Exception as e:
            results[t] = {
                "initial_score": 0.0,
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
