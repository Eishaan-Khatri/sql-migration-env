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
import server.environment # Ensuring server is treated as a package
try:
    from openenv.core.env_server.http_server import create_app
    from models import MigrationAction, MigrationObservation
    from server.environment import DbMigrationEnvironment
except ImportError:
    from openenv.core.env_server.http_server import create_app
    from ..models import MigrationAction, MigrationObservation
    from .environment import DbMigrationEnvironment


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
    """Root endpoint — returns a premium status page for the HF Space UI."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SQL Migration Agent | OpenEnv Benchmark</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <style>
        :root {
            --bg: #03060b;
            --card-bg: rgba(13, 17, 23, 0.8);
            --primary: #58a6ff;
            --accent: #d2a8ff;
            --success: #3fb950;
            --warning: #d29922;
            --danger: #f85149;
            --text-main: #e6edf3;
            --text-dim: #8b949e;
            --border: #30363d;
        }

        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { 
            font-family: 'Outfit', sans-serif; 
            background: var(--bg); 
            color: var(--text-main); 
            line-height: 1.6;
            overflow-x: hidden;
        }

        .background-blob {
            position: fixed;
            width: 600px;
            height: 600px;
            background: radial-gradient(circle, rgba(88, 166, 255, 0.1) 0%, rgba(210, 168, 255, 0.05) 50%, transparent 100%);
            border-radius: 50%;
            z-index: -1;
            filter: blur(80px);
            animation: move 20s infinite alternate;
        }

        @keyframes move {
            from { transform: translate(-10%, -10%); }
            to { transform: translate(20%, 30%); }
        }

        .container { max-width: 1100px; margin: 0 auto; padding: 60px 20px; }
        
        header { 
            margin-bottom: 60px; 
            text-align: center;
            border-bottom: 1px solid var(--border);
            padding-bottom: 40px;
        }

        h1 { font-size: 3rem; font-weight: 700; margin-bottom: 10px; color: var(--primary); letter-spacing: -1px; }
        .badge {
            display: inline-block;
            padding: 4px 12px;
            background: rgba(63, 185, 80, 0.15);
            color: var(--success);
            border: 1px solid rgba(63, 185, 80, 0.3);
            border-radius: 20px;
            font-size: 0.9rem;
            font-weight: 600;
            margin-top: 10px;
        }

        .dashboard-grid {
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 30px;
        }

        .card {
            background: var(--card-bg);
            border: 1px solid var(--border);
            border-radius: 16px;
            padding: 30px;
            backdrop-filter: blur(10px);
            margin-bottom: 30px;
        }

        h2 { font-size: 1.5rem; margin-bottom: 25px; color: var(--accent); }

        .endpoint-list { list-style: none; }
        .endpoint-item {
            display: flex;
            align-items: center;
            padding: 12px;
            border-bottom: 1px solid var(--border);
            font-family: 'JetBrains Mono', monospace;
        }
        .method { font-weight: 700; width: 60px; font-size: 0.85rem; }
        .method.post { color: var(--success); }
        .method.get { color: var(--primary); }
        .path { color: var(--text-main); margin-left: 10px; }
        .desc { color: var(--text-dim); margin-left: auto; font-family: 'Outfit'; font-size: 0.9rem; }

        .task-card {
            padding: 15px;
            border: 1px solid var(--border);
            border-radius: 10px;
            margin-bottom: 12px;
            transition: all 0.3s ease;
        }
        .task-card:hover { border-color: var(--primary); background: rgba(88, 166, 255, 0.05); }
        .task-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 5px; }
        .difficulty { font-size: 0.75rem; text-transform: uppercase; font-weight: 700; }
        .difficulty.easy { color: var(--success); }
        .difficulty.medium { color: var(--warning); }
        .difficulty.hard { color: var(--danger); }
        .task-name { font-weight: 600; font-size: 1.1rem; }

        .footer {
            margin-top: 60px;
            text-align: center;
            color: var(--text-dim);
            font-size: 0.9rem;
        }
        a { color: var(--primary); text-decoration: none; font-weight: 600; }
        a:hover { text-decoration: underline; }

        @media (max-width: 800px) {
            .dashboard-grid { grid-template-columns: 1fr; }
            h1 { font-size: 2.2rem; }
        }
    </style>
</head>
<body>
    <div class="background-blob"></div>
    <div class="container">
        <header>
            <h1>SQL Migration Agent</h1>
            <p style="color: var(--text-dim); font-size: 1.2rem;">Production-Grade OpenEnv Benchmark Suite</p>
            <span class="badge">● Online & Compliant</span>
        </header>

        <div class="dashboard-grid">
            <div class="left-col">
                <div class="card">
                    <h2>Core Endpoints</h2>
                    <div class="endpoint-list">
                        <div class="endpoint-item"><span class="method post">POST</span> <span class="path">/reset</span> <span class="desc">Initialize task state</span></div>
                        <div class="endpoint-item"><span class="method post">POST</span> <span class="path">/step</span>  <span class="desc">Execute SQL agent action</span></div>
                        <div class="endpoint-item"><span class="method get">GET</span>  <span class="path">/state</span> <span class="desc">Current episode status</span></div>
                        <div class="endpoint-item"><span class="method get">GET</span>  <span class="path">/tasks</span> <span class="desc">List benchmark tasks</span></div>
                        <div class="endpoint-item"><span class="method post">POST</span> <span class="path">/grader</span><span class="desc">Run golden-DB comparison</span></div>
                    </div>
                </div>

                <div class="card">
                    <h2>Benchmark Features</h2>
                    <p style="color: var(--text-dim); margin-bottom: 20px;">
                        This environment provides high-fidelity SQLite migration tasks designed to pressure-test schema decomposition, 
                        type coercion, and data integrity handling in LLMs.
                    </p>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                        <div>
                            <strong style="color: var(--primary);">✔ Dynamic Grader</strong>
                            <p style="font-size: 0.85rem; color: var(--text-dim);">Seed-independent golden-DB logic.</p>
                        </div>
                        <div>
                            <strong style="color: var(--primary);">✔ Efficiency Metrics</strong>
                            <p style="font-size: 0.85rem; color: var(--text-dim);">Tracks Query Ops & Latency.</p>
                        </div>
                        <div>
                            <strong style="color: var(--primary);">✔ ERD Viz</strong>
                            <p style="font-size: 0.85rem; color: var(--text-dim);">Real-time Mermaid diagrams.</p>
                        </div>
                        <div>
                            <strong style="color: var(--primary);">✔ Anti-Exploit</strong>
                            <p style="font-size: 0.85rem; color: var(--text-dim);">PRAGMA & dialect blacklisting.</p>
                        </div>
                    </div>
                </div>
            </div>

            <div class="right-col">
                <div class="card">
                    <h2>Assessment Tasks</h2>
                    <div class="task-card">
                        <div class="task-header"><span class="difficulty easy">Easy</span> <span class="task-name">Column Merge</span></div>
                        <p style="font-size: 0.85rem; color: var(--text-dim);">Merge name fields with apostrophe preservation.</p>
                    </div>
                    <div class="task-card">
                        <div class="task-header"><span class="difficulty medium">Medium</span> <span class="task-name">Normalization</span></div>
                        <p style="font-size: 0.85rem; color: var(--text-dim);">Decompose god-table into 3NF schema.</p>
                    </div>
                    <div class="task-card">
                        <div class="task-header"><span class="difficulty hard">Hard</span> <span class="task-name">Cascade Sync</span></div>
                        <p style="font-size: 0.85rem; color: var(--text-dim);">Multi-table FK cascade with audit logging.</p>
                    </div>
                    <div class="task-card">
                        <div class="task-header"><span class="difficulty" style="color: var(--accent);">Extreme</span> <span class="task-name">Data Poisoning</span></div>
                        <p style="font-size: 0.85rem; color: var(--text-dim);">Quarantine poisoned staging data with strict schema integrity.</p>
                    </div>
                    <div style="text-align: center; margin-top: 20px;">
                        <a href="/tasks">View all 8 tasks →</a>
                    </div>
                </div>

                <div class="card">
                    <h2>Developer Info</h2>
                    <p style="font-size: 0.9rem;">
                        <strong>Engine:</strong> OpenEnv v1.0<br>
                        <strong>Dialect:</strong> SQLite 3.x<br>
                        <strong>Port:</strong> 7860
                    </p>
                    <hr style="border: none; border-top: 1px solid var(--border); margin: 15px 0;">
                    <a href="/docs" target="_blank">📚 Swagger API Docs</a>
                </div>
            </div>
        </div>

        <div class="footer">
            Built for the OpenEnv Hackathon &copy; 2026. <br>
            <a href="https://github.com/Eishaan-Khatri/sql-migration-env" target="_blank">Source Code on GitHub</a>
        </div>
    </div>
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
        "example_action": {
            "sql_command": "CREATE TABLE ...",
            "reasoning": "Creating the new destination table before copying data.",
            "submit_final": False
        }
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
        "grader_version": "1.0",
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
