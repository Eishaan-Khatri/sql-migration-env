---
title: SQL Migration Agent
emoji: 🗄️
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
tags:
  - openenv
---

# SQL Schema Migration Agent


> **An OpenEnv environment for benchmarking autonomous database migration agents.**
> 
> Built for the Meta × Hugging Face OpenEnv Hackathon.

---

## Why This Matters (Real-World Utility)

Database schema migrations are among the most error-prone, high-stakes tasks in software engineering. Every production system faces them as application models evolve, yet they are extremely difficult to automate safely because data must be perfectly preserved. 

This environment trains AI agents to autonomously reconcile schema drift the exact way a real CI/CD pipeline would — given a flawed current state and an ideal target state, the agent must compute and safely execute the transformation sequence using raw SQL.

**Real-world analogues:** `Flyway`, `Liquibase`, Django `makemigrations`, `Terraform` state transitions. This environment models that exact problem, reduced to an agentic RL core.

---

## Evaluation Philosophy & Anti-Exploit Mechanics

Unlike simplistic environments that merely string-match SQL schemas, this environment uses a **deep structural reconciliation grader** built specifically to prevent LLM gamification:

1. **Zero-Sum Exploit Protection:** Naive agents will often execute `DROP TABLE x; CREATE TABLE x (...)` to easily match the target schema, silently destroying all data. Our grader actively runs `SELECT COUNT(*)` and data-integrity hashing. If a table's schema matches but the data is gone, the score is brutally clamped to `0.01`.
2. **Granular Partial Credit:** Multi-step migrations (like Task 3's 4-table cascade) require 15+ steps. Binary pass/fail rewards provide zero learning signal. Our grader assigns fractional weights to individual FK constraints, data type coercions, and orphaned record audit logs, providing continuous RL reward gradients.
3. **Deterministic Adversarial Seeds:** Our injected data isn't generic. It includes edge cases that break naive SQL (e.g. `O'Brien` testing quote-escaping parametrization) and orphaned foreign keys testing `CASCADE` knowledge.

---

## Tasks

| # | Name | Difficulty | Description |
|---|------|-----------|-------------|
| 1 | `column-restructure` | Easy | Merge `first_name` + `last_name` → `full_name` without data loss. Adversarial: apostrophes (`O'Brien`), mid-caps (`McDonald`) |
| 2 | `table-normalization` | Medium | Decompose flat `purchases` into `customers` + `orders` with FK. Adversarial: duplicate emails (`alice@` ×3), commas in item names |
| 3 | `cascade-migration` | Hard | 4-table FK cascade: type coercion (`$90000` TEXT → `90000` INTEGER), orphan audit logging, NULL salary removal, full FK chain enforcement |

---

## Observation Space

| Field | Type | Description |
|-------|------|-------------|
| `current_schema_sql` | `str` | Current database DDL extracted from `sqlite_master` |
| `target_schema_sql` | `str` | Target DDL the agent must reach |
| `last_execution_result` | `str` | Result of last SQL execution, or error message |
| `step_number` | `int` | Current step count (0–20) |
| `migration_progress` | `float` | Current grader score [0.0–1.0] |
| `task_name` | `str` | Name of the active task |
| `done` | `bool` | Whether the episode has terminated |
| `reward` | `float` | Step reward: score delta from previous step (can be negative) |

## Action Space

| Field | Type | Description |
|-------|------|-------------|
| `sql_command` | `str` | Raw SQL statement to execute against the database |
| `reasoning` | `str` | Chain-of-thought explanation (logged for review) |
| `submit_final` | `bool` | Set `true` when migration is believed complete |

---

## Reward Function

- **Step reward**: Delta between current and previous migration score. Strongly negative for destructive actions (e.g., wrong DROP TABLE → -0.4).
- **Episode score**: Clamped to [0.0, 1.0]. Final state wins — regressions hurt.
- **Exploit protection**: If schema matches target, but tables are empty (agent deleted data), score is capped at 0.1.
- **Auto-termination**: Episode ends immediately when score reaches 1.0, preventing post-success regression.

### Task 3 Scoring Breakdown

| Check | Weight | Description |
|-------|--------|-------------|
| `audit_log` exists | 0.10 | Orphan audit table created |
| `audit_log` row count ≥ 3 | 0.10 | All orphaned/invalid records logged |
| Correct audit entries | 0.20 | Right `(source_table, reason)` pairs |
| FK: `departments→companies` | 0.05 | FK chain step 1 |
| FK: `employees→departments` | 0.05 | FK chain step 2 |
| FK: `assets→employees` | 0.05 | FK chain step 3 |
| `companies.name` NOT NULL | 0.05 | Constraint enforcement |
| Employee count = 4 | 0.05 | Hal Patel (NULL salary) removed |
| Salary coercion correct | 0.15 | All `$90000` → `90000` INTEGER |
| No orphaned assets | 0.10 | All `asset.employee_id` valid |
| `PRAGMA integrity_check` | 0.10 | Full DB integrity passes |

---

## Setup & Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run baseline inference
export HF_TOKEN=your_token_here
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
python inference.py

# Run validation tests
python test_smoke.py

# Start environment server locally
uvicorn server.app:app --host 0.0.0.0 --port 7860
```

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/reset` | POST | Reset environment, returns initial observation |
| `/step` | POST | Execute action, returns observation + reward |
| `/state` | GET | Current environment state |
| `/tasks` | GET | List all 3 tasks with descriptions |
| `/grader` | POST | Run grader on all tasks, return scores |
| `/schema` | GET | OpenEnv schema (action/observation types) |
| `/ws` | WS | WebSocket for real-time interaction |

---

## Deployment

```bash
# Docker (local test)
docker build -t sql-migration-env .
docker run -p 7860:7860 \
  -e HF_TOKEN=your_token \
  -e API_BASE_URL=https://router.huggingface.co/v1 \
  -e MODEL_NAME=Qwen/Qwen2.5-72B-Instruct \
  sql-migration-env
```

**Hugging Face Spaces:** Push this repo to HF Spaces with your `HF_TOKEN`, `API_BASE_URL`, and `MODEL_NAME` set as Space secrets. The Dockerfile builds automatically.

---

## Baseline Scores

| Task | Score | Steps | Model |
|------|-------|-------|-------|
| `column-restructure` | 1.00 | 4 | qwen/qwen3-32b |
| `table-normalization` | 1.00 | 5-8 | qwen/qwen3-32b |
| `cascade-migration` | 0.30–0.65 | 15-20 | qwen/qwen3-32b |
| **Average** | **0.77** | — | — |

---

## Pre-Submission Checklist

- [x] `docker build` succeeds
- [x] `curl /health` returns 200
- [x] `curl /tasks` returns 3 tasks
- [x] `curl -X POST /reset` returns valid observation
- [x] `openenv validate` passes
- [x] Baseline script completes all 3 tasks without crashing
- [x] Grader scores in [0.0, 1.0] range
- [x] Exploit protection: empty-table shortcuts penalized
