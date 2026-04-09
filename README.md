---
title: SQL Migration Agent
emoji: "\U0001F5C4\uFE0F"
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
> Built for the Meta x Hugging Face OpenEnv Hackathon.

---

## Why This Matters (Real-World Utility)

Database schema migrations are among the most error-prone, high-stakes tasks in software engineering. Every production system faces them as application models evolve, yet they are extremely difficult to automate safely because data must be perfectly preserved. 

This environment trains AI agents to autonomously reconcile schema drift the exact way a real CI/CD pipeline would -- given a flawed current state and an ideal target state, the agent must compute and safely execute the transformation sequence using raw SQL.

**Real-world analogues:** `Flyway`, `Liquibase`, Django `makemigrations`, `Terraform` state transitions. This environment models that exact problem, reduced to an agentic RL core.

---

## Evaluation Philosophy & Anti-Exploit Mechanics

Unlike simplistic environments that merely string-match SQL schemas, this environment uses a **deep structural reconciliation grader** built specifically to prevent LLM gamification:

1. **Zero-Sum Exploit Protection:** Naive agents will often execute `DROP TABLE x; CREATE TABLE x (...)` to easily match the target schema, silently destroying all data. Our grader actively runs `SELECT COUNT(*)`, `SUM(id)`, and data-integrity fingerprinting. If a table's schema matches but the data is gone, the score is brutally clamped to `0.01`.
2. **PRAGMA Bypass Prevention:** The grader re-asserts `PRAGMA foreign_keys = ON` before every scoring pass, preventing agents from disabling FK constraints to cheat.
3. **Granular Partial Credit:** Multi-step migrations (like Task 7's 6-to-4 table consolidation) require 18+ steps. Binary pass/fail rewards provide zero learning signal. Our grader assigns fractional weights to individual FK constraints, data type coercions, and orphaned record audit logs, providing continuous RL reward gradients.
4. **Deterministic Adversarial Seeds:** Our injected data includes edge cases that break naive SQL: `O'Brien` (apostrophes), `$1,234.56` (comma+dollar coercion), orphaned foreign keys, NULL emails, and leading whitespace in emails.

---

## Tasks (2 Easy / 3 Medium / 2 Hard)

| # | Name | Difficulty | Steps | Description |
|---|------|-----------|-------|-------------|
| 1 | `column-restructure` | Easy | 10 | Merge `first_name` + `last_name` into `full_name` without data loss. Adversarial: apostrophes (`O'Brien`), mid-caps (`McDonald`) |
| 2 | `soft-delete-restoration` | Easy | 10 | Restore deleted products from `deletion_log`, add `is_deleted`/`deleted_at` columns. Adversarial: `stock=0` must not be confused with `is_deleted=1` |
| 3 | `table-normalization` | Medium | 15 | Decompose flat `purchases` into `customers` + `orders` with FK. Adversarial: duplicate emails (x3), commas in item names |
| 4 | `schema-version-merge` | Medium | 15 | Merge overlapping `products_v1` (TEXT prices) and `products_v2` (REAL prices) with conflict resolution and `source` tracking. Adversarial: `$XX.XX` coercion, NULL category, high ID=101 |
| 5 | `multi-entity-extraction` | Medium | 15 | Decompose `sales_records` god-table into 3NF (5 tables) with 3 FKs and invalid data routing. Adversarial: leading whitespace email, empty email, comma in SKU |
| 6 | `cascade-migration` | Hard | 20 | 4-table FK cascade: type coercion (`$90000` TEXT to `90000` INTEGER), orphan audit logging, NULL salary removal, full FK chain enforcement |
| 7 | `dual-source-consolidation` | Hard | 20 | Merge 6 tables from two incompatible systems (Legacy CRM + Modern SaaS) into 4 unified tables with cross-system email dedup, currency coercion, orphan detection |

---

## Observation Space

| Field | Type | Description |
|-------|------|-------------|
| `current_schema_sql` | `str` | Current database DDL extracted from `sqlite_master` |
| `target_schema_sql` | `str` | Target DDL the agent must reach |
| `last_execution_result` | `str` | Result of last SQL execution, or error message |
| `step_number` | `int` | Current step count |
| `migration_progress` | `float` | Current grader score [0.01-0.99] |
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

- **Step reward**: Delta between current and previous migration score. Strongly negative for destructive actions (e.g., wrong DROP TABLE leads to -0.4).
- **Episode score**: Clamped to (0.01, 0.99). Final state wins -- regressions hurt.
- **Exploit protection**: If schema matches target but tables are empty (agent deleted data), score is capped at 0.01.
- **PRAGMA protection**: `PRAGMA foreign_keys = ON` is re-asserted before every grading pass.
- **Auto-termination**: Episode ends immediately when score reaches 0.99, preventing post-success regression.

---

## Setup & Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run baseline inference (requires HF_TOKEN)
export HF_TOKEN=your_token_here
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
python inference.py

# Run validation tests
python test_smoke.py
python test_all_tasks.py

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
| `/tasks` | GET | List all 7 tasks with descriptions |
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
| `column-restructure` | 0.99 | 4 | Qwen/Qwen2.5-72B-Instruct |
| `soft-delete-restoration` | 0.99 | 5-7 | Qwen/Qwen2.5-72B-Instruct |
| `table-normalization` | 0.99 | 5-8 | Qwen/Qwen2.5-72B-Instruct |
| `schema-version-merge` | 0.60-0.85 | 8-12 | Qwen/Qwen2.5-72B-Instruct |
| `multi-entity-extraction` | 0.40-0.70 | 12-15 | Qwen/Qwen2.5-72B-Instruct |
| `cascade-migration` | 0.30-0.65 | 15-20 | Qwen/Qwen2.5-72B-Instruct |
| `dual-source-consolidation` | 0.20-0.50 | 18-20 | Qwen/Qwen2.5-72B-Instruct |

---

## Pre-Submission Checklist

- [x] `docker build` succeeds
- [x] `curl /health` returns 200
- [x] `curl /tasks` returns 7 tasks
- [x] `curl -X POST /reset` returns valid observation
- [x] `openenv validate` passes
- [x] Baseline script completes all 7 tasks without crashing
- [x] Grader scores in (0.01, 0.99) range
- [x] Exploit protection: empty-table shortcuts penalized
- [x] PRAGMA bypass protection enforced
