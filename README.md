---
title: SQL Migration Agent Benchmark
emoji: 🗄️
colorFrom: blue
colorTo: purple
sdk: docker
app_file: server/app.py
pinned: false
---

# SQL Migration Agent Benchmark (OpenEnv)
![Badge](https://hitscounter.dev/api/hit?url=https%3A%2F%2Fgithub.com%2FEishaan-Khatri%2Fsql-migration-env&label=Visits&icon=github&color=%23198754&message=&style=flat&tz=UTC)

> **A Production-Grade Evaluation Suite for Database Engineering AI Agents.**
> *Designed and built by Eishaan Khatri for the Meta PyTorch OpenEnv Hackathon 2026.*

[![OpenEnv Compliant](https://img.shields.io/badge/OpenEnv-Compliant-success)](https://github.com/openenv/core)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Hugging Face Space](https://img.shields.io/badge/HF%20Space-Deployed-orange)](https://huggingface.co/spaces/Eishaan/sql-migration-env)

An OpenEnv-compatible environment for evaluating frontier AI agents on autonomous, multi-step SQLite database migration tasks. The agent is deployed into a sandbox containing a drifted or broken schema, and it must write precise SQL DDL and DML to transform it to a target state **without losing any operational data**.

---

## 🌟 Key Engineering Features

As an advanced benchmarking suite, this environment goes far beyond simple token matching. It acts as a full-fledged database simulator with built-in safety, parsing, and scoring mechanisms.

*   **Dynamic Golden Database Grader**: Recomputes the "perfect" solution strictly from the programmatic state, matching agent tables against golden tables using a $10^{-7}$ float-tolerant epsilon comparator.
*   **Live ERD Generation (Mermaid.js)**: Dynamically parses `sqlite_master` and `pragma_foreign_key_list` to inject real-time Entity-Relationship Diagrams directly into the agent's observation window, significantly enhancing spatial reasoning.
*   **Efficiency Metrics (Ops & Latency)**: Evaluates not just *correctness*, but *computational cost*. Integrates with the SQLite VM instruction counter (`sqlite3_progress_handler`) and execution timers to grade agents on query efficiency.
*   **Adversarial Sandboxing**: Implements strict regex-based AST bounds checking, preventing PRAGMA exploits (`writable_schema=ON`, disabling foreign keys) and filtering dangerous file-system SQL (`ATTACH`, `DETACH`).

---

## 🏗️ Architecture Overview

The suite combines formal sequence modeling with a modular local engine.

```mermaid
sequenceDiagram
    participant Agent
    participant Env as MigrationEnv Server
    participant DB as SQLite (:memory:)
    participant Grader as Dynamic Golden Grader

    Agent->>Env: POST /reset (task_name)
    Env->>DB: Seed Source Data
    Env->>Grader: Build Golden Reference
    Grader-->>Env: Initial Score
    Env-->>Agent: Observation (DDL, Schema Diff, ERD)

    loop Migration Steps
        Agent->>Env: POST /step (SQL, Reasoning)
        Env->>DB: Execute SQL (w/ Timeout & VM Ops Metrics)
        Env->>Grader: Compute Delta Reward & State
        Grader-->>Env: current_score, reward
        Env-->>Agent: New Observation + Dynamic ERD 
    end

    Agent->>Env: submit_final = True
    Env->>Grader: Final Integrity & FK Strict Check
    Env-->>Agent: Final Episode Summary (Trajectory)
```

---

## 🎯 Benchmark Task Registry

The environment is strictly calibrated across a progressive curriculum of 8 tasks, specifically engineered to exploit common weaknesses in LLM logic.

| # | Task | Difficulty | Challenge Overview |
|---|------|-----------|-----------|
| 1 | `column-restructure` | 🟢 Easy | Merge `first_name` + `last_name` → `full_name` while escaping tricky text (apostrophes). |
| 2 | `soft-delete-restoration` | 🟢 Easy | Recover deleted products from an audit log back into the active ledger. |
| 3 | `table-normalization` | 🟡 Medium | Decompose a massive god-table (`purchases`) into strict 3NF (`customers`, `orders`) + FK mappings. |
| 4 | `schema-version-merge` | 🟡 Medium | Reconcile overlapping v1/v2 schema tables with priority collision rules and type-casting text-currencies. |
| 5 | `multi-entity-extraction` | 🟡 Medium | Extract hierarchical data and route missing/null required data to an anomaly table. |
| 6 | `cascade-migration` | 🔴 Hard | Safely execute a 4-table cascade rewrite, coercing types and strictly dropping orphaned references without violating constraints. |
| 7 | `dual-source-consolidation`| 🔴 Hard | Merge 6 tables from incompatible systems (Legacy CRM + SaaS API) including global cross-system id mapping. |
| **8** | **`data-poisoning-quarantine`** | 🔥 **Extreme**| **The Ultimate Stress Test**. Migrate a staging ledger poisoned with string symbols, trailing spaces, and `N/A` invalid strings. *Agent must dynamically cleanse floating strings (regex/replace) and partition completely un-coerceable corrupt rows into an isolation quarantine.* |


### 🛠️ Adversarial Edge Cases (The "Stress Tests")
- **O'Brien**: Apostrophe in data — tests SQL escaping and string literal handling.
- **$90,000 salary**: TEXT→INTEGER coercion — tests complex string parsing and casting.
- **Empty string emails**: NOT NULL vs Empty — tests data quality validation logic.
- **Leading whitespace**: ` alice@company.com` — tests TRIM awareness.

---

## ⚖️ Grader Audit (1 October 2026)

I probed the grader with scripted policies that do **not** solve the migration, to see how much score it still awards. Reproduce with `python audit/exploit_probe.py`. The full write-up is in [`audit/GRADER_AUDIT.md`](audit/GRADER_AUDIT.md).

| Task | Do nothing | Schema only | One row lost | Wrong values | Reference |
|------|-----------|-------------|--------------|--------------|-----------|
| `column-restructure` | 0.68 | 0.50 | 0.92 | 0.76 | 0.99 |
| `soft-delete-restoration` | 0.64 | 0.50 | 0.95 | 0.76 | 0.99 |
| `table-normalization` | 0.29 | 0.50 | 0.88 | 0.76 | 0.99 |
| `schema-version-merge` | 0.28 | 0.50 | 0.96 | 0.76 | 0.99 |
| `multi-entity-extraction` | 0.29 | 0.50 | 0.90 | 0.76 | 0.99 |
| `cascade-migration` | 0.61 | 0.50 | 0.86 | 0.76 | 0.99 |
| `dual-source-consolidation`| 0.25 | 0.50 | 0.89 | 0.76 | 0.99 |
| `data-poisoning-quarantine`| 0.53 | 0.50 | 0.89 | 0.76 | 0.99 |
| **Mean** | **0.45** | **0.50** | **0.90** | **0.76** | **0.99** |

**What this shows.**
- The floor is high. Doing nothing scores 0.45 on average, so every score must be read against that floor and not against zero.
- A database with the right schema, keys and row counts and every non-key value wrong still scores 0.76, because the row count is paid separately from the row content.
- Losing a row costs only a few points. In a real migration any lost row is a failure.

**Known limitation and planned fix.** The grader gives partial credit for wrong data. The fix is a strict end-state check, where a run succeeds only if the final database equals the reference on rows and constraints. It is not merged yet.

**Model baselines.** An earlier version of this README listed scores for two open models. They were measured on 12 April 2026, before the last two grader changes, and one row can no longer be reproduced on the current grader. They have been removed until they are re-run.

---

## 🛡️ Security & Reward Function

### The Reward Formula ($R_t$)
Rewards are calculated as progress deltas: $R_t = P_t - P_{t-1}$.
Progress $P_t$ is a weighted sum (0.01 to 0.99) preventing generic "done" token hacking:
- **Schema Signature Match (30%)**: Tables exist with precise `(name, type)` configurations.
- **Data Integrity Margin (40%)**: Row content maps identically to the Golden Generator (order-independent).
- **Relational Stability (20%)**: Foreign keys strictly enforced and SQLite `integrity_check` passes.
- **Anti-Exploit Metric (10%)**: Heavily penalizes schema pollution or lazy "DELETE FROM table" shortcuts.

---

## 🚀 Setup & Usage

### Local Deployment
```bash
pip install -r requirements.txt
python -m server.app  # Starts OpenEnv HTTP & UI server on port 7860
```

### Environment Variables for Baselines
```bash
export HF_TOKEN=your_token
export API_BASE_URL=https://router.huggingface.co/v1
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
```

### Core API Endpoints
- `POST /reset`: Initialize a seeded migration sandbox environment.
- `POST /step`: Submit executed SQL and reasoning vectors.
- `GET /tasks`: Query the active scenario registry.
- `POST /grader`: Instantiate the isolated Golden DB grader comparator.

---

## 📄 License & Author
Released under the MIT License. 
Designed from the ground up as a robust RL testing harness for the **Meta PyTorch OpenEnv Hackathon 2026**.
