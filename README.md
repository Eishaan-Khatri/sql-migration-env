# SQL Schema Migration Agent — OpenEnv Benchmark

An OpenEnv-compatible environment for evaluating AI agents on autonomous SQLite database migration tasks. The agent receives a broken/drifted schema and must write SQL to transform it to a target state without losing data.

## Why This Benchmark?

Database schema migration is a **real-world task** that humans perform daily. Unlike toy benchmarks, it tests:
- **Reasoning under constraints** (SQLite's limited ALTER TABLE support)
- **Data preservation** (agents must never silently drop rows)
- **Multi-step planning** (complex migrations require 5-15 coordinated SQL commands)
- **Edge case handling** (apostrophes, NULL values, empty strings, type coercion)

## Architecture

```
┌─────────────────────────────────┐
│  inference.py (Baseline Agent)  │
│  - LLM API calls (OpenAI fmt)  │
│  - JSON mode + fallback parser │
│  - Task-specific prompts       │
└─────────┬───────────────────────┘
          │ MigrationAction
┌─────────▼───────────────────────┐
│  environment.py (OpenEnv Env)   │
│  - SQLite execution engine      │
│  - SELECT result passthrough    │
│  - SQL timeout (progress hdlr) │
│  - Dangerous SQL blacklist      │
│  - Transaction awareness        │
│  - Trajectory logging           │
└─────────┬───────────────────────┘
          │ score()
┌─────────▼───────────────────────┐
│  grader.py (Golden DB Engine)   │
│  - Dynamic golden reference DB  │
│  - Schema + data + FK scoring   │
│  - Case-insensitive comparison  │
│  - PRAGMA state preservation    │
│  - Anti-exploit checks          │
└─────────────────────────────────┘
```

## Tasks (2 Easy / 3 Medium / 2 Hard)

| # | Task | Difficulty | Steps | Description |
|---|------|-----------|-------|-------------|
| 1 | `column-restructure` | Easy | 10 | Merge first_name + last_name → full_name |
| 2 | `soft-delete-restoration` | Easy | 10 | Restore deleted products from deletion_log |
| 3 | `table-normalization` | Medium | 15 | Normalize purchases → customers + orders + FK |
| 4 | `schema-version-merge` | Medium | 15 | Merge v1/v2 product tables with price coercion |
| 5 | `multi-entity-extraction` | Medium | 15 | 3NF decomposition with invalid data routing |
| 6 | `cascade-migration` | Hard | 20 | 4-table FK cascade, type coercion, orphan audit |
| 7 | `dual-source-consolidation` | Hard | 20 | 6→4 table merge, cross-system email dedup |

### Adversarial Edge Cases
- **O'Brien** (apostrophe in data — tests SQL escaping)
- **$90,000 salary** (TEXT→INTEGER coercion — tests string processing)
- **Empty string emails** (not NULL — tests data validation logic)
- **Leading whitespace** (` alice@company.com` — tests TRIM awareness)
- **ID conflicts** (same ID in two source tables — tests merge logic)
- **Orphaned FKs** (references to deleted entities — tests audit logging)
- **NULL currency** (must default to 'USD' — tests COALESCE)

## Baseline Scores (Qwen/Qwen3-32B)
Tested deterministically via `inference.py` on default seeds:
| Task | Success Score | Step Count |
|------|--------------|------------|
| `column-restructure` | 0.99 | 4-5 |
| `soft-delete-restoration` | 0.99 | 5-7 |
| `table-normalization` | 0.99 | 8-10 |
| `schema-version-merge` | 0.99 | 9-11 |
| `multi-entity-extraction` | 0.99 | 10-12 |
| `cascade-migration` | 0.99 | 13-15 |
| `dual-source-consolidation`| 0.99 | 15-18 |

## Dynamic Golden Database Grading

Unlike benchmarks with hardcoded expected values, our grader is **seed-independent**:

1. At scoring time, a fresh DB is seeded and the correct migration is applied
2. The agent's DB is compared table-by-table against this golden reference
3. If seed data changes, the golden DB auto-updates

**Scoring breakdown (per task):**
- **Schema match (30%)**: Tables exist with correct columns
- **Data match (40%)**: Row content matches golden DB (order-independent)
- **FK & integrity (20%)**: Foreign keys enforced, PRAGMA integrity_check passes
- **Anti-exploit (10%)**: No empty tables, no schema pollution

### Reward Function
The episode step reward is the exact delta of the migration progress score:
```python
step_reward = current_score - previous_score
```
- If an agent reverts progress, `step_reward` is negative.
- Exploit attempts (e.g. `PRAGMA foreign_keys = OFF`) yield immediate `reward = -0.3`.
- Auto-submitted invalid schemas yield negative deltas for missing data.

## Security & Robustness

- **SQL Timeout**: Progress-handler-based execution timeout prevents infinite CTEs
- **Dangerous SQL Blacklist**: ATTACH DATABASE, DETACH, LOAD_EXTENSION blocked
- **Transaction Awareness**: Respects BEGIN/COMMIT/ROLLBACK from agents
- **Case-Insensitive Grading**: Table/column names compared case-insensitively
- **PRAGMA Preservation**: Grader doesn't corrupt agent's FK state
- **Trajectory Logging**: Full SQL history attached to final observation

## Setup

### Requirements
```bash
pip install -r requirements.txt
```

### Environment Variables
```bash
export HF_TOKEN=your_huggingface_token
export API_BASE_URL=https://router.huggingface.co/v1  # or Groq, etc.
export MODEL_NAME=Qwen/Qwen2.5-72B-Instruct
```

### Run Tests
```bash
python test_smoke.py       # Quick validation
python test_all_tasks.py   # All 7 tasks: golden migration + lifecycle
```

### Run Baseline Inference
```bash
python inference.py        # Runs all 7 tasks sequentially
```

### Start Server (HF Spaces)
```bash
uvicorn server.app:app --host 0.0.0.0 --port 7860
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/reset` | POST | Start new migration episode |
| `/step` | POST | Execute a SQL action |
| `/state` | GET | Current environment state |
| `/tasks` | GET | List all 7 tasks with metadata |
| `/grader` | POST | Run grader on specific/all tasks |
| `/health` | GET | Health check |
| `/docs` | GET | Interactive API documentation |

## Action Schema
```json
{
  "sql_command": "ALTER TABLE users ADD COLUMN full_name TEXT",
  "reasoning": "Add the target column before migrating data",
  "submit_final": false
}
```

## Observation Schema
```json
{
  "current_schema_sql": "CREATE TABLE users (...);",
  "target_schema_sql": "CREATE TABLE users (...);",
  "last_execution_result": "Success: 5 rows affected",
  "step_number": 3,
  "migration_progress": 0.75,
  "task_name": "column-restructure",
  "done": false,
  "reward": 0.15
}
```

## Deployment

### Docker
```bash
docker build -t sql-migration-env .
docker run -p 7860:7860 -e HF_TOKEN=your_token sql-migration-env
```

### Hugging Face Spaces
Push to a Space with the included Dockerfile. Set `HF_TOKEN`, `API_BASE_URL`, and `MODEL_NAME` as Space secrets.

## License
MIT
