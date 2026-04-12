#!/usr/bin/env python3
"""
Baseline Inference Script for SQL Migration Environment.

Runs all 7 migration tasks sequentially using an LLM via OpenAI-compatible API.
Outputs structured [START]/[STEP]/[END] format for automated evaluation.

Fixes Applied:
- D1: Task description injected into system prompt
- D2: Hardcoded system prompt traps removed (no more audit_log/INTEGER traps)
- D3: Data discovery rule added (agent runs SELECT before DDL)
- D4: Submit guard added (agent must verify before submitting)
- D5: Context window bloat fixed (schema not repeated every step)
- D6: Parse error counter tracks consecutive errors only
- D7: response_format JSON mode with fallback

Usage:
    python inference.py

Environment Variables:
    API_BASE_URL: LLM inference endpoint (default: HF router)
    MODEL_NAME: Model identifier (default: Qwen/Qwen2.5-72B-Instruct)
    HF_TOKEN or API_KEY: Authentication token
"""

import json
import os
import re
import sys
import time
import traceback

# Server URL for the environment
ENV_BASE_URL = os.getenv("ENV_BASE_URL", "http://localhost:7860")

# LLM Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://router.huggingface.co/v1")
MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen2.5-72B-Instruct")
HF_TOKEN = os.getenv("HF_TOKEN")
API_KEY = os.getenv("OPENAI_API_KEY") or HF_TOKEN or os.getenv("API_KEY")

# --- D2: Cleaned system prompt — no hardcoded table names or type traps ---
SYSTEM_PROMPT_TEMPLATE = """You are an autonomous SQLite database migration engine. You receive the current schema and a target schema. Write SQL to transform the current state to the target state without losing row data.

TASK OBJECTIVE:
{task_description}

CRITICAL SQLite-specific rules (violations cause immediate errors):
1. SQLite does NOT support ALTER TABLE ADD CONSTRAINT, ALTER COLUMN, or ADD PRIMARY KEY.
2. To change column types, add NOT NULL, or add FKs: CREATE new table, INSERT INTO new SELECT FROM old, DROP old, RENAME new.
3. Apostrophes in data (O'Brien, O'Neill) are present — escape with '' in string literals.
4. Execute exactly ONE SQL statement per step.
5. For table normalization: create new tables first, INSERT INTO ... SELECT, then drop old tables.
6. For orphaned FK rows: check the TARGET SCHEMA for the correct anomaly/issues table name (it varies per task). Log invalid records there before dropping.
7. For text currency columns like '$90,000' or '$1,234.56': strip '$' and ',' then cast to the type in the target schema (INTEGER for whole numbers, REAL for decimals).
8. IMPORTANT: Before writing any DDL, execute SELECT * FROM tablename LIMIT 5 for each source table to inspect the actual data format and identify edge cases like empty strings, leading whitespace, NULL values, and special characters.
9. Do NOT set submit_final to true until you have run SELECT COUNT(*) on your target tables and verified the counts and data match what the task requires.
10. When migration is complete and verified, set submit_final to true.

TARGET SCHEMA (achieve this exactly):
{target_ddl}

Respond ONLY with valid JSON — no markdown, no code blocks, no text outside the object:
{{"sql_command": "your SQL here", "reasoning": "why", "submit_final": false}}"""

ALL_TASKS = [
    "column-restructure",
    "soft-delete-restoration",
    "table-normalization",
    "schema-version-merge",
    "multi-entity-extraction",
    "cascade-migration",
    "dual-source-consolidation",
]
MAX_PARSE_ERRORS = 5  # Consecutive parse errors before giving up
AUTO_SUBMIT_THRESHOLD = 0.95


def call_llm(messages: list, timeout: int = 90) -> str:
    """Call the LLM API with JSON mode fallback."""
    from openai import OpenAI

    client = OpenAI(
        base_url=API_BASE_URL,
        api_key=API_KEY,
        timeout=timeout,
    )

    # --- D7: Try JSON mode first, fallback to plain ---
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.0,
            max_tokens=1024,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content.strip()
    except Exception:
        pass

    # Fallback: plain text mode
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.0,
            max_tokens=1024,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        raise TimeoutError(f"LLM API error: {e}")


def parse_action(raw_text: str) -> dict:
    """
    Parse LLM output into an action dict.

    Handles: raw JSON, markdown-wrapped JSON, <think>...</think> blocks,
    escaped quotes in SQL, and truncated output recovery.
    """
    text = raw_text.strip()

    # Strip <think>...</think> blocks (Qwen3, DeepSeek-R1)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()
    text = re.sub(r"<think>.*$", "", text, flags=re.DOTALL).strip()

    # Strip markdown code block fences
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    # Try direct JSON parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object in the text
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    # --- D6: Improved regex that handles escaped quotes ---
    sql_match = re.search(r'"sql_command"\s*:\s*"((?:[^"\\]|\\.)*)"', text)
    if sql_match:
        sql = sql_match.group(1)
        # Unescape JSON string escapes
        sql = sql.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")
        return {
            "sql_command": sql,
            "reasoning": "auto-extracted from malformed response",
            "submit_final": False,
        }

    raise ValueError(f"Could not parse JSON from LLM response: {text[:200]}")


def run_task_local(task_name: str) -> dict:
    """
    Run a single task using a local environment instance (no server needed).
    """
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server.environment import DbMigrationEnvironment
    from models import MigrationAction
    import seeds

    env = DbMigrationEnvironment(task_name=task_name)
    task_config = seeds.TASKS[task_name]
    task_max_steps = task_config.get("max_steps", 20)

    print(f"[START] task={task_name} env=sql-migration-agent model={MODEL_NAME}", flush=True)

    obs = env.reset()

    # --- D1: Inject task description into system prompt ---
    task_system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        task_description=task_config["description"],
        target_ddl=obs.target_schema_sql,
    )
    history = [{"role": "system", "content": task_system_prompt}]

    # Initial observation
    initial_msg = (
        f"CURRENT DATABASE SCHEMA:\n{obs.current_schema_sql}\n\n"
        f"Status: {obs.last_execution_result}\n"
        f"Migration progress: {obs.migration_progress:.2f}\n\n"
        f"Start by inspecting the source data with SELECT queries, then begin the migration."
    )
    history.append({"role": "user", "content": initial_msg})

    rewards_list = []
    consecutive_parse_errors = 0  # D6: Track consecutive only
    final_score = 0.0
    steps_taken = 0
    done = False

    for step in range(task_max_steps):
        if done:
            break

        # --- D5: Context window fix — only keep last 10 messages + system ---
        messages = [history[0]] + history[-10:]

        try:
            raw_response = call_llm(messages)
        except TimeoutError as e:
            error_msg = str(e)[:100]
            print(f"[STEP] step={step+1} action=API_TIMEOUT reward=0.00 done=true error={error_msg}", flush=True)
            done = True
            break

        # Parse the action
        try:
            action_dict = parse_action(raw_response)
            consecutive_parse_errors = 0  # D6: Reset on success
        except ValueError:
            consecutive_parse_errors += 1
            print(f"[STEP] step={step+1} action=PARSE_ERROR reward=0.00 done=false error=parse_error", flush=True)
            if consecutive_parse_errors >= MAX_PARSE_ERRORS:
                print(f"[STEP] step={step+1} action=MAX_PARSE_ERRORS reward=0.00 done=true error=too_many_consecutive_parse_errors", flush=True)
                done = True
                break
            history.append({"role": "assistant", "content": raw_response})
            history.append({
                "role": "user",
                "content": 'ERROR: Your response was not valid JSON. Respond ONLY with: {"sql_command": "...", "reasoning": "...", "submit_final": false}',
            })
            continue

        # Build the MigrationAction
        try:
            action = MigrationAction(
                sql_command=action_dict.get("sql_command", ""),
                reasoning=action_dict.get("reasoning", ""),
                submit_final=action_dict.get("submit_final", False),
            )
        except Exception as e:
            print(f"[STEP] step={step+1} action=INVALID_ACTION reward=0.00 done=false error={str(e)[:50]}", flush=True)
            continue

        # Execute the action
        obs = env.step(action)
        steps_taken = step + 1
        step_reward = obs.reward if obs.reward is not None else 0.0
        rewards_list.append(step_reward)
        final_score = obs.migration_progress
        done = obs.done

        # AUTO-SUBMIT: If we reached near-perfect score, force submit
        if final_score >= AUTO_SUBMIT_THRESHOLD and not done:
            done = True
            submit_action = MigrationAction(
                sql_command="SELECT 1",
                reasoning="Migration complete — auto-submitting",
                submit_final=True,
            )
            obs = env.step(submit_action)
            final_score = obs.migration_progress

        # Log
        sql_abbrev = action.sql_command[:50].replace("\n", " ")
        if len(action.sql_command) > 50:
            sql_abbrev += "..."
        error_str = obs.metadata.get("error", "null") if obs.metadata else "null"
        if error_str != "null":
            error_str = error_str[:80]
        print(
            f"[STEP] step={steps_taken} action={sql_abbrev} "
            f"reward={step_reward:.2f} done={'true' if done else 'false'} "
            f"error={error_str}",
            flush=True,
        )

        # Add to conversation history
        history.append({"role": "assistant", "content": json.dumps(action_dict)})

        # --- D5: Lean feedback — NO schema repetition ---
        feedback_msg = (
            f"EXECUTION RESULT: {obs.last_execution_result}\n"
            f"Progress: {obs.migration_progress:.2f}"
        )
        if done:
            feedback_msg += "\n\nEpisode complete."
        elif obs.migration_progress >= 0.9:
            feedback_msg += (
                "\n\nMigration is nearly complete! Run SELECT COUNT(*) on each table "
                "and compare to your expectations. If everything matches, set submit_final to true."
            )
        else:
            feedback_msg += "\n\nContinue the migration. Write your next SQL command."

        history.append({"role": "user", "content": feedback_msg})

    # Print END
    rewards_str = ",".join(f"{r:.2f}" for r in rewards_list) if rewards_list else "0.00"
    success = "true" if final_score >= 0.8 else "false"
    print(
        f"[END] success={success} steps={steps_taken} "
        f"score={final_score:.2f} rewards={rewards_str}",
        flush=True,
    )

    env.close()

    return {
        "task_name": task_name,
        "score": final_score,
        "steps": steps_taken,
        "rewards": rewards_list,
    }


def main():
    """Run all 7 tasks sequentially."""
    if not API_KEY:
        print("WARNING: No API key found. Set HF_TOKEN or API_KEY.", file=sys.stderr)
        sys.exit(1)

    results = {}
    for task_name in ALL_TASKS:
        try:
            result = run_task_local(task_name)
            results[task_name] = result["score"]
        except Exception as e:
            print(f"[ERROR] task={task_name} error={str(e)[:200]}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            results[task_name] = 0.0

    # Summary
    scores = list(results.values())
    avg = sum(scores) / len(scores) if scores else 0.0
    scores_str = " ".join(f"{t}={s:.2f}" for t, s in results.items())
    print(
        f"[SUMMARY] {scores_str} avg={avg:.2f}",
        flush=True,
    )


if __name__ == "__main__":
    main()
