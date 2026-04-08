#!/usr/bin/env python3
"""
Baseline Inference Script for SQL Migration Environment.

Runs all 3 migration tasks sequentially using an LLM via OpenAI-compatible API.
Outputs structured [START]/[STEP]/[END] format for automated evaluation.

Usage:
    python inference.py

Environment Variables:
    API_BASE_URL: LLM inference endpoint (default: HF router)
    MODEL_NAME: Model identifier (default: Qwen/Qwen2.5-72B-Instruct)
    HF_TOKEN or API_KEY: Authentication token
"""

import json
import os
import sys
import time
import traceback

# Server URL for the environment
ENV_BASE_URL = os.getenv("ENV_BASE_URL", "http://localhost:7860")

# LLM Configuration
API_BASE_URL = os.getenv("API_BASE_URL") or "https://router.huggingface.co/v1"
MODEL_NAME = os.getenv("MODEL_NAME") or "Qwen/Qwen2.5-72B-Instruct"
# API key: spec requires OPENAI_API_KEY as primary variable name
API_KEY = os.getenv("OPENAI_API_KEY") or os.getenv("HF_TOKEN") or os.getenv("API_KEY")

SYSTEM_PROMPT = """You are an autonomous SQLite database migration engine. You receive the current schema and a target schema. Write SQL to transform the current state to the target state without losing row data.

CRITICAL — SQLite-specific rules (violations cause immediate errors):
1. SQLite does NOT support ALTER TABLE ADD CONSTRAINT — never use it.
2. SQLite does NOT support ALTER TABLE ALTER COLUMN — never use it.
3. SQLite does NOT support ALTER TABLE ADD PRIMARY KEY — never use it.
4. SQLite does NOT support ADD COLUMN with non-constant DEFAULT — add column as NULL then UPDATE.
5. To change column types, add NOT NULL, or add FKs: CREATE new table with correct schema, INSERT INTO new SELECT from old, DROP old, RENAME new to original name.
6. Apostrophes in data (e.g., O'Brien, O'Neill) are present — always use parameterized patterns or escape with ''.
7. For table normalization: create new tables first, INSERT INTO ... SELECT, then drop old tables.
8. For ORPHANED FK rows: before inserting into a FK-constrained table, DELETE or INSERT INTO audit_log any rows whose FK reference does not exist in the parent table. Example: DELETE FROM assets WHERE employee_id NOT IN (SELECT id FROM employees).
9. For TEXT salary columns like '$90000': use CAST(REPLACE(REPLACE(salary, '$', ''), ',', '') AS INTEGER) to convert.
10. Execute exactly ONE SQL statement per step.
11. When migration is complete (schemas match, data preserved), set submit_final to true IMMEDIATELY.

Respond ONLY with valid JSON — no markdown, no code blocks, no text outside the object:
{"sql_command": "your SQL here", "reasoning": "why", "submit_final": false}"""

ALL_TASKS = ["column-restructure", "table-normalization", "cascade-migration"]
MAX_STEPS = 20  # 20 gives Task 3 enough budget for 4-table cascade + audit
MAX_PARSE_ERRORS = 5  # Higher tolerance for thinking models (Qwen3, DeepSeek-R1)

# Auto-submit threshold: if migration_progress >= this, force submit_final
AUTO_SUBMIT_THRESHOLD = 0.95


def call_llm(messages: list, timeout: int = 90) -> str:
    """Call the LLM API and return the response content."""
    from openai import OpenAI

    client = OpenAI(
        base_url=API_BASE_URL,
        api_key=API_KEY,
        timeout=timeout,
    )

    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.0,  # Deterministic output — eliminates variance
            max_tokens=1024,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        raise TimeoutError(f"LLM API error: {e}")


def parse_action(raw_text: str) -> dict:
    """
    Parse LLM output into an action dict.

    Handles: raw JSON, markdown-wrapped JSON (```json ... ```),
    <think>...</think> reasoning tokens (Qwen3, DeepSeek-R1),
    and common LLM mistakes like trailing commas or extra text.
    """
    import re
    text = raw_text.strip()

    # Strip <think>...</think> blocks emitted by reasoning models (Qwen3, R1)
    # Must do this BEFORE any other processing
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

    # Also strip partial/unclosed think blocks (truncated output)
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

    # Try to find JSON object in the text (handles preamble text or extra trailing content)
    start = text.find("{")
    end = text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass

    # Last resort: try to extract just sql_command if JSON is truncated
    sql_match = re.search(r'"sql_command"\s*:\s*"([^"]+)"', text)
    if sql_match:
        return {
            "sql_command": sql_match.group(1),
            "reasoning": "auto-extracted from malformed response",
            "submit_final": False,
        }

    raise ValueError(f"Could not parse JSON from LLM response: {text[:200]}")


def run_task_local(task_name: str) -> dict:
    """
    Run a single task using a local environment instance (no server needed).

    This is the primary mode — avoids HTTP overhead and works inside Docker.
    """
    # Import environment directly
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from server.environment import DbMigrationEnvironment
    from models import MigrationAction

    env = DbMigrationEnvironment(task_name=task_name)

    print(f"[START] task={task_name} env=sql-migration-agent model={MODEL_NAME}", flush=True)

    obs = env.reset()
    history = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Initial observation message
    initial_msg = (
        f"CURRENT DATABASE SCHEMA:\n{obs.current_schema_sql}\n\n"
        f"TARGET SCHEMA:\n{obs.target_schema_sql}\n\n"
        f"Status: {obs.last_execution_result}\n"
        f"Migration progress: {obs.migration_progress:.2f}\n\n"
        f"Write your first SQL command to begin the migration."
    )
    history.append({"role": "user", "content": initial_msg})

    rewards_list = []
    parse_errors = 0
    final_score = 0.0
    steps_taken = 0
    done = False
    peak_score = 0.0  # Track the highest score we've reached

    for step in range(MAX_STEPS):
        if done:
            break

        # Context truncation: system prompt + last 10 messages (5 pairs)
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
        except ValueError as e:
            parse_errors += 1
            print(f"[STEP] step={step+1} action=PARSE_ERROR reward=0.00 done=false error=parse_error", flush=True)
            if parse_errors >= MAX_PARSE_ERRORS:
                print(f"[STEP] step={step+1} action=MAX_PARSE_ERRORS reward=0.00 done=true error=too_many_parse_errors", flush=True)
                done = True
                break
            history.append({"role": "assistant", "content": raw_response})
            history.append({
                "role": "user",
                "content": "ERROR: Your response was not valid JSON. Respond ONLY with: {\"sql_command\": \"...\", \"reasoning\": \"...\", \"submit_final\": false}",
            })
            continue

        parse_errors = 0

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

        # Track peak score
        if final_score > peak_score:
            peak_score = final_score

        # AUTO-SUBMIT: If we just reached a near-perfect score, force submit
        # This prevents the LLM from continuing to send queries and regressing
        if final_score >= AUTO_SUBMIT_THRESHOLD and not done:
            done = True
            # Submit a final no-op to lock in the score
            submit_action = MigrationAction(
                sql_command="SELECT 1",
                reasoning="Migration complete — auto-submitting",
                submit_final=True,
            )
            obs = env.step(submit_action)
            final_score = obs.migration_progress

        # Abbreviate SQL for logging
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

        feedback_msg = (
            f"EXECUTION RESULT: {obs.last_execution_result}\n\n"
            f"CURRENT SCHEMA:\n{obs.current_schema_sql}\n\n"
            f"Migration progress: {obs.migration_progress:.2f}"
        )
        if done:
            feedback_msg += "\n\nEpisode complete."
        elif obs.migration_progress >= 0.9:
            feedback_msg += (
                "\n\nMigration is nearly complete! Compare the current schema "
                "carefully to the target schema. If they match and data is "
                "preserved, set submit_final to true in your next response."
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
    """Run all 3 tasks sequentially."""
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
    print(
        f"[SUMMARY] task1={results.get('column-restructure', 0):.2f} "
        f"task2={results.get('table-normalization', 0):.2f} "
        f"task3={results.get('cascade-migration', 0):.2f} "
        f"avg={avg:.2f}",
        flush=True,
    )


if __name__ == "__main__":
    main()
