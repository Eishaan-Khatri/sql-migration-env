"""
SQL Migration Environment Server Implementation.

This is the core environment that wraps SQLite and exposes it via the OpenEnv
Environment interface. Each WebSocket session gets its own environment instance
with an isolated in-memory database.

Architecture Fixes Applied:
- A1: SELECT queries return actual data rows (not just "rows affected")
- A2: SQL execution timeout via progress handler (prevents infinite CTEs)
- A3: Dangerous SQL blacklist (ATTACH, DETACH, LOAD_EXTENSION, writable_schema)
- A4: Transaction awareness (respects BEGIN/COMMIT/ROLLBACK from agent)
- A5: Trajectory logging (full SQL history in metadata on episode end)
- A6: Per-task max_steps from seeds registry
"""

import re
import sqlite3
import uuid
import difflib
from typing import Any, Dict, List, Optional

# Support both in-repo and standalone imports
try:
    from openenv.core.env_server.interfaces import Environment
    from ..models import MigrationAction, MigrationObservation, MigrationState
    from .grader import StateReconciler
except ImportError:
    from openenv.core.env_server.interfaces import Environment
    from models import MigrationAction, MigrationObservation, MigrationState
    from server.grader import StateReconciler

# Import seeds (handle both import paths)
try:
    from .. import seeds
except ImportError:
    import seeds


# --- A3: Dangerous SQL Blacklist ---
_DANGEROUS_PATTERNS = re.compile(
    r"\b(ATTACH\s+DATABASE|DETACH\s+DATABASE|LOAD_EXTENSION)\b"
    r"|PRAGMA\s+writable_schema",
    re.IGNORECASE,
)

# --- A4: Transaction control keywords ---
_TX_BEGIN = re.compile(r"^\s*(BEGIN|BEGIN\s+TRANSACTION|BEGIN\s+DEFERRED|BEGIN\s+IMMEDIATE|BEGIN\s+EXCLUSIVE)\s*;?\s*$", re.IGNORECASE)
_TX_END = re.compile(r"^\s*(COMMIT|END|END\s+TRANSACTION|ROLLBACK)\s*;?\s*$", re.IGNORECASE)

# --- A2: Maximum SQLite operations before timeout ---
_MAX_OPS = 500_000  # ~5 seconds on typical hardware


# (Timeout handled via progress handler return value, no exception needed)


class DbMigrationEnvironment(Environment):
    """
    SQL Schema Migration Environment.

    An AI agent is dropped into a broken or schema-drifted SQLite database
    and must write SQL to migrate it to the target state without losing data.

    Each instance is isolated (per-WebSocket session) with its own :memory: database.
    """

    SUPPORTS_CONCURRENT_SESSIONS = True

    def __init__(self, task_name: str = "column-restructure"):
        """
        Initialize the migration environment.

        Args:
            task_name: One of the registered task names in seeds.TASKS
        """
        super().__init__()

        if task_name not in seeds.TASKS:
            raise ValueError(
                f"Unknown task: {task_name}. "
                f"Available: {list(seeds.TASKS.keys())}"
            )

        self.task_name = task_name
        self._task_config = seeds.TASKS[task_name]
        self._conn: Optional[sqlite3.Connection] = None
        self._reconciler: Optional[StateReconciler] = None
        self._step_count = 0
        self._trajectory: List[Dict[str, Any]] = []  # A5
        self._in_explicit_tx = False  # A4
        self._max_steps = self._task_config.get("max_steps", 20)  # A6
        self._state = MigrationState(
            task_name=task_name,
            migration_progress=0.0,
            max_steps=self._max_steps,  # A6
        )

    def _get_current_schema(self) -> str:
        """Get current database schema as DDL string, filtering internal tables."""
        if self._conn is None:
            return ""
        try:
            cursor = self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' "
                "AND sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
            schemas = [row[0] for row in cursor.fetchall()]
            return ";\n\n".join(schemas) + ";" if schemas else ""
        except Exception:
            return ""

    def _is_read_query(self, sql: str) -> bool:
        """Check if SQL is a read-only query (SELECT or certain PRAGMAs)."""
        stripped = sql.strip().upper()
        if stripped.startswith("SELECT") or stripped.startswith("WITH"):
            return True
        # PRAGMA table_info, foreign_key_list, etc. are read-only
        if stripped.startswith("PRAGMA") and "=" not in stripped:
            return True
        return False

    def _execute_with_timeout(self, sql: str) -> tuple:
        """
        Execute SQL with a progress-handler-based timeout.

        Returns: (cursor_or_None, error_string_or_None)
        """
        ops_count = [0]

        def _progress_callback():
            ops_count[0] += 1
            if ops_count[0] > _MAX_OPS:
                return 1  # Non-zero = abort
            return 0

        self._conn.set_progress_handler(_progress_callback, 1000)
        try:
            cursor = self._conn.execute(sql)
            return cursor, None
        except sqlite3.OperationalError as e:
            err_str = str(e).lower()
            if "interrupted" in err_str or ops_count[0] > _MAX_OPS:
                return None, "Error: Query exceeded execution time limit (possible infinite loop). Simplify your query."
            if "table" in err_str and "already exists" in err_str:
                return None, f"Schema Error: {e}. You must DROP the old table first if replacing it."
            if "has no column" in err_str:
                return None, f"Schema Error: {e}. Check table columns."
            return None, str(e)
        except sqlite3.Warning as e:
            # Multi-statement fallback
            try:
                self._conn.executescript(sql)
                return None, None
            except Exception as script_e:
                return None, f"Error (Multi-Statement Fallback Failed): {script_e}. Original error: {e}"
        except Exception as e:
            err_str = str(e).lower()
            if "values for" in err_str and "columns" in err_str:
                return None, f"Data Error: {e}. Ensure you are inserting the correct number of columns."
            return None, str(e)
        finally:
            self._conn.set_progress_handler(None, 0)

    def _format_query_results(self, cursor) -> str:
        """Format SELECT query results as a readable table string."""
        try:
            rows = cursor.fetchall()
            if not rows:
                return "Query returned 0 rows."

            # Get column names
            col_names = [desc[0] for desc in cursor.description] if cursor.description else []

            # Cap at 50 rows
            truncated = len(rows) > 50
            display_rows = rows[:50]

            # Build output
            header = " | ".join(col_names) if col_names else "Results"
            lines = [header, "-" * len(header)]
            for row in display_rows:
                lines.append(" | ".join(str(v) for v in row))
            if truncated:
                lines.append(f"... ({len(rows) - 50} more rows truncated)")
            lines.append(f"({len(rows)} rows total)")

            return "\n".join(lines)
        except Exception:
            return "Query executed successfully."

    def reset(
        self,
        seed: Optional[int] = None,
        episode_id: Optional[str] = None,
        **kwargs: Any,
    ) -> MigrationObservation:
        """
        Reset the environment: create a fresh in-memory database and seed it.

        Args:
            seed: Unused (deterministic environment)
            episode_id: Optional episode identifier
            **kwargs: Additional reset parameters (including task_name override)

        Returns:
            Initial MigrationObservation with the broken schema and target DDL
        """
        # Allow task_name override via reset kwargs
        task_name = kwargs.get("task_name", self.task_name)
        if task_name != self.task_name and task_name in seeds.TASKS:
            self.task_name = task_name
            self._task_config = seeds.TASKS[task_name]
            self._max_steps = self._task_config.get("max_steps", 20)

        # Clean up previous connection
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

        # Create fresh in-memory database
        self._conn = sqlite3.connect(":memory:")

        # Performance PRAGMAs for Docker I/O
        self._conn.execute("PRAGMA journal_mode = MEMORY")
        
        # CRITICAL: Enable foreign key enforcement
        self._conn.execute("PRAGMA foreign_keys = ON")

        # Seed source data
        self._task_config["seed_fn"](self._conn)

        # Initialize grader
        self._reconciler = StateReconciler(self.task_name)

        # Reset counters and trajectory
        self._step_count = 0
        self._trajectory = []  # A5
        self._in_explicit_tx = False  # A4
        self._state = MigrationState(
            episode_id=episode_id or str(uuid.uuid4()),
            step_count=0,
            task_name=self.task_name,
            migration_progress=0.0,
            max_steps=self._max_steps,  # A6
        )

        # Compute initial score and sync grader baseline
        initial_score = self._reconciler.score(self._conn)
        self._reconciler._last_score = initial_score  # Prevent inflated first-step reward
        self._state.migration_progress = initial_score

        current_ddl = self._get_current_schema()
        target_ddl = self._task_config["target_ddl"]
        diff = "\n".join(difflib.unified_diff(
            current_ddl.splitlines(),
            target_ddl.splitlines(),
            fromfile="current_schema",
            tofile="target_schema",
            lineterm=""
        ))

        return MigrationObservation(
            done=False,
            reward=0.0,
            current_schema_sql=current_ddl,
            target_schema_sql=target_ddl,
            last_execution_result="Environment initialized. Ready for migration.",
            step_number=0,
            migration_progress=initial_score,
            task_name=self.task_name,
            schema_diff=diff if diff else "Schemas match exactly.",
            metadata={"status": "ready"},
        )

    def step(
        self,
        action: MigrationAction,
        timeout_s: Optional[float] = None,
        **kwargs: Any,
    ) -> MigrationObservation:
        """
        Execute a SQL action against the database.

        Args:
            action: MigrationAction with sql_command, reasoning, and submit_final
            timeout_s: Unused (we use progress handler instead)
            **kwargs: Additional parameters

        Returns:
            MigrationObservation with execution result, updated schema, and reward
        """
        if not isinstance(action, MigrationAction):
            raise ValueError(f"Expected MigrationAction, got {type(action)}")

        if self._conn is None or self._reconciler is None:
            return MigrationObservation(
                done=True,
                reward=0.0,
                current_schema_sql="",
                target_schema_sql=self._task_config["target_ddl"],
                last_execution_result="Error: Environment not initialized. Call reset() first.",
                step_number=self._step_count,
                migration_progress=0.01,
                task_name=self.task_name,
                metadata={"error": "not_initialized"},
            )

        self._step_count += 1
        sql_command = action.sql_command.strip()

        # --- A3: Dangerous SQL Blacklist ---
        sql_lower = sql_command.lower()
        if re.search(r"pragma\s+foreign_keys\s*=\s*(off|0)", sql_lower):
            execution_result = "Security Error: Disabling PRAGMA foreign_keys is strictly explicitly forbidden."
            action_error = "pragma_off_blocked"
        elif _DANGEROUS_PATTERNS.search(sql_command):
            execution_result = (
                "Error: This SQL command is not allowed for security reasons. "
                "ATTACH DATABASE, DETACH DATABASE, LOAD_EXTENSION, and "
                "PRAGMA writable_schema are blocked."
            )
            action_error = "blocked_command"
        else:
            # --- A4: Transaction Awareness ---
            execution_result = ""
            action_error = None

            if _TX_BEGIN.match(sql_command):
                # Agent wants to start a transaction
                try:
                    self._conn.execute("BEGIN")
                    self._in_explicit_tx = True
                    execution_result = "Success: Transaction started."
                except Exception as e:
                    execution_result = str(e)
                    action_error = str(e)
            elif _TX_END.match(sql_command):
                # Agent wants to commit or rollback
                try:
                    if sql_command.strip().upper().startswith("ROLLBACK"):
                        self._conn.rollback()
                        execution_result = "Success: Transaction rolled back."
                    else:
                        self._conn.commit()
                        execution_result = "Success: Transaction committed."
                    self._in_explicit_tx = False
                except Exception as e:
                    execution_result = str(e)
                    action_error = str(e)
                    self._in_explicit_tx = False
            else:
                # --- Normal SQL execution with timeout (A1, A2) ---
                cursor, error = self._execute_with_timeout(sql_command)

                if error:
                    execution_result = error
                    action_error = error
                    # Rollback failed transaction
                    try:
                        if not self._in_explicit_tx:
                            self._conn.rollback()
                    except Exception:
                        pass
                else:
                    # --- A1: SELECT result passthrough ---
                    if self._is_read_query(sql_command):
                        execution_result = self._format_query_results(cursor)
                    else:
                        rows_affected = getattr(cursor, "rowcount", -1) if cursor else -1
                        execution_result = f"Success: Action executed. Rows affected: {rows_affected}"
                        # Try to auto-commit
                        if not self._in_explicit_tx:
                            try:
                                self._conn.commit()
                            except Exception:
                                pass

        # Compute scores
        current_score, step_reward = self._reconciler.compute_step_reward(self._conn)

        # Episode termination: submit_final, max steps, OR perfect score
        done = action.submit_final or self._step_count >= self._max_steps or current_score >= 0.99

        # --- A5: Trajectory logging ---
        self._trajectory.append({
            "step": self._step_count,
            "sql": action.sql_command,
            "reasoning": action.reasoning,
            "result": execution_result[:200],  # Truncate for storage
            "score": current_score,
            "reward": step_reward,
            "error": action_error,
        })

        # Update state
        self._state.step_count = self._step_count
        self._state.migration_progress = current_score

        # Build metadata with reasoning and debug info
        meta = {
            "reasoning": action.reasoning,
            "sql_executed": action.sql_command,
            "step": self._step_count,
        }
        if action_error:
            meta["error"] = action_error
        # Include full trajectory on episode end
        if done:
            meta["trajectory"] = self._trajectory

        current_ddl = self._get_current_schema()
        target_ddl = self._task_config["target_ddl"]
        diff = "\n".join(difflib.unified_diff(
            current_ddl.splitlines(),
            target_ddl.splitlines(),
            fromfile="current_schema",
            tofile="target_schema",
            lineterm=""
        ))

        return MigrationObservation(
            done=done,
            reward=step_reward,
            current_schema_sql=current_ddl,
            target_schema_sql=target_ddl,
            last_execution_result=execution_result,
            step_number=self._step_count,
            migration_progress=current_score,
            task_name=self.task_name,
            schema_diff=diff if diff else "Schemas match exactly.",
            metadata=meta,
        )

    def state(self) -> MigrationState:
        """Get current environment state."""
        return self._state

    def close(self) -> None:
        """Clean up resources."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
