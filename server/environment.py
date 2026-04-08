"""
SQL Migration Environment Server Implementation.

This is the core environment that wraps SQLite and exposes it via the OpenEnv
Environment interface. Each WebSocket session gets its own environment instance
with an isolated in-memory database.
"""

import sqlite3
import uuid
from typing import Any, Optional

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
            task_name: One of "column-restructure", "table-normalization", "cascade-migration"
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
        self._state = MigrationState(
            task_name=task_name,
            migration_progress=0.0,
            max_steps=20,
        )

    def _get_current_schema(self) -> str:
        """Get current database schema as DDL string."""
        if self._conn is None:
            return ""
        try:
            cursor = self._conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY name"
            )
            schemas = [row[0] for row in cursor.fetchall()]
            return ";\n\n".join(schemas) + ";" if schemas else ""
        except Exception:
            return ""

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

        # Clean up previous connection
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

        # Create fresh in-memory database
        self._conn = sqlite3.connect(":memory:")

        # CRITICAL: Enable foreign key enforcement
        self._conn.execute("PRAGMA foreign_keys = ON")

        # Seed source data
        self._task_config["seed_fn"](self._conn)

        # Initialize grader
        self._reconciler = StateReconciler(self.task_name)

        # Reset counters
        self._step_count = 0
        self._state = MigrationState(
            episode_id=episode_id or str(uuid.uuid4()),
            step_count=0,
            task_name=self.task_name,
            migration_progress=0.0,
            max_steps=20,
        )

        return MigrationObservation(
            done=False,
            reward=0.0,
            current_schema_sql=self._get_current_schema(),
            target_schema_sql=self._task_config["target_ddl"],
            last_execution_result="Environment initialized. Ready for migration.",
            step_number=0,
            migration_progress=0.0,
            task_name=self.task_name,
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
            timeout_s: Unused
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

        # Execute the SQL command
        execution_result = ""
        action_error = None
        try:
            cursor = self._conn.execute(action.sql_command)
            self._conn.commit()
            rows_affected = cursor.rowcount
            execution_result = f"Success: {rows_affected} rows affected"
        except Exception as e:
            # Never crash — feed the error back to the agent
            execution_result = str(e)
            action_error = str(e)
            # Rollback failed transaction
            try:
                self._conn.rollback()
            except Exception:
                pass

        # Compute scores
        current_score, step_reward = self._reconciler.compute_step_reward(self._conn)

        # Episode termination: submit_final, max steps (20), OR perfect score
        done = action.submit_final or self._step_count >= 20 or current_score >= 0.99

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

        return MigrationObservation(
            done=done,
            reward=step_reward,
            current_schema_sql=self._get_current_schema(),
            target_schema_sql=self._task_config["target_ddl"],
            last_execution_result=execution_result,
            step_number=self._step_count,
            migration_progress=current_score,
            task_name=self.task_name,
            metadata=meta,
        )

    @property
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
