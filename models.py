"""
Data models for SQL Migration Environment.

Defines the Action, Observation, and State types used by the environment,
client, and server. All models extend the OpenEnv base types.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from openenv.core.env_server.types import Action, Observation, State
from pydantic import Field


class MigrationAction(Action):
    """
    Action for the SQL Migration Environment.

    The agent sends a SQL command to execute against the database,
    along with reasoning for chain-of-thought logging.

    Attributes:
        sql_command: Raw SQL statement to execute (e.g., ALTER TABLE, UPDATE, CREATE).
        reasoning: Free-form explanation of why the agent chose this action.
                   Logged in metadata for Phase 3 human review but not used by grader.
        submit_final: When True, signals the agent believes migration is complete.
                      Triggers final grading and ends the episode.
    """

    sql_command: str = Field(
        description="The raw SQL statement to execute against the database"
    )
    reasoning: str = Field(
        default="",
        description="Chain-of-thought explanation for this action"
    )
    submit_final: bool = Field(
        default=False,
        description="Set to true when you believe the migration is complete"
    )


class MigrationObservation(Observation):
    """
    Observation from the SQL Migration Environment.

    Returned after every reset() and step() call. Contains everything
    the agent needs to decide its next action.

    Inherits from Observation:
        done: bool — Whether the episode has terminated
        reward: float | None — Step reward (delta from previous score)
        metadata: dict — Additional metadata

    Attributes:
        current_schema_sql: Current database DDL from sqlite_master.
        target_schema_sql: Target database DDL the agent must achieve.
        last_execution_result: Result of the last SQL execution or error message.
        step_number: Current step count (0 after reset, increments each step).
        migration_progress: Current grader score from 0.0 to 1.0.
        task_name: Name of the current task being attempted.
    """

    current_schema_sql: str = Field(
        default="",
        description="Current database schema DDL from sqlite_master"
    )
    target_schema_sql: str = Field(
        default="",
        description="Target database schema DDL the agent must achieve"
    )
    last_execution_result: str = Field(
        default="",
        description="Result of the last SQL execution or error string"
    )
    step_number: int = Field(
        default=0,
        description="Current step count in the episode"
    )
    migration_progress: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Current migration progress score from 0.0 to 1.0"
    )
    task_name: str = Field(
        default="",
        description="Name of the current task"
    )


class MigrationState(State):
    """
    State for the SQL Migration Environment.

    Returned by the state() property. Contains episode metadata.

    Inherits from State:
        episode_id: str — Unique episode identifier
        step_count: int — Number of steps taken

    Attributes:
        task_name: Name of the current task.
        migration_progress: Current grader score.
        max_steps: Maximum steps allowed per episode.
    """

    task_name: str = Field(
        default="column-restructure",
        description="Name of the current task"
    )
    migration_progress: float = Field(
        default=0.0,
        description="Current migration progress score"
    )
    max_steps: int = Field(
        default=15,
        description="Maximum steps allowed per episode"
    )
