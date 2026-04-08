"""
SQL Migration Environment Client.

Provides the client for connecting to a SQL Migration Environment server.
Extends the base OpenEnv EnvClient for WebSocket-based persistent sessions.

Example:
    >>> from sql_migration_env import DbMigrationEnv
    >>>
    >>> env = DbMigrationEnv(base_url="http://localhost:7860").sync()
    >>> with env:
    ...     result = env.reset()
    ...     result = env.step({"sql_command": "ALTER TABLE users RENAME COLUMN first_name TO full_name", "reasoning": "test"})
    ...     print(result.observation)
"""

from typing import Any, Dict

from openenv.core.env_client import EnvClient
from openenv.core.client_types import StepResult

from .models import MigrationAction, MigrationObservation, MigrationState


class DbMigrationEnv(EnvClient):
    """
    Client for the SQL Migration Environment.

    Inherits connection management, async/sync wrappers, and Docker/HF Space
    support from EnvClient. Provides typed step/reset interactions.

    Example:
        >>> async with DbMigrationEnv(base_url="http://localhost:7860") as env:
        ...     result = await env.reset(task_name="column-restructure")
        ...     while not result.done:
        ...         action = {"sql_command": "...", "reasoning": "...", "submit_final": False}
        ...         result = await env.step(action)
        ...     print(f"Final score: {result.observation.get('migration_progress', 0)}")

    Example with sync wrapper:
        >>> env = DbMigrationEnv(base_url="http://localhost:7860").sync()
        >>> with env:
        ...     result = env.reset()
        ...     print(result.observation)
    """

    def _step_payload(self, action: Any) -> Dict[str, Any]:
        """Convert action to JSON payload for the server."""
        if isinstance(action, MigrationAction):
            return action.model_dump()
        elif isinstance(action, dict):
            return action
        else:
            raise ValueError(f"Expected MigrationAction or dict, got {type(action)}")

    def _parse_result(self, payload: Dict[str, Any]) -> StepResult:
        """Parse server response into StepResult."""
        observation = payload.get("observation", {})
        reward = payload.get("reward")
        done = payload.get("done", False)
        return StepResult(
            observation=observation,
            reward=reward,
            done=done,
        )

    def _parse_state(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Parse state response."""
        return payload
