"""
SQL Migration Environment — An OpenEnv environment for benchmarking
autonomous database migration agents.

This environment places an AI agent inside a broken or schema-drifted
SQLite database and tasks it with autonomously migrating the database
to a target state using only SQL commands, without losing any row data.
"""

from .client import DbMigrationEnv
from .models import MigrationAction, MigrationObservation, MigrationState

__all__ = [
    "DbMigrationEnv",
    "MigrationAction",
    "MigrationObservation",
    "MigrationState",
]
