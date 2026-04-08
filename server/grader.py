"""
StateReconciler — The grading engine for the SQL Migration Environment.

This module scores the current database state against the target schema
and data expectations. Built BEFORE the environment (test-driven development).

CRITICAL RULES:
- The grader NEVER modifies the database (SELECT and PRAGMA only)
- The grader NEVER raises exceptions (catches everything, returns 0.0 on failure)
- Scores are always in [0.0, 1.0]
- The exploit check penalizes empty tables that match the target schema
"""

import sqlite3
from typing import Dict, List, Optional, Set, Tuple

from seeds import (
    TASK1_EXPECTED_ROWS,
    TASK2_EXPECTED_CUSTOMER_COUNT,
    TASK2_EXPECTED_ORDER_COUNT,
    TASK3_EXPECTED_AUDIT_COUNT,
    TASK3_EXPECTED_AUDIT_ENTRIES,
    TASK3_EXPECTED_EMPLOYEE_COUNT,
    TASK3_EXPECTED_SALARIES,
)


def _get_table_names(conn: sqlite3.Connection) -> Set[str]:
    """Get all table names in the database."""
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return {row[0] for row in cursor.fetchall()}
    except Exception:
        return set()


def _get_column_names(conn: sqlite3.Connection, table: str) -> Set[str]:
    """Get column names for a given table."""
    try:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        return {row[1] for row in cursor.fetchall()}
    except Exception:
        return set()


def _get_row_count(conn: sqlite3.Connection, table: str) -> int:
    """Get row count of a table. Returns 0 on any error."""
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def _has_foreign_key(conn: sqlite3.Connection, table: str, ref_table: str) -> bool:
    """Check if table has a FK referencing ref_table."""
    try:
        cursor = conn.execute(f"PRAGMA foreign_key_list({table})")
        for row in cursor.fetchall():
            if row[2] == ref_table:
                return True
        return False
    except Exception:
        return False


class StateReconciler:
    """
    Scores the current database state against the target for a specific task.

    Instantiated once per episode. Tracks previous score to compute step deltas.
    """

    def __init__(self, task_name: str):
        self.task_name = task_name
        self._last_score: float = 0.0

    def score(self, conn: sqlite3.Connection) -> float:
        """
        Compute the current migration score [0.0, 1.0].

        Routes to the appropriate task-specific scorer.
        Never raises — returns 0.0 on any unexpected error.
        """
        try:
            if self.task_name == "column-restructure":
                return self._score_task1(conn)
            elif self.task_name == "table-normalization":
                return self._score_task2(conn)
            elif self.task_name == "cascade-migration":
                return self._score_task3(conn)
            else:
                return 0.0
        except Exception:
            return 0.0

    def compute_step_reward(self, conn: sqlite3.Connection) -> Tuple[float, float]:
        """
        Compute both the current score and the step reward delta.

        Returns:
            (current_score, step_reward) where step_reward = current - previous
        """
        current_score = self.score(conn)
        step_reward = current_score - self._last_score
        self._last_score = current_score
        return current_score, step_reward

    # =========================================================================
    # Task 1: Column Restructure
    # =========================================================================
    # Weights: schema=0.4, row_count=0.2, data=0.4

    def _score_task1(self, conn: sqlite3.Connection) -> float:
        score = 0.0
        tables = _get_table_names(conn)

        if "users" not in tables:
            return 0.0

        columns = _get_column_names(conn, "users")

        # Schema check: full_name exists, old columns gone
        has_full_name = "full_name" in columns
        old_cols_gone = "first_name" not in columns and "last_name" not in columns

        if has_full_name and old_cols_gone:
            score += 0.4  # Full schema credit
        elif has_full_name:
            score += 0.2  # Partial: full_name exists but old cols remain

        # Row count check
        row_count = _get_row_count(conn, "users")
        if row_count == len(TASK1_EXPECTED_ROWS):
            score += 0.2

        # Data correctness check
        if has_full_name:
            try:
                cursor = conn.execute("SELECT id, full_name FROM users ORDER BY id")
                actual_rows = cursor.fetchall()
                if actual_rows == TASK1_EXPECTED_ROWS:
                    score += 0.4
                elif len(actual_rows) > 0:
                    # Partial credit: fraction of correct rows
                    correct = sum(
                        1 for a, e in zip(actual_rows, TASK1_EXPECTED_ROWS)
                        if a == e
                    )
                    score += 0.4 * (correct / len(TASK1_EXPECTED_ROWS))
            except Exception:
                pass

        # Exploit check: if schema matches but table is empty, cap score
        if has_full_name and old_cols_gone and row_count == 0:
            score = min(score, 0.1)

        return min(1.0, score)

    # =========================================================================
    # Task 2: Table Normalization
    # =========================================================================
    # Weights: tables_exist=0.1, fk=0.2, customer_count=0.2,
    #          order_count=0.2, no_null_ids=0.1, integrity=0.2

    def _score_task2(self, conn: sqlite3.Connection) -> float:
        score = 0.0
        tables = _get_table_names(conn)

        # Both tables exist
        has_customers = "customers" in tables
        has_orders = "orders" in tables
        if has_customers and has_orders:
            score += 0.1

        # FK constraint: orders -> customers
        if has_orders and _has_foreign_key(conn, "orders", "customers"):
            score += 0.2

        # Correct distinct customer count
        if has_customers:
            try:
                cursor = conn.execute("SELECT COUNT(DISTINCT email) FROM customers")
                distinct_count = cursor.fetchone()[0]
                if distinct_count == TASK2_EXPECTED_CUSTOMER_COUNT:
                    score += 0.2
            except Exception:
                pass

        # Correct order count (all original purchases preserved)
        if has_orders:
            order_count = _get_row_count(conn, "orders")
            if order_count == TASK2_EXPECTED_ORDER_COUNT:
                score += 0.2

        # No NULL customer_ids in orders
        if has_orders:
            try:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM orders WHERE customer_id IS NULL"
                )
                null_count = cursor.fetchone()[0]
                if null_count == 0:
                    score += 0.1
            except Exception:
                pass

        # Integrity check
        try:
            cursor = conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            if result == "ok":
                score += 0.2
        except Exception:
            pass

        # Exploit check: tables exist but are empty
        if has_customers and has_orders:
            c_count = _get_row_count(conn, "customers")
            o_count = _get_row_count(conn, "orders")
            if c_count == 0 and o_count == 0:
                score = min(score, 0.1)

        return min(1.0, score)

    # =========================================================================
    # Task 3: Cascade Migration
    # =========================================================================
    # Granular partial credit for each relationship in the FK chain.
    # Total weights: audit=0.30, fk_chain=0.20, emp_count=0.05,
    #                salary_coercion=0.15, no_orphans=0.10, integrity=0.10
    #                companies_not_null=0.05 (within fk_chain)
    # Total max = 0.90 for all grader checks + 0.10 integrity = 1.00

    def _score_task3(self, conn: sqlite3.Connection) -> float:
        score = 0.0
        tables = _get_table_names(conn)

        # --- audit_log checks (0.30 total) ---
        has_audit = "audit_log" in tables
        if has_audit:
            score += 0.1  # table exists

        if has_audit:
            audit_count = _get_row_count(conn, "audit_log")
            if audit_count >= TASK3_EXPECTED_AUDIT_COUNT:
                score += 0.1  # has enough rows

        if has_audit:
            try:
                cursor = conn.execute(
                    "SELECT source_table, reason FROM audit_log ORDER BY source_table, reason"
                )
                actual_entries = cursor.fetchall()
                expected_sorted = sorted(TASK3_EXPECTED_AUDIT_ENTRIES)
                if actual_entries == expected_sorted:
                    score += 0.2
                elif len(actual_entries) > 0:
                    correct = sum(1 for a in actual_entries if a in TASK3_EXPECTED_AUDIT_ENTRIES)
                    score += 0.2 * (correct / TASK3_EXPECTED_AUDIT_COUNT)
            except Exception:
                pass

        # --- FK chain checks (0.20 total, 0.05 each) ---
        # departments -> companies
        if "departments" in tables and _has_foreign_key(conn, "departments", "companies"):
            score += 0.05
        # employees -> departments
        if "employees" in tables and _has_foreign_key(conn, "employees", "departments"):
            score += 0.05
        # assets -> employees
        if "assets" in tables and _has_foreign_key(conn, "assets", "employees"):
            score += 0.05
        # companies.name NOT NULL
        if "companies" in tables:
            try:
                cursor = conn.execute("PRAGMA table_info(companies)")
                for row in cursor.fetchall():
                    if row[1] == "name" and row[3] == 1:  # notnull flag
                        score += 0.05
                        break
            except Exception:
                pass

        # --- Employee count (Hal Patel removed) (0.05) ---
        if "employees" in tables:
            emp_count = _get_row_count(conn, "employees")
            if emp_count == TASK3_EXPECTED_EMPLOYEE_COUNT:
                score += 0.05

        # --- Salary coercion: TEXT $90000 -> INTEGER 90000 (0.15) ---
        if "employees" in tables:
            try:
                all_correct = True
                for emp_id, expected_salary in TASK3_EXPECTED_SALARIES.items():
                    cursor = conn.execute(
                        "SELECT salary FROM employees WHERE id = ?", (emp_id,)
                    )
                    row = cursor.fetchone()
                    if row is None:
                        all_correct = False
                        break
                    actual = row[0]
                    if not isinstance(actual, int):
                        try:
                            actual = int(actual)
                        except (ValueError, TypeError):
                            all_correct = False
                            break
                    if actual != expected_salary:
                        all_correct = False
                        break
                if all_correct:
                    score += 0.15
            except Exception:
                pass

        # --- No orphaned assets (0.10) ---
        if "assets" in tables and "employees" in tables:
            try:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM assets WHERE employee_id NOT IN "
                    "(SELECT id FROM employees)"
                )
                orphan_count = cursor.fetchone()[0]
                if orphan_count == 0:
                    score += 0.10
            except Exception:
                pass

        # --- Integrity check (0.10) ---
        try:
            cursor = conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            if result == "ok":
                score += 0.10
        except Exception:
            pass

        # Exploit check: if employees table is empty
        if "employees" in tables and _get_row_count(conn, "employees") == 0:
            score = min(score, 0.1)

        return min(1.0, score)
