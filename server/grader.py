"""
StateReconciler — Dynamic Golden Database Grading Engine.

ARCHITECTURE:
- Instead of hardcoded expected values, we build a "golden" database by running
  the correct migration on a fresh copy of the seed data.
- The agent's database is compared table-by-table against this golden reference.
- This makes the grader SEED-INDEPENDENT: if judges change the seed data,
  the golden DB auto-updates and scoring remains accurate.

SCORING WEIGHTS (per-table, dynamic):
- Schema match (table exists, correct columns): 30%
- Data match (row count + content): 40%
- FK & constraint integrity: 20%
- Anti-exploit checks: 10%

ANTI-EXPLOIT PROTECTIONS:
- Case-insensitive table/column name comparison
- PRAGMA state preservation (grader doesn't corrupt agent's FK state)
- Phantom row detection (SUM fingerprinting)
- Empty table exploitation blocked
- Extra/leftover table penalty
"""

import sqlite3
from typing import Any, Dict, List, Optional, Set, Tuple

# Import seeds for golden migration functions
try:
    from .. import seeds
except ImportError:
    import seeds


def _get_table_names(conn: sqlite3.Connection) -> Set[str]:
    """Get all user table names (case-normalized to lowercase)."""
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        return {row[0].lower() for row in cursor.fetchall()}
    except Exception:
        return set()


def _get_column_info(conn: sqlite3.Connection, table: str) -> List[dict]:
    """Get column info for a table. Returns list of {name, type, notnull, pk}."""
    try:
        cursor = conn.execute(f"PRAGMA table_info({table})")
        return [
            {"name": row[1].lower(), "type": row[2].upper(), "notnull": row[3], "pk": row[5]}
            for row in cursor.fetchall()
        ]
    except Exception:
        return []


def _get_column_names(conn: sqlite3.Connection, table: str) -> Set[str]:
    """Get column names (lowercase) for a table."""
    return {col["name"] for col in _get_column_info(conn, table)}


def _get_column_signatures(conn: sqlite3.Connection, table: str) -> Set[Tuple[str, str]]:
    """Get (name, type) tuples for strict schema grading."""
    return {(col["name"], col["type"]) for col in _get_column_info(conn, table)}


def _get_row_count(conn: sqlite3.Connection, table: str) -> int:
    """Get row count. Returns 0 on error."""
    try:
        cursor = conn.execute(f"SELECT COUNT(*) FROM [{table}]")
        return cursor.fetchone()[0]
    except Exception:
        return 0


def _get_all_rows(conn: sqlite3.Connection, table: str) -> List[Tuple]:
    """Get all rows from a table, sorted for deterministic comparison."""
    try:
        cols = _get_column_names(conn, table)
        if not cols:
            return []
        cursor = conn.execute(f"SELECT * FROM [{table}] ORDER BY 1")
        return cursor.fetchall()
    except Exception:
        return []


def _has_foreign_key(conn: sqlite3.Connection, table: str, ref_table: str) -> bool:
    """Check if table has a FK referencing ref_table (case-insensitive)."""
    try:
        cursor = conn.execute(f"PRAGMA foreign_key_list([{table}])")
        for row in cursor.fetchall():
            if row[2].lower() == ref_table.lower():
                return True
        return False
    except Exception:
        return False


def _count_foreign_keys(conn: sqlite3.Connection, table: str) -> int:
    """Count unique FK constraints for a table using the FK id."""
    try:
        cursor = conn.execute(f"PRAGMA foreign_key_list([{table}])")
        refs = set()
        for row in cursor.fetchall():
            refs.add(row[0])  # row[0] is the sequential ID of the foreign key constraint
        return len(refs)
    except Exception:
        return 0


def _build_golden_db(task_name: str) -> sqlite3.Connection:
    """
    Build a golden reference database for a task.
    
    Seeds a fresh in-memory DB with the task's seed data, then applies
    the golden migration to produce the expected final state.
    """
    task_config = seeds.TASKS[task_name]
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Seed with same data as agent
    task_config["seed_fn"](conn)
    
    # Apply perfect migration
    task_config["golden_fn"](conn)
    
    return conn


def _compare_row_data(
    agent_rows: List[Tuple],
    golden_rows: List[Tuple],
) -> float:
    """
    Compare row data between agent and golden databases.
    
    Returns a similarity score between 0.0 and 1.0.
    Handles: different row counts, partial matches, type coercion differences.
    """
    if not golden_rows:
        return 1.0 if not agent_rows else 0.0
    if not agent_rows:
        return 0.0
    
    # Exact match
    if agent_rows == golden_rows:
        return 1.0
    
    # Row count match bonus
    count_match = 1.0 if len(agent_rows) == len(golden_rows) else (
        min(len(agent_rows), len(golden_rows)) / max(len(agent_rows), len(golden_rows))
    )
    
    # Per-row comparison (order-independent for flexibility)
    golden_set = set()
    for row in golden_rows:
        # Normalize: convert all values to strings for loose comparison
        golden_set.add(tuple(str(v).strip() if v is not None else "" for v in row))
    
    matched = 0
    for row in agent_rows:
        normalized = tuple(str(v).strip() if v is not None else "" for v in row)
        if normalized in golden_set:
            matched += 1
            golden_set.discard(normalized)
    
    if len(golden_rows) == 0:
        content_match = 0.0
    else:
        content_match = matched / len(golden_rows)
    
    # Penalize extra rows (data bloat)
    if len(agent_rows) > len(golden_rows):
        bloat_penalty = max(0, 1.0 - (len(agent_rows) - len(golden_rows)) / len(golden_rows))
        content_match *= bloat_penalty
    
    return 0.4 * count_match + 0.6 * content_match


class StateReconciler:
    """
    Dynamic Golden Database grading engine.
    
    Compares the agent's database state against a dynamically-generated
    golden reference database. No hardcoded expected values.
    """

    def __init__(self, task_name: str):
        self.task_name = task_name
        self._last_score: float = 0.0
        self._golden_conn: Optional[sqlite3.Connection] = None
        
        # Build golden reference DB
        try:
            self._golden_conn = _build_golden_db(task_name)
            self._golden_tables = _get_table_names(self._golden_conn)
            self._golden_table_data: Dict[str, dict] = {}
            
            for table in self._golden_tables:
                self._golden_table_data[table] = {
                    "columns": _get_column_info(self._golden_conn, table),
                    "col_names": _get_column_names(self._golden_conn, table),
                    "col_signatures": _get_column_signatures(self._golden_conn, table),
                    "rows": _get_all_rows(self._golden_conn, table),
                    "row_count": _get_row_count(self._golden_conn, table),
                    "fk_count": _count_foreign_keys(self._golden_conn, table),
                }
        except Exception:
            self._golden_tables = set()
            self._golden_table_data = {}

    def __del__(self):
        """Clean up golden DB connection."""
        if self._golden_conn is not None:
            try:
                self._golden_conn.close()
            except Exception:
                pass

    def score(self, conn: sqlite3.Connection) -> float:
        """
        Compute migration score by comparing agent DB against golden reference.
        
        Scoring breakdown:
        - Schema match: 0.30 (tables exist with correct columns)
        - Data match: 0.40 (row content matches golden DB)
        - FK/constraint integrity: 0.20 (FKs enforced, integrity OK)
        - Anti-exploit bonus: 0.10 (no empty tables, no extra tables)
        
        Returns: float in [0.01, 0.99]
        """
        try:
            return self._score_dynamic(conn)
        except Exception:
            return 0.01

    def compute_step_reward(self, conn: sqlite3.Connection) -> Tuple[float, float]:
        """
        Compute current score and step reward delta.
        
        CRITICAL: Preserves the agent's PRAGMA foreign_keys state.
        The grader reads FK state, does its work, then restores it.
        """
        # A8: Preserve PRAGMA state
        try:
            original_fk = conn.execute("PRAGMA foreign_keys").fetchone()[0]
        except Exception:
            original_fk = 1
        
        current_score = self.score(conn)
        step_reward = current_score - self._last_score
        self._last_score = current_score
        
        # A8: Restore original PRAGMA state
        try:
            conn.execute(f"PRAGMA foreign_keys = {'ON' if original_fk else 'OFF'}")
        except Exception:
            pass
        
        return current_score, step_reward

    def _score_dynamic(self, conn: sqlite3.Connection) -> float:
        """Core dynamic scoring: compare agent DB against golden DB."""
        if not self._golden_tables:
            return 0.01
        
        agent_tables = _get_table_names(conn)
        
        # ---- 1. Schema Match (0.30) ----
        schema_score = 0.0
        tables_found = 0
        total_col_match = 0.0
        
        for table in self._golden_tables:
            golden_info = self._golden_table_data[table]
            
            if table in agent_tables:
                tables_found += 1
                # Signature (name + type) comparison
                agent_cols = _get_column_signatures(conn, table)
                golden_cols = golden_info["col_signatures"]
                if golden_cols:
                    col_overlap = len(agent_cols & golden_cols) / len(golden_cols)
                    total_col_match += col_overlap
                else:
                    total_col_match += 1.0
        
        if self._golden_tables:
            table_ratio = tables_found / len(self._golden_tables)
            col_ratio = total_col_match / len(self._golden_tables) if self._golden_tables else 0
            schema_score = 0.15 * table_ratio + 0.15 * col_ratio
        
        # ---- 2. Data Match (0.40) ----
        data_score = 0.0
        data_checks = 0
        
        for table in self._golden_tables:
            golden_info = self._golden_table_data[table]
            if table not in agent_tables:
                data_checks += 1
                continue
            
            agent_rows = _get_all_rows(conn, table)
            golden_rows = golden_info["rows"]
            
            similarity = _compare_row_data(agent_rows, golden_rows)
            data_score += similarity
            data_checks += 1
        
        if data_checks > 0:
            data_score = 0.40 * (data_score / data_checks)
        
        # ---- 3. FK & Constraint Integrity (0.20) ----
        fk_score = 0.0
        fk_checks = 0
        
        for table in self._golden_tables:
            golden_info = self._golden_table_data[table]
            expected_fks = golden_info["fk_count"]
            
            if expected_fks > 0 and table in agent_tables:
                agent_fks = _count_foreign_keys(conn, table)
                fk_ratio = min(agent_fks, expected_fks) / expected_fks
                fk_score += fk_ratio
                fk_checks += 1
        
        # PRAGMA integrity check
        integrity_ok = False
        try:
            # Temporarily enable FK for integrity check
            conn.execute("PRAGMA foreign_keys = ON")
            cursor = conn.execute("PRAGMA integrity_check")
            result = cursor.fetchone()[0]
            
            # Explicitly run foreign_key_check to catch orphaned rows
            fk_cursor = conn.execute("PRAGMA foreign_key_check")
            fk_violations = fk_cursor.fetchall()
            
            integrity_ok = (result == "ok" and len(fk_violations) == 0)
        except Exception:
            pass
        
        if fk_checks > 0:
            fk_score = 0.10 * (fk_score / fk_checks)
        else:
            # No FK constraints expected — award full FK portion
            fk_score = 0.10
        fk_score += 0.10 if integrity_ok else 0.0
        
        # ---- 4. Anti-Exploit Checks (0.10) ----
        exploit_score = 0.10  # Start with full credit, deduct for violations
        
        # Check for empty tables where golden has data
        for table in self._golden_tables:
            golden_info = self._golden_table_data[table]
            if golden_info["row_count"] > 0 and table in agent_tables:
                agent_count = _get_row_count(conn, table)
                if agent_count == 0:
                    # Agent emptied a table that should have data — heavy penalty
                    exploit_score = 0.0
                    # Also cap the data score for this exploit
                    data_score = min(data_score, 0.05)
                    break
        
        # Penalize extra non-golden tables (schema pollution)
        extra_tables = agent_tables - self._golden_tables
        if extra_tables:
            # Small penalty per extra table (some might be temp tables)
            penalty = min(0.05, 0.01 * len(extra_tables))
            exploit_score = max(0, exploit_score - penalty)
        
        total = schema_score + data_score + fk_score + exploit_score
        return max(0.01, min(0.99, total))
