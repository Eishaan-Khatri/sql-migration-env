"""Smoke test for the SQL Migration Environment."""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'OpenEnv', 'src'))

import sqlite3

# Test 1: Models import
from models import MigrationAction, MigrationObservation, MigrationState
print("PASS: Models imported")

# Test 2: Task 1 seeds
from seeds import seed_task1, seed_task2, seed_task3, TASKS
conn = sqlite3.connect(":memory:")
seed_task1(conn)
cursor = conn.execute("SELECT COUNT(*) FROM users")
count = cursor.fetchone()[0]
assert count == 5, f"Expected 5, got {count}"
cursor = conn.execute("SELECT last_name FROM users WHERE id=1")
name = cursor.fetchone()[0]
assert name == "O'Brien", f"Expected O'Brien, got {name}"
conn.close()
print("PASS: Task 1 seeds - 5 rows, apostrophe preserved")

# Test 3: Task 2 seeds
conn = sqlite3.connect(":memory:")
seed_task2(conn)
cursor = conn.execute("SELECT COUNT(*) FROM purchases")
assert cursor.fetchone()[0] == 7
conn.close()
print("PASS: Task 2 seeds - 7 rows")

# Test 4: Task 3 seeds
conn = sqlite3.connect(":memory:")
seed_task3(conn)
cursor = conn.execute("SELECT COUNT(*) FROM employees")
assert cursor.fetchone()[0] == 5
cursor = conn.execute("SELECT salary FROM employees WHERE id=5")
assert cursor.fetchone()[0] is None
conn.close()
print("PASS: Task 3 seeds - 5 employees, NULL salary")

# Test 5: Grader
from server.grader import StateReconciler
conn = sqlite3.connect(":memory:")
seed_task1(conn)
reconciler = StateReconciler("column-restructure")
score = reconciler.score(conn)
print(f"PASS: Grader score for unmodified Task 1: {score:.2f}")

# Simulate correct migration
conn.execute("CREATE TABLE users_new (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)")
conn.execute("INSERT INTO users_new (id, full_name) SELECT id, first_name || ' ' || last_name FROM users")
conn.execute("DROP TABLE users")
conn.execute("ALTER TABLE users_new RENAME TO users")
conn.commit()
score = reconciler.score(conn)
print(f"PASS: Score after correct Task 1: {score:.2f}")
assert score == 1.0, f"Expected 1.0, got {score}"
conn.close()

# Test 6: Full environment
from server.environment import DbMigrationEnvironment
env = DbMigrationEnvironment(task_name="column-restructure")
obs = env.reset()
assert obs.done == False
assert obs.step_number == 0
assert "users" in obs.current_schema_sql
print(f"PASS: Environment reset. Step={obs.step_number}")

# Run a complete correct migration
steps = [
    "CREATE TABLE users_new (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)",
    "INSERT INTO users_new (id, full_name) SELECT id, first_name || ' ' || last_name FROM users",
    "DROP TABLE users",
    "ALTER TABLE users_new RENAME TO users",
]
for i, sql in enumerate(steps):
    is_final = (i == len(steps) - 1)
    action = MigrationAction(
        sql_command=sql,
        reasoning=f"Step {i+1}",
        submit_final=is_final,
    )
    obs = env.step(action)
    print(f"  Step {i+1}: reward={obs.reward:.2f}, progress={obs.migration_progress:.2f}, done={obs.done}")

assert obs.done == True
assert obs.migration_progress == 1.0, f"Expected 1.0, got {obs.migration_progress}"
env.close()
print("PASS: Full migration episode completed with score 1.0")

# Test 7: Task 2 grader
conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA foreign_keys = ON")
seed_task2(conn)
reconciler2 = StateReconciler("table-normalization")
score_before = reconciler2.score(conn)
print(f"PASS: Task 2 grader before migration: {score_before:.2f}")
conn.close()

# Test 8: Task 3 grader
conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA foreign_keys = ON")
seed_task3(conn)
reconciler3 = StateReconciler("cascade-migration")
score_before = reconciler3.score(conn)
print(f"PASS: Task 3 grader before migration: {score_before:.2f}")
conn.close()

print()
print("=" * 50)
print("ALL TESTS PASSED! Environment is fully working!")
print("=" * 50)
