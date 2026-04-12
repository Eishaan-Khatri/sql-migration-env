"""Smoke test for the SQL Migration Environment (updated for Golden DB grader)."""
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

# Test 5: Golden migrations run without error
from seeds import golden_task1, golden_task2, golden_task3, golden_task4, golden_task5, golden_task6, golden_task7
for i, (seed_fn, golden_fn, name) in enumerate([
    (seed_task1, golden_task1, "column-restructure"),
    (seed_task2, golden_task2, "table-normalization"),
    (seed_task3, golden_task3, "cascade-migration"),
], 1):
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    seed_fn(conn)
    golden_fn(conn)
    conn.close()
    print(f"PASS: Golden migration {name} runs without error")

# Test 6: Grader with Golden DB
from server.grader import StateReconciler
conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA foreign_keys = ON")
seed_task1(conn)
reconciler = StateReconciler("column-restructure")
score = reconciler.score(conn)
print(f"PASS: Grader score for unmodified Task 1: {score:.2f}")
assert score < 0.7, f"Expected moderate score before migration, got {score}"

# Simulate correct migration
conn.execute("CREATE TABLE users_new (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)")
conn.execute("INSERT INTO users_new (id, full_name) SELECT id, first_name || ' ' || last_name FROM users")
conn.execute("DROP TABLE users")
conn.execute("ALTER TABLE users_new RENAME TO users")
conn.commit()
score = reconciler.score(conn)
print(f"PASS: Score after correct Task 1: {score:.2f}")
assert score >= 0.89, f"Expected >= 0.89, got {score}"
conn.close()

# Test 7: Full environment with SELECT passthrough
from server.environment import DbMigrationEnvironment
env = DbMigrationEnvironment(task_name="column-restructure")
obs = env.reset()
assert obs.done == False
assert obs.step_number == 0
assert "users" in obs.current_schema_sql.lower()
print(f"PASS: Environment reset. Step={obs.step_number}")

# Test SELECT returns actual data (A1 fix)
select_action = MigrationAction(
    sql_command="SELECT * FROM users LIMIT 2",
    reasoning="Inspecting data",
    submit_final=False,
)
obs = env.step(select_action)
assert "O'Brien" in obs.last_execution_result, f"SELECT should return data, got: {obs.last_execution_result}"
print(f"PASS: SELECT returns actual data rows")

# Test dangerous SQL is blocked (A3 fix)
dangerous_action = MigrationAction(
    sql_command="ATTACH DATABASE ':memory:' AS evil",
    reasoning="Testing security",
    submit_final=False,
)
obs = env.step(dangerous_action)
assert "not allowed" in obs.last_execution_result.lower() or "blocked" in obs.last_execution_result.lower(), \
    f"ATTACH should be blocked, got: {obs.last_execution_result}"
print(f"PASS: Dangerous SQL is blocked")

# Run a complete correct migration
env2 = DbMigrationEnvironment(task_name="column-restructure")
obs2 = env2.reset()
steps = [
    "CREATE TABLE users_new (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)",
    "INSERT INTO users_new (id, full_name) SELECT id, first_name || ' ' || last_name FROM users",
    "DROP TABLE users",
    "ALTER TABLE users_new RENAME TO users",
]
for i, sql in enumerate(steps):
    is_final = (i == len(steps) - 1)
    action = MigrationAction(sql_command=sql, reasoning=f"Step {i+1}", submit_final=is_final)
    obs2 = env2.step(action)
    print(f"  Step {i+1}: reward={obs2.reward:.2f}, progress={obs2.migration_progress:.2f}, done={obs2.done}")

assert obs2.done == True
assert obs2.migration_progress >= 0.89, f"Expected >= 0.89, got {obs2.migration_progress}"
# Check trajectory is included in final metadata
assert "trajectory" in obs2.metadata, "Trajectory should be in final metadata"
print(f"PASS: Full migration completed with score {obs2.migration_progress:.2f}")

env.close()
env2.close()

# Test 8: Task 2 grader
conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA foreign_keys = ON")
seed_task2(conn)
reconciler2 = StateReconciler("table-normalization")
score_before = reconciler2.score(conn)
print(f"PASS: Task 2 grader before migration: {score_before:.2f}")
conn.close()

# Test 9: Task 3 grader
conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA foreign_keys = ON")
seed_task3(conn)
reconciler3 = StateReconciler("cascade-migration")
score_before = reconciler3.score(conn)
print(f"PASS: Task 3 grader before migration: {score_before:.2f}")
conn.close()

# Test 10: Case insensitivity (A7)
conn = sqlite3.connect(":memory:")
conn.execute("PRAGMA foreign_keys = ON")
seed_task1(conn)
conn.execute("CREATE TABLE USERS_NEW (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)")
conn.execute("INSERT INTO USERS_NEW SELECT id, first_name || ' ' || last_name FROM users")
conn.execute("DROP TABLE users")
conn.execute("ALTER TABLE USERS_NEW RENAME TO USERS")
conn.commit()
reconciler_case = StateReconciler("column-restructure")
score_case = reconciler_case.score(conn)
print(f"PASS: Case-insensitive grading score: {score_case:.2f}")
assert score_case >= 0.79, f"Case-insensitive should score high, got {score_case}"
conn.close()

print()
print("=" * 50)
print("ALL TESTS PASSED! Environment is fully working!")
print("=" * 50)
