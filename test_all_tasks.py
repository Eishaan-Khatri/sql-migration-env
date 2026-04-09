"""Quick validation of all 7 tasks: seeds + graders."""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from seeds import TASKS
from server.grader import StateReconciler

print(f"Tasks registered: {len(TASKS)}")
assert len(TASKS) == 7, f"Expected 7 tasks, got {len(TASKS)}"
print(f"  Names: {list(TASKS.keys())}")

for name, cfg in TASKS.items():
    # Seed
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    cfg["seed_fn"](conn)
    
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [r[0] for r in cursor.fetchall()]
    print(f"\n[{name}] ({cfg['difficulty']}, max_steps={cfg.get('max_steps', 20)})")
    print(f"  Tables: {tables}")
    
    # Grade
    reconciler = StateReconciler(name)
    score = reconciler.score(conn)
    assert 0.01 <= score <= 0.99, f"Score {score} out of [0.01, 0.99]!"
    print(f"  Initial score: {score:.2f} OK")
    
    conn.close()

# Also test environment resets for each task
from server.environment import DbMigrationEnvironment

for name in TASKS:
    env = DbMigrationEnvironment(task_name=name)
    obs = env.reset()
    assert obs.done == False
    assert obs.step_number == 0
    print(f"  [{name}] Environment reset OK")
    env.close()

print("\n" + "=" * 50)
print("ALL 7 TASKS VALIDATED SUCCESSFULLY!")
print("=" * 50)
