"""Test all 7 tasks: seed, golden migration, grade, reset, close."""
import sys
import os
import sqlite3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'OpenEnv', 'src'))

import seeds
from server.grader import StateReconciler
from server.environment import DbMigrationEnvironment
from models import MigrationAction


def test_golden_migration(task_name: str) -> None:
    """Test that golden migration produces a near-perfect grader score."""
    config = seeds.TASKS[task_name]
    
    # 1. Create DB and seed
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    config["seed_fn"](conn)
    
    # 2. Score before migration (should be low)
    reconciler = StateReconciler(task_name)
    score_before = reconciler.score(conn)
    
    # 3. Run golden migration
    config["golden_fn"](conn)
    
    # 4. Score after migration (should be >0.90)
    score_after = reconciler.score(conn)
    
    conn.close()
    
    status = "PASS" if score_after >= 0.90 else "FAIL"
    print(f"  [{status}] {task_name}: before={score_before:.2f} after={score_after:.2f}")
    
    if score_after < 0.90:
        raise AssertionError(f"{task_name}: golden migration only scored {score_after:.2f}")


def test_environment_lifecycle(task_name: str) -> None:
    """Test that environment can reset, step, and close without crashes."""
    env = DbMigrationEnvironment(task_name=task_name)
    obs = env.reset()
    
    assert not obs.done, f"{task_name}: obs.done should be False after reset"
    assert obs.step_number == 0, f"{task_name}: step should be 0 after reset"
    assert obs.current_schema_sql, f"{task_name}: should have current schema"
    assert obs.target_schema_sql, f"{task_name}: should have target schema"
    
    # Run a SELECT to verify data passthrough
    action = MigrationAction(
        sql_command="SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
        reasoning="List tables",
        submit_final=False,
    )
    obs = env.step(action)
    assert "rows total" in obs.last_execution_result or "Query returned" in obs.last_execution_result, \
        f"{task_name}: SELECT should return formatted data, got: {obs.last_execution_result[:100]}"
    
    env.close()
    print(f"  [PASS] {task_name}: environment lifecycle OK (SELECT data passthrough verified)")


def main():
    print("=" * 60)
    print("Testing Golden Migrations (all 7 tasks)")
    print("=" * 60)
    
    errors = []
    for task_name in seeds.TASKS:
        try:
            test_golden_migration(task_name)
        except Exception as e:
            errors.append(f"Golden {task_name}: {e}")
    
    print()
    print("=" * 60)
    print("Testing Environment Lifecycle (all 7 tasks)")
    print("=" * 60)
    
    for task_name in seeds.TASKS:
        try:
            test_environment_lifecycle(task_name)
        except Exception as e:
            errors.append(f"Lifecycle {task_name}: {e}")
    
    print()
    if errors:
        print("=" * 60)
        print(f"FAILURES ({len(errors)}):")
        for e in errors:
            print(f"  ✗ {e}")
        print("=" * 60)
        sys.exit(1)
    else:
        print("=" * 60)
        print("ALL 7 TASKS PASSED!")
        print("=" * 60)


if __name__ == "__main__":
    main()
