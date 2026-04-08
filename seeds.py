"""
Seed data for all 3 migration tasks.

EVERY value in this file is a hardcoded constant. No datetime.now(),
no random(), no runtime generation. This guarantees deterministic
grader behavior across every execution.
"""

import sqlite3
from typing import Dict, List, Tuple


# =============================================================================
# TASK 1: Column Restructure (Easy)
# =============================================================================
# Agent must merge first_name + last_name into full_name without data loss.
# Adversarial: O'Brien (apostrophe), McDonald (capital mid-word).

TASK1_SOURCE_DDL = """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL
);
"""

TASK1_SOURCE_DATA = [
    (1, "John", "O'Brien"),
    (2, "Mary", "McDonald"),
    (3, "Alice", "Smith"),
    (4, "Bob", "Jones"),
    (5, "Carol", "White"),
]

TASK1_TARGET_DDL = """CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    full_name TEXT NOT NULL
);"""

TASK1_EXPECTED_ROWS: List[Tuple] = [
    (1, "John O'Brien"),
    (2, "Mary McDonald"),
    (3, "Alice Smith"),
    (4, "Bob Jones"),
    (5, "Carol White"),
]


def seed_task1(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 1: Column Restructure."""
    conn.executescript(TASK1_SOURCE_DDL)
    conn.executemany(
        "INSERT INTO users (id, first_name, last_name) VALUES (?, ?, ?)",
        TASK1_SOURCE_DATA,
    )
    conn.commit()


# =============================================================================
# TASK 2: Table Normalization (Medium)
# =============================================================================
# Agent must split flat purchases table into customers + orders with FK.
# Adversarial: alice@example.com appears 3 times (forces SELECT DISTINCT),
# "Laptop, 15-inch" has a comma (breaks naive CSV parsing).

TASK2_SOURCE_DDL = """
CREATE TABLE purchases (
    purchase_id INTEGER PRIMARY KEY,
    item_name TEXT NOT NULL,
    price INTEGER NOT NULL,
    customer_name TEXT NOT NULL,
    customer_email TEXT NOT NULL
);
"""

TASK2_SOURCE_DATA = [
    (1, "Laptop, 15-inch", 80000, "Alice Smith", "alice@example.com"),
    (2, "Mouse", 2500, "Bob Jones", "bob@example.com"),
    (3, "Keyboard", 4500, "Alice Smith", "alice@example.com"),
    (4, "Monitor", 25000, "Carol White", "carol@example.com"),
    (5, "Webcam", 3500, "Alice Smith", "alice@example.com"),
    (6, "USB Hub", 1500, "Bob Jones", "bob@example.com"),
    (7, "Headphones", 6000, "Carol White", "carol@example.com"),
]

TASK2_TARGET_DDL = """CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    item_name TEXT NOT NULL,
    price INTEGER NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);"""

TASK2_EXPECTED_CUSTOMER_COUNT = 3
TASK2_EXPECTED_ORDER_COUNT = 7


def seed_task2(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 2: Table Normalization."""
    conn.executescript(TASK2_SOURCE_DDL)
    conn.executemany(
        "INSERT INTO purchases (purchase_id, item_name, price, customer_name, customer_email) "
        "VALUES (?, ?, ?, ?, ?)",
        TASK2_SOURCE_DATA,
    )
    conn.commit()


# =============================================================================
# TASK 3: Cascade Migration (Hard)
# =============================================================================
# Agent must fix types, enforce FKs, and handle orphaned/NULL records.
# Adversarial: salary as "$50,000" strings, one NULL salary,
# two orphaned assets referencing nonexistent employees.

TASK3_SOURCE_DDL = """
CREATE TABLE companies (
    id INTEGER PRIMARY KEY,
    name TEXT
);

CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    company_id INTEGER,
    name TEXT
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    department_id INTEGER,
    name TEXT,
    salary TEXT
);

CREATE TABLE assets (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER,
    description TEXT
);
"""

TASK3_COMPANIES_DATA = [
    (1, "Acme Corp"),
    (2, "Globex Inc"),
]

TASK3_DEPARTMENTS_DATA = [
    (1, 1, "Engineering"),
    (2, 1, "Marketing"),
    (3, 2, "Sales"),
]

TASK3_EMPLOYEES_DATA = [
    (1, 1, "Dave Kumar", "$90000"),
    (2, 1, "Eve Chen", "$75000"),
    (3, 2, "Frank O'Neill", "$60000"),
    (4, 3, "Grace Lee", "$85000"),
    (5, 3, "Hal Patel", None),  # NULL salary — violates target NOT NULL
]

TASK3_ASSETS_DATA = [
    (1, 1, "MacBook Pro"),
    (2, 2, "Dell Monitor"),
    (3, 3, "Standing Desk"),
    (4, 99, "Orphaned Laptop"),   # employee_id=99 does not exist
    (5, 100, "Orphaned Chair"),   # employee_id=100 does not exist
]

TASK3_TARGET_DDL = """CREATE TABLE companies (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE departments (
    id INTEGER PRIMARY KEY,
    company_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES companies(id)
);

CREATE TABLE employees (
    id INTEGER PRIMARY KEY,
    department_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    salary INTEGER NOT NULL,
    FOREIGN KEY (department_id) REFERENCES departments(id)
);

CREATE TABLE assets (
    id INTEGER PRIMARY KEY,
    employee_id INTEGER NOT NULL,
    description TEXT NOT NULL,
    FOREIGN KEY (employee_id) REFERENCES employees(id)
);

CREATE TABLE audit_log (
    id INTEGER PRIMARY KEY,
    source_table TEXT NOT NULL,
    original_row_json TEXT NOT NULL,
    reason TEXT NOT NULL
);"""

# Expected audit_log entries: 1 NULL salary employee + 2 orphaned assets = 3 rows
TASK3_EXPECTED_AUDIT_COUNT = 3
TASK3_EXPECTED_AUDIT_ENTRIES = [
    ("employees", "null_salary"),
    ("assets", "orphaned_record"),
    ("assets", "orphaned_record"),
]

# Expected employee salaries after migration (Hal Patel removed)
TASK3_EXPECTED_SALARIES: Dict[int, int] = {
    1: 90000,
    2: 75000,
    3: 60000,
    4: 85000,
}

TASK3_EXPECTED_EMPLOYEE_COUNT = 4


def seed_task3(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 3: Cascade Migration."""
    conn.executescript(TASK3_SOURCE_DDL)
    conn.executemany(
        "INSERT INTO companies (id, name) VALUES (?, ?)",
        TASK3_COMPANIES_DATA,
    )
    conn.executemany(
        "INSERT INTO departments (id, company_id, name) VALUES (?, ?, ?)",
        TASK3_DEPARTMENTS_DATA,
    )
    conn.executemany(
        "INSERT INTO employees (id, department_id, name, salary) VALUES (?, ?, ?, ?)",
        TASK3_EMPLOYEES_DATA,
    )
    conn.executemany(
        "INSERT INTO assets (id, employee_id, description) VALUES (?, ?, ?)",
        TASK3_ASSETS_DATA,
    )
    conn.commit()


# =============================================================================
# Task Registry
# =============================================================================

TASKS = {
    "column-restructure": {
        "seed_fn": seed_task1,
        "target_ddl": TASK1_TARGET_DDL,
        "description": "Merge first_name and last_name into a single full_name column without data loss",
        "difficulty": "easy",
    },
    "table-normalization": {
        "seed_fn": seed_task2,
        "target_ddl": TASK2_TARGET_DDL,
        "description": "Decompose a flat purchases table into normalized customers and orders tables with FK",
        "difficulty": "medium",
    },
    "cascade-migration": {
        "seed_fn": seed_task3,
        "target_ddl": TASK3_TARGET_DDL,
        "description": "Multi-table FK cascade with type coercion, NULL handling, and orphan audit logging",
        "difficulty": "hard",
    },
}
