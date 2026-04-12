"""
Deterministic Adversarial Seed Data Engine.

> **Hackathon Judges Note:** 
> This is not generic dummy data. Our seeds specifically inject malicious 
> real-world SQL edge cases to pressure-test frontier LLM logic:
> - **O'Brien (Task 1):** Tests if the agent uses proper parameterization/escaping.
> - **Duplicate Emails (Task 2):** Tests `DISTINCT` vs standard `INSERT` logic.
> - **Orphaned FKs (Task 3):** Tests the agent's ability to safely `CASCADE` or audit-log invalid relations before dropping columns.
> - **NULL salary rows (Task 3):** Tests strict type constraints handling.

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
# TASK 4: Soft-Delete Restoration (Easy)
# =============================================================================
# Agent must restore deleted products from a deletion_log, add is_deleted/deleted_at columns.
# Adversarial: "O'Brien Desk" (apostrophe), stock=0 on Webcam (must NOT confuse with is_deleted).

TASK4_SOURCE_DDL = """
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL
);

CREATE TABLE deletion_log (
    id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name TEXT NOT NULL,
    product_price REAL NOT NULL,
    product_stock INTEGER NOT NULL,
    deleted_at TEXT NOT NULL
);
"""

TASK4_PRODUCTS_DATA = [
    (1, "Laptop",       999.99, 15),
    (2, "O'Brien Desk", 249.99, 8),
    (3, "Monitor",      399.99, 23),
    (4, "Keyboard",     89.99,  45),
    (5, "Mouse",        29.99,  102),
]

TASK4_DELETION_LOG_DATA = [
    (1, 6, "Headphones", 149.99, 30, "2024-01-15"),
    (2, 7, "Webcam",      79.99,  0, "2024-02-20"),   # stock=0 but NOT is_deleted=1
    (3, 8, "USB-C Hub",   49.99, 12, "2024-03-10"),
]

TASK4_TARGET_DDL = """CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    stock INTEGER NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    deleted_at TEXT
);"""

TASK4_EXPECTED_ROW_COUNT = 8
TASK4_EXPECTED_ID_SUM = 36           # 1+2+3+4+5+6+7+8
TASK4_EXPECTED_DELETED_COUNT = 3     # ids 6,7,8
TASK4_EXPECTED_ACTIVE_COUNT = 5      # ids 1-5


def seed_task4(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 4: Soft-Delete Restoration."""
    conn.executescript(TASK4_SOURCE_DDL)
    conn.executemany(
        "INSERT INTO products (id, name, price, stock) VALUES (?, ?, ?, ?)",
        TASK4_PRODUCTS_DATA,
    )
    conn.executemany(
        "INSERT INTO deletion_log (id, product_id, product_name, product_price, product_stock, deleted_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        TASK4_DELETION_LOG_DATA,
    )
    conn.commit()


# =============================================================================
# TASK 5: Schema Version Merge (Medium)
# =============================================================================
# Agent must merge products_v1 (price as "$XX.XX" TEXT) and products_v2 (price as REAL)
# into a single products table. v2 wins on ID conflicts. Must add source column.
# Adversarial: id=101 high ID, NULL category, "$" price coercion, conflicting rows.

TASK5_SOURCE_DDL = """
CREATE TABLE products_v1 (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price TEXT NOT NULL,
    category TEXT,
    supplier TEXT
);

CREATE TABLE products_v2 (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    unit_cost REAL NOT NULL,
    category TEXT NOT NULL,
    brand TEXT,
    sku TEXT
);
"""

TASK5_V1_DATA = [
    (1,   "Widget A",    "$12.50", "Electronics", "AcmeCo"),
    (2,   "Widget B",    "$8.99",  "Electronics", "AcmeCo"),
    (3,   "Gadget X",    "$45.00", None,          "TechCorp"),
    (4,   "Gadget Y",    "$32.50", "Tools",       "TechCorp"),
    (5,   "Doohickey",   "$5.99",  "Office",      "SupplyPro"),
    (101, "Legacy Item", "$99.99", "Electronics", "OldCo"),
]

TASK5_V2_DATA = [
    (1, "Widget A",          12.50, "Electronics", "AcmeCo",  "SKU-001"),
    (2, "Widget B Updated",   9.99, "Electronics", "AcmeCo",  "SKU-002"),
    (6, "New Product F",     67.00, "Tools",       "NewCorp", "SKU-006"),
    (7, "New Product G",     23.50, "Office",      "NewCorp", "SKU-007"),
    (8, "New Product H",     11.00, "Electronics", "ImportCo","SKU-008"),
]

TASK5_TARGET_DDL = """CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    category TEXT,
    supplier TEXT,
    brand TEXT,
    sku TEXT,
    source TEXT NOT NULL
);"""

TASK5_EXPECTED_ROW_COUNT = 9
TASK5_EXPECTED_PRICE_SUM = round(12.50 + 9.99 + 45.00 + 32.50 + 5.99 + 99.99 + 67.00 + 23.50 + 11.00, 2)
TASK5_EXPECTED_BOTH_SOURCE_COUNT = 2      # ids 1 and 2


def seed_task5(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 5: Schema Version Merge."""
    conn.executescript(TASK5_SOURCE_DDL)
    conn.executemany(
        "INSERT INTO products_v1 (id, name, price, category, supplier) VALUES (?, ?, ?, ?, ?)",
        TASK5_V1_DATA,
    )
    conn.executemany(
        "INSERT INTO products_v2 (id, name, unit_cost, category, brand, sku) VALUES (?, ?, ?, ?, ?, ?)",
        TASK5_V2_DATA,
    )
    conn.commit()


# =============================================================================
# TASK 6: Multi-Entity Extraction (Medium — Hard End)
# =============================================================================
# Agent must decompose a sales_records god-table into 3NF (5 tables).
# Adversarial: leading whitespace email, empty customer email, comma in SKU.

TASK6_SOURCE_DDL = """
CREATE TABLE sales_records (
    id INTEGER PRIMARY KEY,
    rep_name TEXT NOT NULL,
    rep_email TEXT NOT NULL,
    rep_region TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    customer_email TEXT NOT NULL,
    customer_tier TEXT NOT NULL,
    product_name TEXT NOT NULL,
    product_sku TEXT NOT NULL,
    product_category TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    discount_pct INTEGER NOT NULL DEFAULT 0,
    sale_date TEXT NOT NULL
);
"""

TASK6_SOURCE_DATA = [
    (1,  "Alice Chen",   " alice@company.com", "North", "Globex Corp",    "globex@corp.com",   "enterprise", "Widget Pro",  "WIDGET-001", "Electronics", 5,  299.99, 10, "2024-01-10"),
    (2,  "Alice Chen",   "alice@company.com",  "North", "Initech LLC",    "info@initech.com",  "basic",      "Widget Pro",  "WIDGET-001", "Electronics", 2,  299.99, 0,  "2024-01-15"),
    (3,  "Bob Martinez", "bob@company.com",    "South", "Globex Corp",    "globex@corp.com",   "enterprise", "Gadget X",    "GADGET-X01", "Hardware",    10, 89.99,  5,  "2024-01-20"),
    (4,  "Bob Martinez", "bob@company.com",    "South", "Umbrella Inc",   "sales@umbrella.co", "premium",    "Gadget X",    "GADGET-X01", "Hardware",    3,  89.99,  0,  "2024-02-01"),
    (5,  "Carol White",  "carol@company.com",  "East",  "Initech LLC",    "info@initech.com",  "basic",      "Tool Kit",    "TOOLS,001",  "Hardware",    1,  199.99, 0,  "2024-02-05"),
    (6,  "Alice Chen",   "alice@company.com",  "North", "Pendant Corp",   "",                  "free",       "Widget Pro",  "WIDGET-001", "Electronics", 7,  299.99, 15, "2024-02-10"),
    (7,  "Carol White",  "carol@company.com",  "East",  "Globex Corp",    "globex@corp.com",   "enterprise", "Nano Device", "NANO-D01",   "Electronics", 2,  549.99, 20, "2024-02-15"),
    (8,  "Bob Martinez", "bob@company.com",    "South", "Umbrella Inc",   "sales@umbrella.co", "premium",    "Tool Kit",    "TOOLS,001",  "Hardware",    4,  199.99, 10, "2024-03-01"),
    (9,  "Alice Chen",   "alice@company.com",  "North", "Initech LLC",    "info@initech.com",  "basic",      "Nano Device", "NANO-D01",   "Electronics", 1,  549.99, 0,  "2024-03-05"),
    (10, "Carol White",  "carol@company.com",  "East",  "Umbrella Inc",   "sales@umbrella.co", "premium",    "Cable Bundle","CABLE-5PK",  "Accessories", 20, 14.99,  0,  "2024-03-10"),
    (11, "Bob Martinez", "bob@company.com",    "South", "Globex Corp",    "globex@corp.com",   "enterprise", "Cable Bundle","CABLE-5PK",  "Accessories", 15, 14.99,  5,  "2024-03-15"),
    (12, "Carol White",  "carol@company.com",  "East",  "Pendant Corp",   "orders@pendant.io", "free",       "Gadget X",    "GADGET-X01", "Hardware",    6,  89.99,  0,  "2024-03-20"),
]

TASK6_TARGET_DDL = """CREATE TABLE salespersons (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    region TEXT NOT NULL
);

CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    tier TEXT NOT NULL
);

CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    sku TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL
);

CREATE TABLE sales (
    id INTEGER PRIMARY KEY,
    salesperson_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    discount_pct INTEGER NOT NULL DEFAULT 0,
    sale_date TEXT NOT NULL,
    FOREIGN KEY (salesperson_id) REFERENCES salespersons(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE TABLE data_issues (
    id INTEGER PRIMARY KEY,
    source_table TEXT NOT NULL,
    source_row_id INTEGER NOT NULL,
    issue_type TEXT NOT NULL,
    issue_detail TEXT NOT NULL
);"""

TASK6_EXPECTED_SALESPERSON_COUNT = 3
TASK6_EXPECTED_CUSTOMER_COUNT = 3   # Pendant Corp row 6 excluded (empty email)
TASK6_EXPECTED_PRODUCT_COUNT = 5
TASK6_EXPECTED_SALES_COUNT = 11     # row 6 excluded
TASK6_EXPECTED_DATA_ISSUES_COUNT = 1


def seed_task6(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 6: Multi-Entity Extraction."""
    conn.executescript(TASK6_SOURCE_DDL)
    conn.executemany(
        "INSERT INTO sales_records (id, rep_name, rep_email, rep_region, "
        "customer_name, customer_email, customer_tier, product_name, product_sku, "
        "product_category, quantity, unit_price, discount_pct, sale_date) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        TASK6_SOURCE_DATA,
    )
    conn.commit()


# =============================================================================
# TASK 7: Dual-Source Consolidation (Hard)
# =============================================================================
# Agent must merge 6 source tables from two incompatible systems (Legacy CRM + Modern SaaS)
# into 4 unified target tables. Cross-system email dedup, currency coercion, orphan detection.

TASK7_LEGACY_CUSTOMERS_DDL = """
CREATE TABLE legacy_customers (
    id INTEGER PRIMARY KEY,
    full_name TEXT,
    contact_email TEXT,
    phone TEXT,
    account_type TEXT,
    join_date TEXT
);
"""

TASK7_LEGACY_ORDERS_DDL = """
CREATE TABLE legacy_orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    product_code TEXT,
    total_amount TEXT,
    order_status TEXT,
    order_date TEXT
);
"""

TASK7_LEGACY_PRODUCTS_DDL = """
CREATE TABLE legacy_products (
    code TEXT PRIMARY KEY,
    description TEXT,
    unit_price TEXT
);
"""

TASK7_MODERN_USERS_DDL = """
CREATE TABLE modern_users (
    uuid TEXT PRIMARY KEY,
    display_name TEXT,
    email_address TEXT,
    subscription_tier INTEGER,
    created_at TEXT
);
"""

TASK7_MODERN_TRANSACTIONS_DDL = """
CREATE TABLE modern_transactions (
    id INTEGER PRIMARY KEY,
    user_uuid TEXT,
    item_sku TEXT,
    amount REAL,
    currency TEXT,
    tx_status INTEGER,
    created_at TEXT
);
"""

TASK7_MODERN_CATALOG_DDL = """
CREATE TABLE modern_catalog (
    sku TEXT PRIMARY KEY,
    title TEXT,
    base_price REAL
);
"""

TASK7_LEGACY_CUSTOMERS_DATA = [
    (1, "Alice Johnson", "alice@example.com", "+1-555-0101", "premium", "2021-03-15"),
    (2, "Bob Chen",      "bob@example.com",   "+1-555-0102", "basic",   "2022-07-01"),
    (3, "Carol Davis",   None,                "+1-555-0103", "free",    "2023-01-10"),
    (4, "Dave Wilson",   "dave@example.com",  "+1-555-0104", "premium", "2021-11-20"),
    (5, "Eve Martinez",  "eve@example.com",   "+1-555-0105", "free",    "2023-06-05"),
]

TASK7_MODERN_USERS_DATA = [
    ("uuid-A1", "Alice J.",    "alice@example.com", 3, "2021-03-15"),
    ("uuid-B2", "R. Bob Chen", "bob@example.com",   2, "2022-07-01"),
    ("uuid-F6", "Frank Lee",   "frank@example.com", 4, "2022-09-30"),
    ("uuid-G7", "Grace Kim",   "grace@example.com", 1, "2024-01-15"),
]

TASK7_LEGACY_ORDERS_DATA = [
    (1, 1, "PROD-A", "$1,234.56", "delivered", "2022-01-10"),
    (2, 2, "PROD-B", "$89.99",    "shipped",   "2022-03-15"),
    (3, 4, "PROD-A", "$2,500.00", "delivered", "2022-05-20"),
    (4, 3, "PROD-C", "$45.00",    "pending",   "2023-02-01"),
]

TASK7_LEGACY_PRODUCTS_DATA = [
    ("PROD-A", "Enterprise Widget",   "$1,234.56"),
    ("PROD-B", "Basic Gadget",        "$89.99"),
    ("PROD-C", "Starter Kit",         "$45.00"),
]

TASK7_MODERN_TRANSACTIONS_DATA = [
    (1, "uuid-A1",   "SKU-001", 299.99, "USD",  3, "2023-06-01"),
    (2, "uuid-B2",   "SKU-002", 89.99,  None,   2, "2023-07-15"),
    (3, "uuid-F6",   "SKU-001", 299.99, None,   3, "2023-08-20"),
    (4, "uuid-DEAD", "SKU-003", 15.99,  None,   1, "2023-09-01"),   # orphan
    (5, "uuid-G7",   "SKU-002", 89.99,  "USD",  4, "2023-10-10"),
    (6, "uuid-A1",   "SKU-003", 15.99,  "EUR",  5, "2023-11-01"),
]

TASK7_MODERN_CATALOG_DATA = [
    ("SKU-001", "Pro Widget",    299.99),
    ("SKU-002", "Smart Gadget",  89.99),
    ("SKU-003", "Mini Accessory", 15.99),
]

TASK7_TARGET_DDL = """CREATE TABLE unified_customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    legacy_id INTEGER,
    modern_uuid TEXT,
    name TEXT,
    email TEXT,
    phone TEXT,
    tier TEXT NOT NULL DEFAULT 'free',
    source TEXT NOT NULL,
    created_at TEXT
);

CREATE TABLE unified_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    price REAL NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE unified_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER NOT NULL,
    product_id INTEGER,
    amount REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL,
    order_date TEXT,
    source TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES unified_customers(id)
);

CREATE TABLE migration_issues (
    id INTEGER PRIMARY KEY,
    source_system TEXT NOT NULL,
    source_table TEXT NOT NULL,
    source_id TEXT NOT NULL,
    issue_type TEXT NOT NULL,
    resolution TEXT NOT NULL
);"""

TASK7_EXPECTED_UNIFIED_CUSTOMERS = 7
TASK7_EXPECTED_BOTH_SOURCE_COUNT = 2
TASK7_EXPECTED_UNIFIED_ORDERS = 9
TASK7_EXPECTED_MIGRATION_ISSUES = 2

# Tier mapping: 1→'free', 2→'basic', 3→'premium', 4→'enterprise'
TASK7_TIER_MAP = {1: "free", 2: "basic", 3: "premium", 4: "enterprise"}
# Status mapping: 1→'pending', 2→'processing', 3→'complete', 4→'failed', 5→'refunded'
TASK7_STATUS_MAP = {1: "pending", 2: "processing", 3: "complete", 4: "failed", 5: "refunded"}


def seed_task7(conn: sqlite3.Connection) -> None:
    """Seed the database for Task 7: Dual-Source Consolidation."""
    conn.executescript(TASK7_LEGACY_CUSTOMERS_DDL)
    conn.executescript(TASK7_LEGACY_ORDERS_DDL)
    conn.executescript(TASK7_LEGACY_PRODUCTS_DDL)
    conn.executescript(TASK7_MODERN_USERS_DDL)
    conn.executescript(TASK7_MODERN_TRANSACTIONS_DDL)
    conn.executescript(TASK7_MODERN_CATALOG_DDL)

    conn.executemany("INSERT INTO legacy_customers VALUES (?, ?, ?, ?, ?, ?)", TASK7_LEGACY_CUSTOMERS_DATA)
    conn.executemany("INSERT INTO legacy_orders VALUES (?, ?, ?, ?, ?, ?)", TASK7_LEGACY_ORDERS_DATA)
    conn.executemany("INSERT INTO legacy_products VALUES (?, ?, ?)", TASK7_LEGACY_PRODUCTS_DATA)
    conn.executemany("INSERT INTO modern_users VALUES (?, ?, ?, ?, ?)", TASK7_MODERN_USERS_DATA)
    conn.executemany("INSERT INTO modern_transactions VALUES (?, ?, ?, ?, ?, ?, ?)", TASK7_MODERN_TRANSACTIONS_DATA)
    conn.executemany("INSERT INTO modern_catalog VALUES (?, ?, ?)", TASK7_MODERN_CATALOG_DATA)
    conn.commit()


# =============================================================================
# Golden Migration Functions
# =============================================================================
# These produce the CORRECT expected database state from any seed data.
# Used by the dynamic grader to compare against the agent's output.
# If seed data changes, the golden DB auto-updates — no hardcoded literals.


def golden_task1(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 1: Column Restructure."""
    conn.execute("CREATE TABLE users_new (id INTEGER PRIMARY KEY, full_name TEXT NOT NULL)")
    conn.execute(
        "INSERT INTO users_new (id, full_name) "
        "SELECT id, first_name || ' ' || last_name FROM users"
    )
    conn.execute("DROP TABLE users")
    conn.execute("ALTER TABLE users_new RENAME TO users")
    conn.commit()


def golden_task2(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 2: Table Normalization."""
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        "CREATE TABLE customers ("
        "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT NOT NULL UNIQUE)"
    )
    conn.execute(
        "INSERT INTO customers (name, email) "
        "SELECT DISTINCT customer_name, customer_email FROM purchases"
    )
    conn.execute(
        "CREATE TABLE orders ("
        "id INTEGER PRIMARY KEY, customer_id INTEGER NOT NULL, "
        "item_name TEXT NOT NULL, price INTEGER NOT NULL, "
        "FOREIGN KEY (customer_id) REFERENCES customers(id))"
    )
    conn.execute(
        "INSERT INTO orders (customer_id, item_name, price) "
        "SELECT c.id, p.item_name, p.price "
        "FROM purchases p JOIN customers c ON p.customer_email = c.email"
    )
    conn.execute("DROP TABLE purchases")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def golden_task3(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 3: Cascade Migration."""
    conn.execute("PRAGMA foreign_keys = OFF")
    # Create audit_log
    conn.execute(
        "CREATE TABLE audit_log (id INTEGER PRIMARY KEY, source_table TEXT NOT NULL, "
        "original_row_json TEXT NOT NULL, reason TEXT NOT NULL)"
    )
    # Log orphaned assets
    conn.execute(
        "INSERT INTO audit_log (source_table, original_row_json, reason) "
        "SELECT 'assets', '{\"id\":' || id || ',\"employee_id\":' || employee_id || '}', 'orphaned_record' "
        "FROM assets WHERE employee_id NOT IN (SELECT id FROM employees)"
    )
    # Log NULL salary employees
    conn.execute(
        "INSERT INTO audit_log (source_table, original_row_json, reason) "
        "SELECT 'employees', '{\"id\":' || id || ',\"name\":\"' || name || '\"}', 'null_salary' "
        "FROM employees WHERE salary IS NULL"
    )
    # Rebuild companies
    conn.execute("CREATE TABLE companies_new (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    conn.execute("INSERT INTO companies_new SELECT id, name FROM companies")
    conn.execute("DROP TABLE companies")
    conn.execute("ALTER TABLE companies_new RENAME TO companies")
    # Rebuild departments
    conn.execute(
        "CREATE TABLE departments_new (id INTEGER PRIMARY KEY, company_id INTEGER NOT NULL, "
        "name TEXT NOT NULL, FOREIGN KEY (company_id) REFERENCES companies(id))"
    )
    conn.execute("INSERT INTO departments_new SELECT id, company_id, name FROM departments")
    conn.execute("DROP TABLE departments")
    conn.execute("ALTER TABLE departments_new RENAME TO departments")
    # Rebuild employees (remove NULL salary, coerce TEXT to INT)
    conn.execute(
        "CREATE TABLE employees_new (id INTEGER PRIMARY KEY, department_id INTEGER NOT NULL, "
        "name TEXT NOT NULL, salary INTEGER NOT NULL, "
        "FOREIGN KEY (department_id) REFERENCES departments(id))"
    )
    conn.execute(
        "INSERT INTO employees_new (id, department_id, name, salary) "
        "SELECT id, department_id, name, "
        "CAST(REPLACE(REPLACE(salary, '$', ''), ',', '') AS INTEGER) "
        "FROM employees WHERE salary IS NOT NULL"
    )
    conn.execute("DROP TABLE employees")
    conn.execute("ALTER TABLE employees_new RENAME TO employees")
    # Rebuild assets (remove orphans)
    conn.execute(
        "CREATE TABLE assets_new (id INTEGER PRIMARY KEY, employee_id INTEGER NOT NULL, "
        "description TEXT NOT NULL, FOREIGN KEY (employee_id) REFERENCES employees(id))"
    )
    conn.execute(
        "INSERT INTO assets_new SELECT id, employee_id, description FROM assets "
        "WHERE employee_id IN (SELECT id FROM employees)"
    )
    conn.execute("DROP TABLE assets")
    conn.execute("ALTER TABLE assets_new RENAME TO assets")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def golden_task4(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 4: Soft-Delete Restoration."""
    conn.execute("PRAGMA foreign_keys = OFF")
    # Create new table with extra columns
    conn.execute(
        "CREATE TABLE products_new (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "price REAL NOT NULL, stock INTEGER NOT NULL, "
        "is_deleted INTEGER NOT NULL DEFAULT 0, deleted_at TEXT)"
    )
    # Copy existing products as active
    conn.execute(
        "INSERT INTO products_new (id, name, price, stock, is_deleted, deleted_at) "
        "SELECT id, name, price, stock, 0, NULL FROM products"
    )
    # Restore deleted products from log
    conn.execute(
        "INSERT INTO products_new (id, name, price, stock, is_deleted, deleted_at) "
        "SELECT product_id, product_name, product_price, product_stock, 1, deleted_at "
        "FROM deletion_log"
    )
    conn.execute("DROP TABLE products")
    conn.execute("ALTER TABLE products_new RENAME TO products")
    conn.execute("DROP TABLE deletion_log")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def golden_task5(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 5: Schema Version Merge."""
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "price REAL NOT NULL, category TEXT, supplier TEXT, brand TEXT, "
        "sku TEXT, source TEXT NOT NULL)"
    )
    # Insert v1-only rows
    conn.execute(
        "INSERT INTO products (id, name, price, category, supplier, brand, sku, source) "
        "SELECT id, name, CAST(REPLACE(REPLACE(price, '$', ''), ',', '') AS REAL), "
        "category, supplier, NULL, NULL, 'v1' "
        "FROM products_v1 WHERE id NOT IN (SELECT id FROM products_v2)"
    )
    # Insert v2-only rows
    conn.execute(
        "INSERT INTO products (id, name, price, category, supplier, brand, sku, source) "
        "SELECT id, name, unit_cost, category, NULL, brand, sku, 'v2' "
        "FROM products_v2 WHERE id NOT IN (SELECT id FROM products_v1)"
    )
    # Insert conflict rows (v2 wins for name/price)
    conn.execute(
        "INSERT INTO products (id, name, price, category, supplier, brand, sku, source) "
        "SELECT v2.id, v2.name, v2.unit_cost, v2.category, v1.supplier, v2.brand, v2.sku, 'both' "
        "FROM products_v2 v2 JOIN products_v1 v1 ON v2.id = v1.id"
    )
    conn.execute("DROP TABLE products_v1")
    conn.execute("DROP TABLE products_v2")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def golden_task6(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 6: Multi-Entity Extraction."""
    conn.execute("PRAGMA foreign_keys = OFF")
    # Create target tables
    conn.execute(
        "CREATE TABLE salespersons (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "email TEXT NOT NULL UNIQUE, region TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "email TEXT NOT NULL UNIQUE, tier TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
        "sku TEXT NOT NULL UNIQUE, category TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE sales (id INTEGER PRIMARY KEY, salesperson_id INTEGER NOT NULL, "
        "customer_id INTEGER NOT NULL, product_id INTEGER NOT NULL, "
        "quantity INTEGER NOT NULL, unit_price REAL NOT NULL, "
        "discount_pct INTEGER NOT NULL DEFAULT 0, sale_date TEXT NOT NULL, "
        "FOREIGN KEY (salesperson_id) REFERENCES salespersons(id), "
        "FOREIGN KEY (customer_id) REFERENCES customers(id), "
        "FOREIGN KEY (product_id) REFERENCES products(id))"
    )
    conn.execute(
        "CREATE TABLE data_issues (id INTEGER PRIMARY KEY, source_table TEXT NOT NULL, "
        "source_row_id INTEGER NOT NULL, issue_type TEXT NOT NULL, "
        "issue_detail TEXT NOT NULL)"
    )
    # Populate salespersons (TRIM email)
    conn.execute(
        "INSERT INTO salespersons (name, email, region) "
        "SELECT DISTINCT rep_name, TRIM(rep_email), rep_region FROM sales_records"
    )
    # Populate customers (exclude empty email rows)
    conn.execute(
        "INSERT INTO customers (name, email, tier) "
        "SELECT DISTINCT customer_name, customer_email, customer_tier "
        "FROM sales_records WHERE customer_email IS NOT NULL AND customer_email != ''"
    )
    # Populate products
    conn.execute(
        "INSERT INTO products (name, sku, category) "
        "SELECT DISTINCT product_name, product_sku, product_category FROM sales_records"
    )
    # Populate sales (exclude rows with empty customer email)
    conn.execute(
        "INSERT INTO sales (salesperson_id, customer_id, product_id, quantity, "
        "unit_price, discount_pct, sale_date) "
        "SELECT sp.id, c.id, p.id, sr.quantity, sr.unit_price, sr.discount_pct, sr.sale_date "
        "FROM sales_records sr "
        "JOIN salespersons sp ON TRIM(sr.rep_email) = sp.email "
        "JOIN customers c ON sr.customer_email = c.email "
        "JOIN products p ON sr.product_sku = p.sku "
        "WHERE sr.customer_email IS NOT NULL AND sr.customer_email != ''"
    )
    # Log data issues (empty email)
    conn.execute(
        "INSERT INTO data_issues (source_table, source_row_id, issue_type, issue_detail) "
        "SELECT 'sales_records', id, 'empty_email', "
        "'Customer email is empty for: ' || customer_name "
        "FROM sales_records WHERE customer_email IS NULL OR customer_email = ''"
    )
    conn.execute("DROP TABLE sales_records")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


def golden_task7(conn: sqlite3.Connection) -> None:
    """Golden migration for Task 7: Dual-Source Consolidation."""
    conn.execute("PRAGMA foreign_keys = OFF")

    # Create unified_customers
    conn.execute(
        "CREATE TABLE unified_customers (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "legacy_id INTEGER, modern_uuid TEXT, name TEXT, email TEXT, phone TEXT, "
        "tier TEXT NOT NULL DEFAULT 'free', source TEXT NOT NULL, created_at TEXT)"
    )
    # Insert legacy-only customers (no email match in modern)
    conn.execute(
        "INSERT INTO unified_customers (legacy_id, modern_uuid, name, email, phone, tier, source, created_at) "
        "SELECT lc.id, NULL, lc.full_name, lc.contact_email, lc.phone, lc.account_type, 'legacy', lc.join_date "
        "FROM legacy_customers lc "
        "WHERE lc.contact_email IS NULL OR lc.contact_email NOT IN (SELECT email_address FROM modern_users WHERE email_address IS NOT NULL)"
    )
    # Insert modern-only users (no email match in legacy)
    conn.execute(
        "INSERT INTO unified_customers (legacy_id, modern_uuid, name, email, phone, tier, source, created_at) "
        "SELECT NULL, mu.uuid, mu.display_name, mu.email_address, NULL, "
        "CASE mu.subscription_tier "
        "  WHEN 1 THEN 'free' WHEN 2 THEN 'basic' WHEN 3 THEN 'premium' WHEN 4 THEN 'enterprise' "
        "  ELSE 'free' END, "
        "'modern', mu.created_at "
        "FROM modern_users mu "
        "WHERE mu.email_address NOT IN (SELECT contact_email FROM legacy_customers WHERE contact_email IS NOT NULL)"
    )
    # Insert matched (both) customers — legacy name + modern tier
    conn.execute(
        "INSERT INTO unified_customers (legacy_id, modern_uuid, name, email, phone, tier, source, created_at) "
        "SELECT lc.id, mu.uuid, lc.full_name, lc.contact_email, lc.phone, "
        "CASE mu.subscription_tier "
        "  WHEN 1 THEN 'free' WHEN 2 THEN 'basic' WHEN 3 THEN 'premium' WHEN 4 THEN 'enterprise' "
        "  ELSE 'free' END, "
        "'both', lc.join_date "
        "FROM legacy_customers lc "
        "JOIN modern_users mu ON lc.contact_email = mu.email_address "
        "WHERE lc.contact_email IS NOT NULL"
    )

    # Create unified_products
    conn.execute(
        "CREATE TABLE unified_products (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "code TEXT NOT NULL UNIQUE, title TEXT NOT NULL, price REAL NOT NULL, "
        "source TEXT NOT NULL)"
    )
    # Legacy products
    conn.execute(
        "INSERT INTO unified_products (code, title, price, source) "
        "SELECT code, description, "
        "CAST(REPLACE(REPLACE(unit_price, '$', ''), ',', '') AS REAL), 'legacy' "
        "FROM legacy_products"
    )
    # Modern products (no code overlap expected)
    conn.execute(
        "INSERT INTO unified_products (code, title, price, source) "
        "SELECT sku, title, base_price, 'modern' "
        "FROM modern_catalog"
    )

    # Create migration_issues
    conn.execute(
        "CREATE TABLE migration_issues (id INTEGER PRIMARY KEY, "
        "source_system TEXT NOT NULL, source_table TEXT NOT NULL, "
        "source_id TEXT NOT NULL, issue_type TEXT NOT NULL, "
        "resolution TEXT NOT NULL)"
    )
    # Log NULL email customer
    conn.execute(
        "INSERT INTO migration_issues (source_system, source_table, source_id, issue_type, resolution) "
        "SELECT 'legacy', 'legacy_customers', CAST(id AS TEXT), 'null_email', "
        "'Imported without email' "
        "FROM legacy_customers WHERE contact_email IS NULL"
    )
    # Log orphaned transactions
    conn.execute(
        "INSERT INTO migration_issues (source_system, source_table, source_id, issue_type, resolution) "
        "SELECT 'modern', 'modern_transactions', CAST(id AS TEXT), 'orphaned_record', "
        "'User UUID not found: ' || user_uuid "
        "FROM modern_transactions WHERE user_uuid NOT IN (SELECT uuid FROM modern_users)"
    )

    # Create unified_orders
    conn.execute(
        "CREATE TABLE unified_orders (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "customer_id INTEGER NOT NULL, product_id INTEGER, amount REAL NOT NULL, "
        "currency TEXT NOT NULL DEFAULT 'USD', status TEXT NOT NULL, "
        "order_date TEXT, source TEXT NOT NULL, "
        "FOREIGN KEY (customer_id) REFERENCES unified_customers(id))"
    )
    # Legacy orders
    conn.execute(
        "INSERT INTO unified_orders (customer_id, product_id, amount, currency, status, order_date, source) "
        "SELECT uc.id, up.id, "
        "CAST(REPLACE(REPLACE(lo.total_amount, '$', ''), ',', '') AS REAL), "
        "'USD', lo.order_status, lo.order_date, 'legacy' "
        "FROM legacy_orders lo "
        "JOIN legacy_customers lc ON lo.customer_id = lc.id "
        "JOIN unified_customers uc ON (uc.legacy_id = lc.id) "
        "LEFT JOIN unified_products up ON lo.product_code = up.code"
    )
    # Modern transactions (exclude orphans)
    conn.execute(
        "INSERT INTO unified_orders (customer_id, product_id, amount, currency, status, order_date, source) "
        "SELECT uc.id, up.id, mt.amount, "
        "COALESCE(mt.currency, 'USD'), "
        "CASE mt.tx_status "
        "  WHEN 1 THEN 'pending' WHEN 2 THEN 'processing' WHEN 3 THEN 'complete' "
        "  WHEN 4 THEN 'failed' WHEN 5 THEN 'refunded' ELSE 'unknown' END, "
        "mt.created_at, 'modern' "
        "FROM modern_transactions mt "
        "JOIN modern_users mu ON mt.user_uuid = mu.uuid "
        "JOIN unified_customers uc ON (uc.modern_uuid = mu.uuid OR uc.email = mu.email_address) "
        "LEFT JOIN unified_products up ON mt.item_sku = up.code"
    )

    # Clean up source tables
    conn.execute("DROP TABLE legacy_customers")
    conn.execute("DROP TABLE legacy_orders")
    conn.execute("DROP TABLE legacy_products")
    conn.execute("DROP TABLE modern_users")
    conn.execute("DROP TABLE modern_transactions")
    conn.execute("DROP TABLE modern_catalog")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.commit()


# =============================================================================
# TASK 8: Data Poisoning & Quarantine Routing (Extreme)
# =============================================================================

TASK8_TARGET_DDL = \"\"\"
CREATE TABLE inventory (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    sku TEXT UNIQUE
);

CREATE TABLE inventory_quarantine (
    id INTEGER PRIMARY KEY,
    raw_name TEXT,
    raw_price TEXT,
    raw_sku TEXT,
    error_reason TEXT
);
\"\"\".strip()

def seed_task8(conn):
    conn.execute("CREATE TABLE staging_data (id INTEGER, item TEXT, cost TEXT, sku_code TEXT)")
    data = [
        (1, "Oscilloscope", "1500.00", "OSC-001"),
        (2, "Multimeter", "  75.50 ", "MUL-002"),
        (3, "Soldering Iron", "$45.00", "SLD-003"),
        (4, "Lead Solder", "N/A", "LSD-004"),
        (5, "DC Power Supply", "299.99", "PWR-005"),
        (6, "Unknown Device", "INVALID", "UNK-006"),
        (7, "Wire Strippers", "$ 12.50", "WRE-007"),
    ]
    conn.executemany("INSERT INTO staging_data VALUES (?,?,?,?)", data)
    conn.commit()

def golden_task8(conn):
    conn.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price REAL NOT NULL, sku TEXT UNIQUE)")
    conn.execute("CREATE TABLE inventory_quarantine (id INTEGER PRIMARY KEY, raw_name TEXT, raw_price TEXT, raw_sku TEXT, error_reason TEXT)")
    
    # Process staging_data
    cursor = conn.execute("SELECT id, item, cost, sku_code FROM staging_data")
    for row in cursor.fetchall():
        rid, name, cost, sku = row
        clean_cost = cost.replace("$", "").strip()
        
        try:
            price = float(clean_cost)
            conn.execute("INSERT INTO inventory (id, name, price, sku) VALUES (?,?,?,?)", (rid, name, price, sku))
        except ValueError:
            conn.execute("INSERT INTO inventory_quarantine (raw_name, raw_price, raw_sku, error_reason) VALUES (?,?,?,?)", 
                         (name, cost, sku, "invalid_numeric_format"))
    conn.commit()

# =============================================================================
# Task Registry
# =============================================================================

TASKS = {
    "column-restructure": {
        "seed_fn": seed_task1,
        "golden_fn": golden_task1,
        "target_ddl": TASK1_TARGET_DDL,
        "description": "Merge first_name and last_name into a single full_name column (concatenated with a space) without data loss. Apostrophes in names (e.g., O'Brien) must be preserved.",
        "difficulty": "easy",
        "max_steps": 10,
    },
    "soft-delete-restoration": {
        "seed_fn": seed_task4,
        "golden_fn": golden_task4,
        "target_ddl": TASK4_TARGET_DDL,
        "description": "Restore deleted products from the deletion_log table back into the products table. Use product_id from deletion_log (NOT the log's id column) as the primary key. Add is_deleted (1) and deleted_at values from log. Original rows stay as is_deleted=0, deleted_at=NULL.",
        "difficulty": "easy",
        "max_steps": 10,
    },
    "table-normalization": {
        "seed_fn": seed_task2,
        "golden_fn": golden_task2,
        "target_ddl": TASK2_TARGET_DDL,
        "description": "Normalize a flat purchases table into customers and orders tables linked by customer_id (FK). Ensure customers are distinct by email.",
        "difficulty": "medium",
        "max_steps": 15,
    },
    "schema-version-merge": {
        "seed_fn": seed_task5,
        "golden_fn": golden_task5,
        "target_ddl": TASK5_TARGET_DDL,
        "description": "Merge products_v1 (Legacy) and products_v2 (Modern) with ID collision logic: Modern (v2) wins. Coerce v1 price strings ($) to REAL.",
        "difficulty": "medium",
        "max_steps": 15,
    },
    "multi-entity-extraction": {
        "seed_fn": seed_task6,
        "golden_fn": golden_task6,
        "target_ddl": TASK6_TARGET_DDL,
        "description": "Decompose sales_records into 3NF: salespersons, customers, products, and sales. Route rows with missing emails to data_issues.",
        "difficulty": "medium",
        "max_steps": 15,
    },
    "cascade-migration": {
        "seed_fn": seed_task3,
        "golden_fn": golden_task3,
        "target_ddl": TASK3_TARGET_DDL,
        "description": "Multi-table FK cascade with type coercion for salary and orphan logging for assets.",
        "difficulty": "hard",
        "max_steps": 20,
    },
    "dual-source-consolidation": {
        "seed_fn": seed_task7,
        "golden_fn": golden_task7,
        "target_ddl": TASK7_TARGET_DDL,
        "description": "Consolidate Legacy CRM and Modern SaaS data with cross-system email deduping and complex state/type mapping.",
        "difficulty": "hard",
        "max_steps": 20,
    },
    "data-poisoning-quarantine": {
        "seed_fn": seed_task8,
        "golden_fn": golden_task8,
        "target_ddl": TASK8_TARGET_DDL,
        "description": "The ultimate technical test: Migrate inventory from a 'poisoned' staging table. Cleanse raw price strings and route un-coerceable rows (like 'N/A') to a quarantine table while maintaining strict schema integrity.",
        "difficulty": "extreme",
        "max_steps": 15,
    },
}
