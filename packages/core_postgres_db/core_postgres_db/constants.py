# packages/core_postgres_db/core_postgres_db/constants.py
"""
Constants for PostgreSQL operations - Sonarqube compliant
"""

# Timeout constants
DEFAULT_TIMEOUT_SECONDS = 30
BULK_OPERATION_TIMEOUT = 120
RAW_SQL_TIMEOUT = 60

# Retry constants
MAX_DEADLOCK_RETRIES = 3
INITIAL_RETRY_WAIT = 0.1

# Batch operations
DEFAULT_BATCH_SIZE = 1000

# Pool configuration
DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 30
DEFAULT_POOL_TIMEOUT = 30
DEFAULT_POOL_RECYCLE = 3600
DEFAULT_STATEMENT_TIMEOUT = 30000

# Transaction isolation levels
READ_COMMITTED = "READ COMMITTED"
REPEATABLE_READ = "REPEATABLE READ"
SERIALIZABLE = "SERIALIZABLE"

# Query limits
MAX_QUERY_LIMIT = 1000
DEFAULT_PAGE_SIZE = 20

# Error messages
DUPLICATE_KEY_ERROR = "duplicate key"
CONSTRAINT_VIOLATION_ERROR = "violates"

# Table name pattern for validation
TABLE_NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*$"

# Logging constants
LOG_QUERY_EXECUTION = True
LOG_ERROR_DETAILS = True