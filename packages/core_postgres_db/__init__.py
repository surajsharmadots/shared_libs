# packages/core_postgres_db/__init__.py (ROOT PACKAGE)
"""
Core PostgreSQL Database Wrapper for E-commerce Microservices

A high-performance, async-first PostgreSQL client with atomic transactions,
connection pooling, and comprehensive error handling.

Features:
- Async-first design (optimized for FastAPI)
- Full atomic transaction support
- Automatic retry on deadlocks
- Query timeouts and performance monitoring
- E-commerce specific optimizations
- Sonarqube compliant code
"""

__version__ = "0.1.0"
__author__ = "Suraj Sharma"
__license__ = "MIT"

# Main exports
from .core_postgres_db import (
    AsyncPostgresDB,
    SyncPostgresDB,
    DatabaseConfig,
    get_database_config,
    create_database_client,  
    # Exceptions
    DatabaseError,
    ConnectionError,
    QueryError,
    IntegrityError,
    DuplicateEntryError,
    ConstraintViolationError,
    ForeignKeyViolationError,
    TransactionError,
    TimeoutError,
    ConfigurationError,
    ValidationError,
    ResourceNotFoundError,
    DatabaseOperationError,
    # Types
    QueryOptions,
    BulkInsertOptions,
    UpdateOptions,
    PaginationParams,
    PaginatedResult,
    # Utilities
    rows_to_dicts,
    build_where_clause,
    QueryBuilder,
    QueryStats,
)

# Re-export for backward compatibility
__all__ = [
    # Main clients
    "AsyncPostgresDB",
    "SyncPostgresDB",
    "create_database_client",
    
    # Configuration
    "DatabaseConfig",
    "get_database_config",
    
    # Exceptions
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    "IntegrityError",
    "DuplicateEntryError",
    "ConstraintViolationError",
    "ForeignKeyViolationError",
    "TransactionError",
    "TimeoutError",
    "ConfigurationError",
    "ValidationError",
    "ResourceNotFoundError",
    "DatabaseOperationError",
    
    # Types
    "QueryOptions",
    "BulkInsertOptions",
    "UpdateOptions",
    "PaginationParams",
    "PaginatedResult",
    
    # Utilities
    "rows_to_dicts",
    "build_where_clause",
    "QueryBuilder",
    "QueryStats",
    
    # Version
    "__version__",
]