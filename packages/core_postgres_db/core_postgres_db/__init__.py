# packages/core_postgres_db/__init__.py
"""
Core PostgreSQL Database Module

This module contains all the core functionality for database operations.
It's designed to be imported by microservices that need database access.
"""

# Re-export main components for easy access
from .async_postgres import AsyncPostgresDB
from .sync_postgres import SyncPostgresDB
from .config import DatabaseConfig, get_database_config
from .exceptions import *
from .types import *
from .utils import rows_to_dicts, build_where_clause
from .query_builder import QueryBuilder
from .performance_monitor import QueryStats

# Factory function for creating database clients
def create_database_client(
    connection_string: str = None,
    config: DatabaseConfig = None,
    use_async: bool = True,
    **kwargs
):
    """
    Factory function to create database client
    
    Args:
        connection_string: PostgreSQL connection string
        config: Pre-configured DatabaseConfig object
        use_async: Whether to create async client (default: True)
        **kwargs: Additional configuration parameters
    
    Returns:
        AsyncPostgresDB or SyncPostgresDB instance
    
    Examples:
        # Async client (recommended for FastAPI)
        db = create_database_client(use_async=True)
        
        # With connection string
        db = create_database_client(
            connection_string="postgresql://user:pass@localhost/db",
            use_async=True
        )
        
        # Sync client (for legacy code)
        db = create_database_client(use_async=False)
        
        # With configuration object
        config = DatabaseConfig(...)
        db = create_database_client(config=config)
    """
    if use_async:
        return AsyncPostgresDB(
            connection_string=connection_string,
            config=config,
            **kwargs
        )
    else:
        return SyncPostgresDB(
            connection_string=connection_string,
            config=config,
            **kwargs
        )

__all__ = [
    # Main classes
    "AsyncPostgresDB",
    "SyncPostgresDB",
    "DatabaseConfig",
    "get_database_config",
    "create_database_client", 
    
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
]