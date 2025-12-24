# packages/core_postgres_db/core_postgres_db/base_operations.py
"""
Common operations shared between sync and async implementations
Sonarqube compliant: Reduces code duplication
"""
import logging
import time
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from .constants import (
    TABLE_NAME_PATTERN, DUPLICATE_KEY_ERROR, CONSTRAINT_VIOLATION_ERROR
)
from .exceptions import DatabaseError, DuplicateEntryError, ConstraintViolationError
from .performance_monitor import QueryStats
from .utils import safe_table_ref

logger = logging.getLogger(__name__)


class BaseDatabaseOperations:
    """
    Common database operations to avoid code duplication
    This class contains shared logic between sync and async implementations
    """
    
    def __init__(self, config, engine: Engine, metadata: MetaData):
        """Initialize with shared components"""
        self.config = config
        self.engine = engine
        self.metadata = metadata
        self._table_cache: Dict[str, Table] = {}
        self._query_stats = QueryStats()
        
        logger.info(f"Database operations initialized for schema: {config.schema}")
    
    # ============= Table Management =============
    
    def _get_table(self, table_name: str) -> Table:
        """
        Get table with caching and validation
        Shared between sync and async
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        safe_name = safe_table_ref(table_name, TABLE_NAME_PATTERN)
        
        if safe_name not in self._table_cache:
            table = Table(
                safe_name,
                self.metadata,
                autoload_with=self.engine,
                schema=self.config.schema,
                extend_existing=True
            )
            self._table_cache[safe_name] = table
        
        return self._table_cache[safe_name]
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get table metadata information
        Shared between sync and async
        """
        table = self._get_table(table_name)
        
        return {
            "name": table.name,
            "schema": table.schema,
            "columns": [
                {
                    "name": column.name,
                    "type": str(column.type),
                    "nullable": column.nullable,
                    "primary_key": column.primary_key
                }
                for column in table.columns
            ],
            "primary_key": [col.name for col in table.primary_key.columns],
            "foreign_keys": [
                {
                    "constrained_columns": list(fk.constrained_columns),
                    "referred_table": fk.referred_table.name,
                    "referred_columns": list(fk.referred_columns)
                }
                for fk in table.foreign_keys
            ]
        }
    
    # ============= Error Handling =============
    
    def _handle_integrity_error(
        self, 
        error: SQLAlchemyError, 
        operation: str,
        table_name: str
    ) -> None:
        """
        Handle integrity errors (duplicate keys, constraints)
        Shared error handling logic
        """
        error_msg = str(error).lower()
        
        if DUPLICATE_KEY_ERROR in error_msg:
            raise DuplicateEntryError(
                f"Duplicate entry in {operation} for table '{table_name}': {error}"
            )
        elif CONSTRAINT_VIOLATION_ERROR in error_msg:
            raise ConstraintViolationError(
                f"Constraint violation in {operation} for table '{table_name}': {error}"
            )
        else:
            raise DatabaseError(
                f"Integrity error in {operation} for table '{table_name}': {error}"
            )
    
    def _handle_database_error(
        self, 
        error: SQLAlchemyError, 
        operation: str,
        table_name: str
    ) -> None:
        """
        Handle general database errors
        Shared error handling logic
        """
        raise DatabaseError(
            f"Database error in {operation} for table '{table_name}': {error}"
        )
    
    # ============= Metrics Recording =============
    
    def _record_query_metrics(
        self,
        operation: str,
        table_name: str,
        start_time: float,
        rows_affected: int,
        status: str = "success",
        error: Optional[str] = None
    ) -> None:
        """
        Record query execution metrics
        Shared metrics logic
        """
        elapsed = time.time() - start_time
        key = f"{operation}:{table_name}"
        
        if operation in ["create", "update", "delete", "bulk_create"]:
            self._query_stats.record_write(
                key=key,
                exec_time=elapsed,
                rows=rows_affected,
                status=status,
                error=error
            )
        else:
            self._query_stats.record_read(
                key=key,
                exec_time=elapsed,
                rows=rows_affected,
                status=status,
                error=error
            )
    
    # ============= Validation Methods =============
    
    def _validate_conditions(self, conditions: Dict[str, Any]) -> None:
        """Validate query conditions"""
        if conditions is None:
            return
        
        if not isinstance(conditions, dict):
            raise ValueError("Conditions must be a dictionary")
        
        # Check for invalid keys
        for key in conditions.keys():
            if not isinstance(key, str):
                raise ValueError(f"Condition key must be string, got {type(key)}")
    
    def _validate_data(self, data: Dict[str, Any], operation: str) -> None:
        """Validate data for create/update operations"""
        if not data:
            raise ValueError(f"Data cannot be empty for {operation}")
        
        if not isinstance(data, dict):
            raise ValueError(f"Data must be a dictionary for {operation}")
    
    # ============= Utility Methods =============
    
    def clear_table_cache(self) -> None:
        """Clear the table metadata cache"""
        self._table_cache.clear()
        logger.debug("Table cache cleared")
    
    def get_cached_tables(self) -> List[str]:
        """Get list of cached tables"""
        return list(self._table_cache.keys())
    
    def get_query_stats(self) -> Dict[str, Any]:
        """Get query performance statistics"""
        return self._query_stats.snapshot()
    
    def reset_query_stats(self) -> None:
        """Reset query statistics"""
        self._query_stats.reset()
        logger.debug("Query statistics reset")
    
    # ============= Context Managers =============
    
    @contextmanager
    def _measure_execution_time(self, operation: str, table_name: str):
        """
        Context manager to measure query execution time
        Shared timing logic
        """
        start_time = time.time()
        status = "success"
        error = None
        rows_affected = 0
        
        try:
            yield {
                "start_time": start_time,
                "set_rows": lambda rows: rows_affected.__set__(rows_affected, rows)
            }
        except Exception as e:
            status = "error"
            error = str(e)
            raise
        finally:
            self._record_query_metrics(
                operation=operation,
                table_name=table_name,
                start_time=start_time,
                rows_affected=rows_affected,
                status=status,
                error=error
            )