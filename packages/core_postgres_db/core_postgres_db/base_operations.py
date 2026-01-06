# packages/core_postgres_db/core_postgres_db/base_operations.py
"""
PURE SYNC operations - Sonarqube compliant
"""
import logging
import time
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

from sqlalchemy import Table, MetaData, inspect
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
    PURE SYNC database operations
    Contains ONLY sync methods
    """
    
    def __init__(self, config, engine: Engine, metadata: MetaData):
        """Initialize with SYNC components"""
        self.config = config
        self.engine = engine  # SYNC engine
        self.metadata = metadata
        self._table_cache: Dict[str, Table] = {}
        self._query_stats = QueryStats()
        
        logger.info(f"SYNC Database operations initialized for schema: {config.schema}")
    
    # ============= Table Management (SYNC) =============
    
    def _get_table(self, table_name: str) -> Table:
        """
        Get table with caching - SYNC VERSION
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        safe_name = safe_table_ref(table_name, TABLE_NAME_PATTERN)
        
        if safe_name not in self._table_cache:
            try:
                # SYNC table reflection
                table = Table(
                    safe_name,
                    self.metadata,
                    autoload_with=self.engine,  # SYNC engine
                    schema=self.config.schema,
                    extend_existing=True
                )
                self._table_cache[safe_name] = table
                logger.debug(f"SYNC: Table {safe_name} reflected successfully")
                
            except Exception as e:
                logger.warning(f"SYNC: Failed to autoload table {safe_name}: {e}")
                # Fallback: create minimal table
                table = Table(
                    safe_name,
                    self.metadata,
                    schema=self.config.schema
                )
                self._table_cache[safe_name] = table
        
        return self._table_cache[safe_name]
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get table metadata information - SYNC
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
    
    # ============= Error Handling (SYNC) =============
    
    def _handle_integrity_error(
        self, 
        error: SQLAlchemyError, 
        operation: str,
        table_name: str
    ) -> None:
        """
        Handle integrity errors - SYNC
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
        Handle general database errors - SYNC
        """
        raise DatabaseError(
            f"Database error in {operation} for table '{table_name}': {error}"
        )
    
    # ============= Metrics Recording (SYNC) =============
    
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
        Record query execution metrics - SYNC
        """
        elapsed = time.time() - start_time
        key = f"sync_{operation}:{table_name}"
        
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
    
    # ============= Validation Methods (SYNC) =============
    
    def _validate_conditions(self, conditions: Dict[str, Any]) -> None:
        """Validate query conditions - SYNC"""
        if conditions is None:
            return
        
        if not isinstance(conditions, dict):
            raise ValueError("Conditions must be a dictionary")
        
        # Check for invalid keys
        for key in conditions.keys():
            if not isinstance(key, str):
                raise ValueError(f"Condition key must be string, got {type(key)}")
    
    def _validate_data(self, data: Dict[str, Any], operation: str) -> None:
        """Validate data for create/update operations - SYNC"""
        if not data:
            raise ValueError(f"Data cannot be empty for {operation}")
        
        if not isinstance(data, dict):
            raise ValueError(f"Data must be a dictionary for {operation}")
    
    # ============= Utility Methods (SYNC) =============
    
    def clear_table_cache(self) -> None:
        """Clear the table metadata cache - SYNC"""
        self._table_cache.clear()
        logger.debug("SYNC Table cache cleared")
    
    def get_cached_tables(self) -> List[str]:
        """Get list of cached tables - SYNC"""
        return list(self._table_cache.keys())
    
    def get_query_stats(self) -> Dict[str, Any]:
        """Get query performance statistics - SYNC"""
        return self._query_stats.snapshot()
    
    def reset_query_stats(self) -> None:
        """Reset query statistics - SYNC"""
        self._query_stats.reset()
        logger.debug("SYNC Query statistics reset")
    
    # ============= Context Managers (SYNC) =============
    
    @contextmanager
    def _measure_execution_time(self, operation: str, table_name: str):
        """
        Context manager to measure query execution time - SYNC
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