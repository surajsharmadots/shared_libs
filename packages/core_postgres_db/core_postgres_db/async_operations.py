# packages/core_postgres_db/core_postgres_db/async_operations.py
"""
PURE ASYNC operations - Sonarqube compliant
"""
import logging
import time
from typing import Any, Dict, List, Optional

from sqlalchemy import Table, MetaData, inspect, Column
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.exc import SQLAlchemyError

from .constants import TABLE_NAME_PATTERN
from .exceptions import DatabaseError, DuplicateEntryError, ConstraintViolationError
from .performance_monitor import QueryStats
from .utils import safe_table_ref

logger = logging.getLogger(__name__)


class AsyncDatabaseOperations:
    """
    PURE ASYNC database operations
    Contains ONLY async methods
    """
    
    def __init__(self, config, async_engine: AsyncEngine, metadata: MetaData):
        """Initialize with ASYNC components"""
        self.config = config
        self.async_engine = async_engine  # ASYNC engine
        self.metadata = metadata
        self._table_cache: Dict[str, Table] = {}
        self._query_stats = QueryStats()
        self._reflected_tables = set()  # Track reflected tables
        
        logger.info(f"ASYNC Database operations initialized for schema: {config.schema}")
    
    # ============= Table Management (ASYNC) =============
    
    async def _get_table(self, table_name: str) -> Table:
        """
        Get table with caching - ASYNC VERSION
        """
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        safe_name = safe_table_ref(table_name, TABLE_NAME_PATTERN)
        
        if safe_name not in self._table_cache or safe_name not in self._reflected_tables:
            table = await self._reflect_table_async(safe_name)
            self._table_cache[safe_name] = table
            self._reflected_tables.add(safe_name)
        
        return self._table_cache[safe_name]
    
    async def _reflect_table_async(self, table_name: str) -> Table:
        """
        Reflect table structure from database - ASYNC
        """
        from sqlalchemy import Table as SA_Table
        
        try:
            async with self.async_engine.connect() as conn:
                # Reflect all tables in schema using run_sync
                await conn.run_sync(
                    lambda sync_conn: self.metadata.reflect(
                        bind=sync_conn,
                        schema=self.config.schema
                    )
                )
                
                # Table ka full name banaye (schema ke saath)
                full_table_name = f"{self.config.schema}.{table_name}" if self.config.schema else table_name
                
                if full_table_name in self.metadata.tables:
                    table = self.metadata.tables[full_table_name]
                    logger.debug(f"ASYNC: Table {table_name} reflected successfully")
                    return table
                else:
                    # Table nahi mila to empty table create karo
                    logger.warning(f"ASYNC: Table {table_name} not found in metadata")
                    
        except Exception as e:
            logger.error(f"ASYNC: Failed to reflect table {table_name}: {e}")
            # Log detailed error
            import traceback
            logger.error(f"ASYNC: Traceback: {traceback.format_exc()}")
        
        # Fallback: create minimal table
        return SA_Table(
            table_name,
            self.metadata,
            schema=self.config.schema
        )
    
    async def get_table_info_async(self, table_name: str) -> Dict[str, Any]:
        """
        Get table metadata information - ASYNC
        """
        table = await self._get_table(table_name)
        
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
            ] if hasattr(table, 'foreign_keys') else []
        }
    
    # ============= Error Handling (ASYNC) =============
    
    def _handle_integrity_error_async(
        self, 
        error: SQLAlchemyError, 
        operation: str,
        table_name: str
    ) -> None:
        """
        Handle integrity errors - ASYNC
        """
        error_msg = str(error).lower()
        
        if "duplicate" in error_msg or "unique" in error_msg:
            raise DuplicateEntryError(
                f"Duplicate entry in {operation} for table '{table_name}': {error}"
            )
        elif "constraint" in error_msg or "violates" in error_msg:
            raise ConstraintViolationError(
                f"Constraint violation in {operation} for table '{table_name}': {error}"
            )
        else:
            raise DatabaseError(
                f"Integrity error in {operation} for table '{table_name}': {error}"
            )
    
    def _handle_database_error_async(
        self, 
        error: SQLAlchemyError, 
        operation: str,
        table_name: str
    ) -> None:
        """
        Handle general database errors - ASYNC
        """
        raise DatabaseError(
            f"Database error in {operation} for table '{table_name}': {error}"
        )
    
    # ============= Metrics Recording (ASYNC) =============
    
    async def _record_query_metrics_async(
        self,
        operation: str,
        table_name: str,
        start_time: float,
        rows_affected: int,
        status: str = "success",
        error: Optional[str] = None
    ) -> None:
        """
        Record query execution metrics - ASYNC
        """
        elapsed = time.time() - start_time
        key = f"async_{operation}:{table_name}"
        
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
    
    # ============= Validation Methods (ASYNC) =============
    
    def _validate_conditions_async(self, conditions: Dict[str, Any]) -> None:
        """Validate query conditions - ASYNC"""
        if conditions is None:
            return
        
        if not isinstance(conditions, dict):
            raise ValueError("Conditions must be a dictionary")
        
        # Check for invalid keys
        for key in conditions.keys():
            if not isinstance(key, str):
                raise ValueError(f"Condition key must be string, got {type(key)}")
    
    def _validate_data_async(self, data: Dict[str, Any], operation: str) -> None:
        """Validate data for create/update operations - ASYNC"""
        if not data:
            raise ValueError(f"Data cannot be empty for {operation}")
        
        if not isinstance(data, dict):
            raise ValueError(f"Data must be a dictionary for {operation}")
    
    # ============= Utility Methods (ASYNC) =============
    
    async def clear_table_cache_async(self) -> None:
        """Clear the table metadata cache - ASYNC"""
        self._table_cache.clear()
        self._reflected_tables.clear()
        self.metadata.clear()
        logger.debug("ASYNC Table cache cleared")
    
    async def get_cached_tables_async(self) -> List[str]:
        """Get list of cached tables - ASYNC"""
        return list(self._table_cache.keys())
    
    async def get_query_stats_async(self) -> Dict[str, Any]:
        """Get query performance statistics - ASYNC"""
        return self._query_stats.snapshot()
    
    async def reset_query_stats_async(self) -> None:
        """Reset query statistics - ASYNC"""
        self._query_stats.reset()
        logger.debug("ASYNC Query statistics reset")