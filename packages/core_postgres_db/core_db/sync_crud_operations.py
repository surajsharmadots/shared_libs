"""
Synchronous CRUD operations with atomic transactions
"""
import time
import logging
from typing import Any, Dict, List, Optional, Union
from contextlib import contextmanager
from datetime import datetime

from sqlalchemy import Table, insert, select, update, delete, text, func
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy.sql import Select

from .connection import ConnectionManager
from .types import DatabaseConfig, QueryOptions, BulkInsertOptions, UpdateOptions
from .exceptions import (
    DatabaseError, DuplicateEntryError, ConstraintViolationError,
    RecordNotFoundError, TimeoutError, TransactionError
)
from .performance_monitor import QueryStats, QueryMetrics
from .utils import (
    safe_table_ref, rows_to_dicts, validate_table_name,
    build_where_clause, paginate_query, extract_returning_columns
)
from .decorators import retry_on_deadlock, timeout, log_query_execution
from .query_builder import QueryBuilder
from .transactions import TransactionManager
from .connection import get_database_config

logger = logging.getLogger(__name__)

class SyncPostgresDB:
    """
    Synchronous PostgreSQL operations with full atomic support
    
    Features:
    - All writes are atomic by default
    - Connection pooling
    - Automatic retry on deadlocks
    - Query timeouts
    - Comprehensive error handling
    - Environment-based configuration
    """
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        config: Optional[DatabaseConfig] = None,
        **kwargs
    ):
        """
        Initialize database connection
        
        Args:
            connection_string: PostgreSQL connection string (optional)
            config: DatabaseConfig object (optional)
            **kwargs: Database configuration parameters
        """
        if config:
            self.config = config
        else:
            self.config = get_database_config(
                connection_string=connection_string,
                **kwargs
            )
        
        self.connection_manager = ConnectionManager(self.config)
        self.engine: Engine = self.connection_manager.get_sync_engine()
        self.metadata = self.connection_manager.metadata
        
        # Cache for table metadata
        self._table_cache: Dict[str, Table] = {}
        self._query_stats = QueryStats()
        self._query_builder = QueryBuilder()
        self._transaction_manager = TransactionManager(self.engine)
        
        logger.info(f"SyncPostgresDB initialized for schema: {self.config.schema}")
    
    # ==================== TABLE MANAGEMENT ====================
    
    def _get_table(self, table_name: str) -> Table:
        """Get table with caching and schema support"""
        safe_name = safe_table_ref(table_name)
        
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
        """Get table metadata information"""
        table = self._get_table(table_name)
        return {
            "name": table.name,
            "schema": table.schema,
            "columns": [{"name": c.name, "type": str(c.type)} for c in table.columns],
            "primary_key": [c.name for c in table.primary_key.columns],
            "foreign_keys": [
                {
                    "constrained_columns": list(fk.constrained_columns),
                    "referred_table": fk.referred_table.name,
                    "referred_columns": list(fk.referred_columns)
                }
                for fk in table.foreign_keys
            ]
        }
    
    # ==================== ATOMIC CREATE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=30)
    @log_query_execution
    def create(
        self,
        table_name: str,
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Atomic insert operation
        
        Args:
            table_name: Target table
            data: Data to insert
            returning: Whether to return inserted row
            
        Returns:
            Inserted row or None
        """
        start_time = time.time()
        table = self._get_table(table_name)
        
        try:
            # Build INSERT statement
            stmt = insert(table).values(**data)
            if returning:
                stmt = stmt.returning(table)
            
            # Execute with atomic transaction
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                
                if returning:
                    row = result.fetchone()
                    elapsed = time.time() - start_time
                    self._query_stats.record_write(
                        f"create:{table_name}",
                        elapsed,
                        1,
                        "success"
                    )
                    return dict(row._mapping) if row else None
                
                elapsed = time.time() - start_time
                self._query_stats.record_write(
                    f"create:{table_name}",
                    elapsed,
                    1,
                    "success"
                )
                return None
                
        except IntegrityError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"create:{table_name}",
                elapsed,
                0,
                "integrity_error",
                str(e)
            )
            
            if "duplicate key" in str(e).lower():
                raise DuplicateEntryError(f"Duplicate entry for {table_name}: {e}")
            raise ConstraintViolationError(f"Constraint violation in {table_name}: {e}")
            
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"create:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Insert failed for {table_name}: {e}")
    
    # ==================== ATOMIC BULK CREATE ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=120)
    @log_query_execution
    def bulk_create(
        self,
        table_name: str,
        data_list: List[Dict[str, Any]],
        options: Optional[BulkInsertOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Atomic bulk insert with batch processing
        
        Args:
            table_name: Target table
            data_list: List of data dictionaries
            options: Bulk insert options
            
        Returns:
            List of inserted rows
        """
        if not data_list:
            return []
        
        opts = options or BulkInsertOptions()
        table = self._get_table(table_name)
        all_rows = []
        total_rows = len(data_list)
        
        start_time = time.time()
        
        try:
            # Execute in a single atomic transaction
            with self.engine.begin() as conn:
                for i in range(0, total_rows, opts.batch_size):
                    batch = data_list[i:i + opts.batch_size]
                    
                    stmt = insert(table).values(batch)
                    
                    # Handle conflict resolution
                    if opts.on_conflict_do_nothing:
                        stmt = stmt.on_conflict_do_nothing()
                    elif opts.on_conflict_do_update:
                        stmt = stmt.on_conflict_do_update(
                            constraint=opts.on_conflict_do_update.get("constraint"),
                            set_=opts.on_conflict_do_update.get("set_", {})
                        )
                    
                    if opts.return_rows:
                        stmt = stmt.returning(table)
                        result = conn.execute(stmt)
                        all_rows.extend(rows_to_dicts(result))
                    else:
                        conn.execute(stmt)
                
                elapsed = time.time() - start_time
                self._query_stats.record_write(
                    f"bulk_create:{table_name}",
                    elapsed,
                    total_rows,
                    "success"
                )
                return all_rows
                
        except IntegrityError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"bulk_create:{table_name}",
                elapsed,
                0,
                "integrity_error",
                str(e)
            )
            
            if "duplicate key" in str(e).lower():
                raise DuplicateEntryError(f"Duplicate in bulk insert to {table_name}: {e}")
            raise ConstraintViolationError(f"Constraint violation in bulk insert to {table_name}: {e}")
            
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"bulk_create:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Bulk insert failed for {table_name}: {e}")
    
    # ==================== READ OPERATIONS ====================
    
    @timeout(seconds=30)
    @log_query_execution
    def read(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Read records with filtering
        
        Args:
            table_name: Target table
            conditions: WHERE conditions
            options: Query options
            
        Returns:
            List of records
        """
        start_time = time.time()
        table = self._get_table(table_name)
        
        try:
            # Build query using query builder
            stmt = self._query_builder.build_select_query(
                table=table,
                columns=options.columns if options else None,
                conditions=conditions,
                order_by=options.order_by if options else None,
                limit=options.limit if options else None,
                offset=options.offset if options else 0,
                distinct=options.distinct if options else False,
                for_update=options.for_update if options else False
            )
            
            with self.engine.connect() as conn:
                result = conn.execute(stmt)
                rows = rows_to_dicts(result)
                
                elapsed = time.time() - start_time
                self._query_stats.record_read(
                    f"read:{table_name}",
                    elapsed,
                    len(rows),
                    "success"
                )
                return rows
                
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_read(
                f"read:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Read failed for {table_name}: {e}")
    
    # ==================== ATOMIC UPDATE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=30)
    @log_query_execution
    def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        conditions: Dict[str, Any],
        options: Optional[UpdateOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Atomic update operation
        
        Args:
            table_name: Target table
            data: Data to update
            conditions: WHERE conditions
            options: Update options
            
        Returns:
            List of updated rows
        """
        start_time = time.time()
        table = self._get_table(table_name)
        opts = options or UpdateOptions()
        
        try:
            # Build UPDATE statement
            stmt = update(table).values(**data)
            
            # Apply WHERE conditions
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)
            
            if opts.returning:
                stmt = stmt.returning(table)
            
            # Execute with atomic transaction if specified
            if opts.atomic:
                with self.engine.begin() as conn:
                    result = conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"update:{table_name}",
                            elapsed,
                            len(rows),
                            "success"
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"update:{table_name}",
                            elapsed,
                            rowcount,
                            "success"
                        )
                        return []
            else:
                with self.engine.connect() as conn:
                    result = conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"update:{table_name}",
                            elapsed,
                            len(rows),
                            "success"
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"update:{table_name}",
                            elapsed,
                            rowcount,
                            "success"
                        )
                        return []
                
        except IntegrityError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"update:{table_name}",
                elapsed,
                0,
                "integrity_error",
                str(e)
            )
            raise ConstraintViolationError(f"Update constraint violation in {table_name}: {e}")
            
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"update:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Update failed for {table_name}: {e}")
    
    # ==================== ATOMIC DELETE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=30)
    @log_query_execution
    def delete(
        self,
        table_name: str,
        conditions: Dict[str, Any],
        returning: bool = False
    ) -> Union[int, List[Dict[str, Any]]]:
        """
        Atomic delete operation
        
        Args:
            table_name: Target table
            conditions: WHERE conditions
            returning: Whether to return deleted rows
            
        Returns:
            Number of deleted rows or list of deleted rows
        """
        start_time = time.time()
        table = self._get_table(table_name)
        
        try:
            # Build DELETE statement
            stmt = delete(table)
            
            # Apply WHERE conditions
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)
            
            if returning:
                stmt = stmt.returning(table)
            
            # Execute with atomic transaction
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                
                if returning:
                    rows = rows_to_dicts(result)
                    elapsed = time.time() - start_time
                    self._query_stats.record_write(
                        f"delete:{table_name}",
                        elapsed,
                        len(rows),
                        "success"
                    )
                    return rows
                else:
                    rowcount = result.rowcount
                    elapsed = time.time() - start_time
                    self._query_stats.record_write(
                        f"delete:{table_name}",
                        elapsed,
                        rowcount,
                        "success"
                    )
                    return rowcount
                
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"delete:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Delete failed for {table_name}: {e}")
    
    # ==================== ADVANCED QUERIES ====================
    
    @timeout(seconds=30)
    def read_one(
        self,
        table_name: str,
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Read single record"""
        options = QueryOptions(limit=1)
        results = self.read(table_name, conditions, options)
        return results[0] if results else None
    
    @timeout(seconds=30)
    def read_by_id(self, table_name: str, record_id: Any) -> Optional[Dict[str, Any]]:
        """Read record by ID"""
        return self.read_one(table_name, {"id": record_id})
    
    @timeout(seconds=30)
    def exists(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """Check if record exists"""
        table = self._get_table(table_name)
        stmt = select(func.count()).select_from(table)
        
        where_clauses = build_where_clause(table, conditions)
        if where_clauses:
            stmt = stmt.where(*where_clauses)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(stmt)
                count = result.scalar()
                return count > 0
        except SQLAlchemyError as e:
            raise DatabaseError(f"Exists check failed for {table_name}: {e}")
    
    @timeout(seconds=30)
    def count(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> int:
        """Count records"""
        table = self._get_table(table_name)
        stmt = select(func.count()).select_from(table)
        
        if conditions:
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(stmt)
                return result.scalar() or 0
        except SQLAlchemyError as e:
            raise DatabaseError(f"Count failed for {table_name}: {e}")
    
    # ==================== RAW SQL WITH ATOMIC SUPPORT ====================
    
    @timeout(seconds=60)
    @log_query_execution
    def execute_raw_sql(
        self,
        sql_query: str,
        parameters: Optional[Dict[str, Any]] = None,
        fetch_results: bool = True,
        atomic: bool = False
    ) -> Union[List[Dict[str, Any]], int, None]:
        """
        Execute raw SQL with atomic transaction support
        
        Args:
            sql_query: SQL query string
            parameters: Query parameters
            fetch_results: Whether to fetch results
            atomic: Whether to execute in transaction
            
        Returns:
            Query results or row count
        """
        start_time = time.time()
        sql_lower = sql_query.strip().lower()
        
        # Determine if query is a write operation
        is_write = sql_lower.startswith(("insert", "update", "delete", "truncate"))
        
        # Use transaction for writes or if explicitly requested
        use_transaction = atomic or is_write
        
        try:
            if use_transaction:
                with self.engine.begin() as conn:
                    result = conn.execute(text(sql_query), parameters or {})
                    
                    if fetch_results and result.returns_rows:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_raw_sql(
                            "raw_sql",
                            elapsed,
                            len(rows),
                            "success" if is_write else "read",
                            sql_lower[:50]
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        elapsed = time.time() - start_time
                        self._query_stats.record_raw_sql(
                            "raw_sql",
                            elapsed,
                            rowcount,
                            "write",
                            sql_lower[:50]
                        )
                        return rowcount
            else:
                with self.engine.connect() as conn:
                    result = conn.execute(text(sql_query), parameters or {})
                    
                    if fetch_results and result.returns_rows:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_raw_sql(
                            "raw_sql",
                            elapsed,
                            len(rows),
                            "read",
                            sql_lower[:50]
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        elapsed = time.time() - start_time
                        self._query_stats.record_raw_sql(
                            "raw_sql",
                            elapsed,
                            rowcount,
                            "write",
                            sql_lower[:50]
                        )
                        return rowcount
                    
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_raw_sql(
                "raw_sql",
                elapsed,
                0,
                "error",
                sql_lower[:50],
                str(e)
            )
            raise DatabaseError(f"Raw SQL execution failed: {e}")
    
    # ==================== TRANSACTION MANAGEMENT ====================
    
    @contextmanager
    def transaction(self, isolation_level: str = "READ COMMITTED"):
        """
        Context manager for database transactions
        
        Usage:
            with db.transaction() as tx:
                db.create("users", {...})
                db.update("profiles", {...}, {...})
        """
        with self._transaction_manager.begin(isolation_level=isolation_level) as conn:
            try:
                yield conn
            except Exception as e:
                logger.error(f"Transaction failed: {e}")
                raise TransactionError(f"Transaction failed: {e}")
    
    def begin_transaction(self, isolation_level: str = "READ COMMITTED"):
        """Begin a new transaction"""
        return self._transaction_manager.begin(isolation_level=isolation_level)
    
    # ==================== UTILITIES ====================
    
    def health_check(self) -> bool:
        """Check database health"""
        return self.connection_manager.health_check()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        pool = self.engine.pool
        
        return {
            "connection_pool": {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
            },
            "query_stats": self._query_stats.snapshot(),
            "schema": self.config.schema,
            "tables_cached": len(self._table_cache),
            "config": {
                "pool_size": self.config.pool_size,
                "max_overflow": self.config.max_overflow,
                "statement_timeout": self.config.statement_timeout,
                "use_ssl": self.config.use_ssl,
            }
        }
    
    def clear_cache(self):
        """Clear table cache"""
        self._table_cache.clear()
    
    def close(self):
        """Close database connection"""
        self.connection_manager.dispose()
        logger.info("Database connection closed")