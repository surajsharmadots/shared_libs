"""
Asynchronous CRUD operations with atomic transactions
"""
import time
import logging
from typing import Any, Dict, List, Optional, Union
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import Table, insert, select, update, delete, text, func
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncConnection
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

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
from .transactions import AsyncTransactionManager

logger = logging.getLogger(__name__)

class AsyncPostgresDB:
    """
    Asynchronous PostgreSQL operations with full atomic support
    
    Features:
    - All writes are atomic by default
    - Async connection pooling
    - Automatic retry on deadlocks
    - Query timeouts
    - Comprehensive error handling
    """
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        config: Optional[DatabaseConfig] = None,
        **kwargs
    ):
        """
        Initialize async database connection
        
        Args:
            connection_string: PostgreSQL connection string (optional)
            config: DatabaseConfig object (optional)
            **kwargs: Database configuration parameters
        """
        from .connection import get_database_config
        
        if config:
            self.config = config
        else:
            self.config = get_database_config(
                connection_string=connection_string,
                **kwargs
            )
        
        self.connection_manager = ConnectionManager(self.config)
        self.async_engine: AsyncEngine = self.connection_manager.get_async_engine()
        self.metadata = self.connection_manager.metadata
        
        # Cache for table metadata
        self._table_cache: Dict[str, Table] = {}
        self._query_stats = QueryStats()
        self._query_builder = QueryBuilder()
        self._transaction_manager = AsyncTransactionManager(self.async_engine)
        
        logger.info(f"AsyncPostgresDB initialized for schema: {self.config.schema}")
    
    # ==================== TABLE MANAGEMENT ====================
    
    def _get_table(self, table_name: str) -> Table:
        """Get table with caching and schema support"""
        safe_name = safe_table_ref(table_name)
        
        if safe_name not in self._table_cache:
            # Note: Async engine doesn't support autoload directly
            # We'll use sync engine for table reflection
            sync_engine = self.connection_manager.get_sync_engine()
            table = Table(
                safe_name,
                self.metadata,
                autoload_with=sync_engine,
                schema=self.config.schema,
                extend_existing=True
            )
            self._table_cache[safe_name] = table
        
        return self._table_cache[safe_name]
    
    # ==================== ATOMIC ASYNC CREATE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=30)
    @log_query_execution
    async def acreate(
        self,
        table_name: str,
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Atomic async insert operation
        
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
            async with self.async_engine.begin() as conn:
                result = await conn.execute(stmt)
                
                if returning:
                    row = result.fetchone()
                    elapsed = time.time() - start_time
                    self._query_stats.record_write(
                        f"async_create:{table_name}",
                        elapsed,
                        1,
                        "success"
                    )
                    return dict(row._mapping) if row else None
                
                elapsed = time.time() - start_time
                self._query_stats.record_write(
                    f"async_create:{table_name}",
                    elapsed,
                    1,
                    "success"
                )
                return None
                
        except IntegrityError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"async_create:{table_name}",
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
                f"async_create:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Async insert failed for {table_name}: {e}")
    
    # ==================== ATOMIC ASYNC BULK CREATE ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=120)
    @log_query_execution
    async def abulk_create(
        self,
        table_name: str,
        data_list: List[Dict[str, Any]],
        options: Optional[BulkInsertOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Atomic async bulk insert with batch processing
        
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
            async with self.async_engine.begin() as conn:
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
                        result = await conn.execute(stmt)
                        all_rows.extend(rows_to_dicts(result))
                    else:
                        await conn.execute(stmt)
                
                elapsed = time.time() - start_time
                self._query_stats.record_write(
                    f"async_bulk_create:{table_name}",
                    elapsed,
                    total_rows,
                    "success"
                )
                return all_rows
                
        except IntegrityError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"async_bulk_create:{table_name}",
                elapsed,
                0,
                "integrity_error",
                str(e)
            )
            
            if "duplicate key" in str(e).lower():
                raise DuplicateEntryError(f"Duplicate in async bulk insert to {table_name}: {e}")
            raise ConstraintViolationError(f"Constraint violation in async bulk insert to {table_name}: {e}")
            
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"async_bulk_create:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Async bulk insert failed for {table_name}: {e}")
    
    # ==================== ASYNC READ OPERATIONS ====================
    
    @timeout(seconds=30)
    @log_query_execution
    async def aread(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Async read records with filtering
        
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
            
            async with self.async_engine.connect() as conn:
                result = await conn.execute(stmt)
                rows = rows_to_dicts(result)
                
                elapsed = time.time() - start_time
                self._query_stats.record_read(
                    f"async_read:{table_name}",
                    elapsed,
                    len(rows),
                    "success"
                )
                return rows
                
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_read(
                f"async_read:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Async read failed for {table_name}: {e}")
    
    # ==================== ATOMIC ASYNC UPDATE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=30)
    @log_query_execution
    async def aupdate(
        self,
        table_name: str,
        data: Dict[str, Any],
        conditions: Dict[str, Any],
        options: Optional[UpdateOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Atomic async update operation
        
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
                async with self.async_engine.begin() as conn:
                    result = await conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"async_update:{table_name}",
                            elapsed,
                            len(rows),
                            "success"
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"async_update:{table_name}",
                            elapsed,
                            rowcount,
                            "success"
                        )
                        return []
            else:
                async with self.async_engine.connect() as conn:
                    result = await conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"async_update:{table_name}",
                            elapsed,
                            len(rows),
                            "success"
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        elapsed = time.time() - start_time
                        self._query_stats.record_write(
                            f"async_update:{table_name}",
                            elapsed,
                            rowcount,
                            "success"
                        )
                        return []
                
        except IntegrityError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"async_update:{table_name}",
                elapsed,
                0,
                "integrity_error",
                str(e)
            )
            raise ConstraintViolationError(f"Async update constraint violation in {table_name}: {e}")
            
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"async_update:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Async update failed for {table_name}: {e}")
    
    # ==================== ATOMIC ASYNC DELETE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=3)
    @timeout(seconds=30)
    @log_query_execution
    async def adelete(
        self,
        table_name: str,
        conditions: Dict[str, Any],
        returning: bool = False
    ) -> Union[int, List[Dict[str, Any]]]:
        """
        Atomic async delete operation
        
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
            async with self.async_engine.begin() as conn:
                result = await conn.execute(stmt)
                
                if returning:
                    rows = rows_to_dicts(result)
                    elapsed = time.time() - start_time
                    self._query_stats.record_write(
                        f"async_delete:{table_name}",
                        elapsed,
                        len(rows),
                        "success"
                    )
                    return rows
                else:
                    rowcount = result.rowcount
                    elapsed = time.time() - start_time
                    self._query_stats.record_write(
                        f"async_delete:{table_name}",
                        elapsed,
                        rowcount,
                        "success"
                    )
                    return rowcount
                
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_write(
                f"async_delete:{table_name}",
                elapsed,
                0,
                "error",
                str(e)
            )
            raise DatabaseError(f"Async delete failed for {table_name}: {e}")
    
    # ==================== ADVANCED ASYNC QUERIES ====================
    
    @timeout(seconds=30)
    async def aread_one(
        self,
        table_name: str,
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Async read single record"""
        options = QueryOptions(limit=1)
        results = await self.aread(table_name, conditions, options)
        return results[0] if results else None
    
    @timeout(seconds=30)
    async def aread_by_id(self, table_name: str, record_id: Any) -> Optional[Dict[str, Any]]:
        """Async read record by ID"""
        return await self.aread_one(table_name, {"id": record_id})
    
    @timeout(seconds=30)
    async def aexists(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """Async check if record exists"""
        table = self._get_table(table_name)
        stmt = select(func.count()).select_from(table)
        
        where_clauses = build_where_clause(table, conditions)
        if where_clauses:
            stmt = stmt.where(*where_clauses)
        
        try:
            async with self.async_engine.connect() as conn:
                result = await conn.execute(stmt)
                count = result.scalar()
                return count > 0
        except SQLAlchemyError as e:
            raise DatabaseError(f"Async exists check failed for {table_name}: {e}")
    
    @timeout(seconds=30)
    async def acount(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> int:
        """Async count records"""
        table = self._get_table(table_name)
        stmt = select(func.count()).select_from(table)
        
        if conditions:
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)
        
        try:
            async with self.async_engine.connect() as conn:
                result = await conn.execute(stmt)
                return result.scalar() or 0
        except SQLAlchemyError as e:
            raise DatabaseError(f"Async count failed for {table_name}: {e}")
    
    # ==================== ASYNC RAW SQL WITH ATOMIC SUPPORT ====================
    
    @timeout(seconds=60)
    @log_query_execution
    async def aexecute_raw_sql(
        self,
        sql_query: str,
        parameters: Optional[Dict[str, Any]] = None,
        fetch_results: bool = True,
        atomic: bool = False
    ) -> Union[List[Dict[str, Any]], int, None]:
        """
        Async execute raw SQL with atomic transaction support
        
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
                async with self.async_engine.begin() as conn:
                    result = await conn.execute(text(sql_query), parameters or {})
                    
                    if fetch_results and result.returns_rows:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_raw_sql(
                            "async_raw_sql",
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
                            "async_raw_sql",
                            elapsed,
                            rowcount,
                            "write",
                            sql_lower[:50]
                        )
                        return rowcount
            else:
                async with self.async_engine.connect() as conn:
                    result = await conn.execute(text(sql_query), parameters or {})
                    
                    if fetch_results and result.returns_rows:
                        rows = rows_to_dicts(result)
                        elapsed = time.time() - start_time
                        self._query_stats.record_raw_sql(
                            "async_raw_sql",
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
                            "async_raw_sql",
                            elapsed,
                            rowcount,
                            "write",
                            sql_lower[:50]
                        )
                        return rowcount
                    
        except SQLAlchemyError as e:
            elapsed = time.time() - start_time
            self._query_stats.record_raw_sql(
                "async_raw_sql",
                elapsed,
                0,
                "error",
                sql_lower[:50],
                str(e)
            )
            raise DatabaseError(f"Async raw SQL execution failed: {e}")
    
    # ==================== ASYNC TRANSACTION MANAGEMENT ====================
    
    @asynccontextmanager
    async def atransaction(self, isolation_level: str = "READ COMMITTED"):
        """
        Async context manager for database transactions
        
        Usage:
            async with db.atransaction() as tx:
                await db.acreate("users", {...})
                await db.aupdate("profiles", {...}, {...})
        """
        async with self._transaction_manager.begin(isolation_level=isolation_level) as conn:
            try:
                yield conn
            except Exception as e:
                logger.error(f"Async transaction failed: {e}")
                raise TransactionError(f"Async transaction failed: {e}")
    
    # ==================== UTILITIES ====================
    
    async def ahealth_check(self) -> bool:
        """Async check database health"""
        try:
            async with self.async_engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Async health check failed: {e}")
            return False
    
    async def aget_stats(self) -> Dict[str, Any]:
        """Get async database statistics"""
        pool = self.async_engine.pool
        
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
    
    async def aclose(self):
        """Close async database connection"""
        await self.async_engine.dispose()
        logger.info("Async database connection closed")