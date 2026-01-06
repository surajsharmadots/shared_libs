# packages/core_postgres_db/core_postgres_db/async_postgres.py
"""
Async PostgreSQL client - uses AsyncDatabaseOperations
"""
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Union
from contextlib import asynccontextmanager

from sqlalchemy import insert, select, update, delete, text, func
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import MetaData

from .base_crud import AsyncBaseDatabaseClient
from .async_operations import AsyncDatabaseOperations 
from .config import DatabaseConfig, get_database_config
from .constants import (
    DEFAULT_TIMEOUT_SECONDS, BULK_OPERATION_TIMEOUT, MAX_DEADLOCK_RETRIES,
    READ_COMMITTED
)
from .exceptions import DatabaseError, TransactionError
from .types import QueryOptions, BulkInsertOptions, UpdateOptions, PaginatedResult
from .decorators import retry_on_deadlock, timeout, log_query_execution
from .query_builder import QueryBuilder
from .transactions import AsyncTransactionManager
from .utils import rows_to_dicts, build_where_clause, chunk_list

logger = logging.getLogger(__name__)


class AsyncPostgresDB(AsyncBaseDatabaseClient):
    """
    Async PostgreSQL client
    Uses AsyncDatabaseOperations (NOT BaseDatabaseOperations)
    """
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        config: Optional[DatabaseConfig] = None,
        **kwargs
    ):
        # Get configuration
        if config:
            self.config = config
        else:
            self.config = get_database_config(
                connection_string=connection_string,
                **kwargs
            )
        
        # Create async engine
        async_url = self.config.connection_string.replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        
        self.async_engine: AsyncEngine = create_async_engine(
            async_url,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            echo=self.config.echo,
            pool_pre_ping=True,
        )
        
        # Initialize metadata
        self.metadata = MetaData(schema=self.config.schema)
        
        # Initialize ASYNC operations (NOT sync)
        self.async_ops = AsyncDatabaseOperations(
            config=self.config,
            async_engine=self.async_engine,
            metadata=self.metadata
        )
        
        # Initialize components
        self._query_builder = QueryBuilder()
        self._transaction_manager = AsyncTransactionManager(self.async_engine)
        
        logger.info(f"AsyncPostgresDB initialized for schema: {self.config.schema}")
    
    # ============= ASYNC METHODS =============
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    async def acreate(
        self,
        table_name: str,
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """Async create"""
        start_time = time.time()
        
        # Get table using async operations
        table = await self.async_ops._get_table(table_name)
        
        try:
            # Validate data
            self.async_ops._validate_data_async(data, "create")
            
            # Build and execute INSERT
            stmt = insert(table).values(**data)
            if returning:
                stmt = stmt.returning(table)
            
            async with self.async_engine.begin() as conn:
                result = await conn.execute(stmt)
                
                if returning:
                    row = result.fetchone()
                    rows_affected = 1 if row else 0
                    await self.async_ops._record_query_metrics_async(
                        operation="create",
                        table_name=table_name,
                        start_time=start_time,
                        rows_affected=rows_affected
                    )
                    return dict(row._mapping) if row else None
                
                await self.async_ops._record_query_metrics_async(
                    operation="create",
                    table_name=table_name,
                    start_time=start_time,
                    rows_affected=1
                )
                return None
                
        except IntegrityError as e:
            self.async_ops._handle_integrity_error_async(e, "create", table_name)
        except SQLAlchemyError as e:
            self.async_ops._handle_database_error_async(e, "create", table_name)
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    async def aread(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """Async read"""
        start_time = time.time()
        
        # Get table using async operations
        table = await self.async_ops._get_table(table_name)
        
        # Validate conditions
        if conditions:
            self.async_ops._validate_conditions_async(conditions)
        
        try:
            # Build query
            if options is not None and isinstance(options, dict):
                options = QueryOptions(**options)
            elif options is None:
                options = QueryOptions()
                
            stmt = self._query_builder.build_select_query(
                table=table,
                columns=options.columns,
                conditions=conditions,
                order_by=options.order_by,
                limit=options.limit,
                offset=options.offset,
                distinct=options.distinct,
                for_update=options.for_update
            )
            
            # Execute query
            async with self.async_engine.connect() as conn:
                result = await conn.execute(stmt)
                rows = rows_to_dicts(result)
                
                # Record metrics
                await self.async_ops._record_query_metrics_async(
                    operation="read",
                    table_name=table_name,
                    start_time=start_time,
                    rows_affected=len(rows)
                )
                return rows
                
        except SQLAlchemyError as e:
            self.async_ops._handle_database_error_async(e, "read", table_name)
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    async def aupdate(
        self,
        table_name: str,
        data: Dict[str, Any],
        conditions: Dict[str, Any],
        options: Optional[UpdateOptions] = None
    ) -> List[Dict[str, Any]]:
        """Async update"""
        start_time = time.time()
        
        # Get table using async operations
        table = await self.async_ops._get_table(table_name)
        
        if options is not None and isinstance(options, dict):
            opts = UpdateOptions(**options)
        elif options is None:
            opts = UpdateOptions()
        
        # Validate
        self.async_ops._validate_data_async(data, "update")
        self.async_ops._validate_conditions_async(conditions)
        
        try:
            # Build UPDATE statement
            stmt = update(table).values(**data)
            
            # Apply WHERE conditions
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)

            if opts.returning:
                stmt = stmt.returning(table)
            
            # Execute with appropriate context
            if opts.atomic:
                async with self.async_engine.begin() as conn:
                    result = await conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        await self.async_ops._record_query_metrics_async(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        await self.async_ops._record_query_metrics_async(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return []
            else:
                async with self.async_engine.connect() as conn:
                    result = await conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        await self.async_ops._record_query_metrics_async(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        await self.async_ops._record_query_metrics_async(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return []
                
        except IntegrityError as e:
            self.async_ops._handle_integrity_error_async(e, "update", table_name)
        except SQLAlchemyError as e:
            self.async_ops._handle_database_error_async(e, "update", table_name)
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    async def adelete(
        self,
        table_name: str,
        conditions: Dict[str, Any],
        returning: bool = False
    ) -> Union[int, List[Dict[str, Any]]]:
        """Async delete"""
        start_time = time.time()
        
        # Get table using async operations
        table = await self.async_ops._get_table(table_name)
        
        self.async_ops._validate_conditions_async(conditions)
        
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
                    await self.async_ops._record_query_metrics_async(
                        operation="delete",
                        table_name=table_name,
                        start_time=start_time,
                        rows_affected=len(rows)
                    )
                    return rows
                else:
                    rowcount = result.rowcount
                    await self.async_ops._record_query_metrics_async(
                        operation="delete",
                        table_name=table_name,
                        start_time=start_time,
                        rows_affected=rowcount
                    )
                    return rowcount
                
        except SQLAlchemyError as e:
            self.async_ops._handle_database_error_async(e, "delete", table_name)
    
    # ============= E-COMMERCE SPECIFIC METHODS =============
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    async def aread_one(
        self,
        table_name: str,
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Read single record"""
        options = QueryOptions(limit=1)
        results = await self.aread(table_name, conditions, options)
        return results[0] if results else None
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    async def aread_by_id(self, table_name: str, record_id: Any) -> Optional[Dict[str, Any]]:
        """Read record by ID"""
        return await self.aread_one(table_name, {"id": record_id})
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    async def aexists(self, table_name: str, conditions: Dict[str, Any]) -> bool:
        """Check if record exists"""
        table = await self.async_ops._get_table(table_name)
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
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    async def acount(self, table_name: str, conditions: Optional[Dict[str, Any]] = None) -> int:
        """Count records"""
        table = await self.async_ops._get_table(table_name)
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
    
    async def apaginate(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        order_by: Optional[List[tuple]] = None
    ) -> PaginatedResult:
        """Async pagination"""
        # Get total count
        total = await self.acount(table_name, conditions)
        
        # Calculate pagination
        total_pages = (total + per_page - 1) // per_page if total > 0 else 1
        page = max(1, min(page, total_pages))
        
        # Build query options
        options = QueryOptions(
            limit=per_page,
            offset=(page - 1) * per_page,
            order_by=order_by
        )
        
        # Get items
        items = await self.aread(table_name, conditions, options)
        
        return PaginatedResult(
            items=items,
            total=total,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
    
    # ============= BULK OPERATIONS =============
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=BULK_OPERATION_TIMEOUT)
    @log_query_execution
    async def abulk_create(
        self,
        table_name: str,
        data_list: List[Dict[str, Any]],
        options: Optional[BulkInsertOptions] = None
    ) -> List[Dict[str, Any]]:
        """Async bulk create"""
        if not data_list:
            return []
        
        opts = options or BulkInsertOptions()
        table = await self.async_ops._get_table(table_name)
        all_rows = []
        
        start_time = time.time()
        
        try:
            # Execute in single atomic transaction
            async with self.async_engine.begin() as conn:
                # Process in batches
                for batch in chunk_list(data_list, opts.batch_size):
                    stmt = insert(table).values(batch)
                    
                    # Handle conflicts
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
                
                # Record metrics
                await self.async_ops._record_query_metrics_async(
                    operation="bulk_create",
                    table_name=table_name,
                    start_time=start_time,
                    rows_affected=len(data_list)
                )
                return all_rows
                
        except IntegrityError as e:
            self.async_ops._handle_integrity_error_async(e, "bulk_create", table_name)
        except SQLAlchemyError as e:
            self.async_ops._handle_database_error_async(e, "bulk_create", table_name)
    
    # ============= RAW SQL =============
    
    @timeout(seconds=BULK_OPERATION_TIMEOUT)
    @log_query_execution
    async def aexecute_raw_sql(
        self,
        sql_query: str,
        parameters: Optional[Dict[str, Any]] = None,
        fetch_results: bool = True,
        atomic: bool = False
    ) -> Union[List[Dict[str, Any]], int, None]:
        """Execute raw SQL"""
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
                        await self.async_ops._record_query_metrics_async(
                            operation="raw_sql_write",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        await self.async_ops._record_query_metrics_async(
                            operation="raw_sql_write",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return rowcount
            else:
                async with self.async_engine.connect() as conn:
                    result = await conn.execute(text(sql_query), parameters or {})
                    
                    if fetch_results and result.returns_rows:
                        rows = rows_to_dicts(result)
                        await self.async_ops._record_query_metrics_async(
                            operation="raw_sql_read",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        await self.async_ops._record_query_metrics_async(
                            operation="raw_sql_write",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return rowcount
                    
        except SQLAlchemyError as e:
            self.async_ops._handle_database_error_async(e, "raw_sql", "raw_sql")
    
    # ============= TRANSACTIONS =============
    
    @asynccontextmanager
    async def atransaction(self, isolation_level: str = READ_COMMITTED):
        """Async transaction"""
        async with self._transaction_manager.begin(isolation_level=isolation_level) as conn:
            try:
                yield conn
            except Exception as e:
                logger.error(f"Async transaction failed: {e}")
                raise TransactionError(f"Async transaction failed: {e}")
    
    # ============= UTILITIES =============
    
    async def ahealth_check(self) -> bool:
        """Async health check"""
        try:
            async with self.async_engine.connect() as conn:
                result = await conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Async health check failed: {e}")
            return False
    
    async def aget_stats(self) -> Dict[str, Any]:
        """Get async stats"""
        pool = self.async_engine.pool
        
        return {
            "connection_pool": {
                "size": pool.size(),
                "checked_in": pool.checkedin(),
                "checked_out": pool.checkedout(),
                "overflow": pool.overflow(),
            },
            "query_stats": await self.async_ops.get_query_stats_async(),
            "schema": self.config.schema,
            "tables_cached": len(self.async_ops._table_cache),
            "config": {
                "pool_size": self.config.pool_size,
                "max_overflow": self.config.max_overflow,
                "statement_timeout": self.config.statement_timeout,
                "use_ssl": self.config.use_ssl,
            }
        }
    
    async def aclose(self):
        """Close async connection"""
        await self.async_engine.dispose()
        logger.info("Async database connection closed")
    
    # ============= ABSTRACT METHODS IMPLEMENTATION =============
    
    async def create(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.acreate(*args, **kwargs)
    
    async def read(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.aread(*args, **kwargs)
    
    async def update(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.aupdate(*args, **kwargs)
    
    async def delete(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.adelete(*args, **kwargs)
    
    async def bulk_create(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.abulk_create(*args, **kwargs)
    
    async def count(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.acount(*args, **kwargs)
    
    async def execute_raw_sql(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.aexecute_raw_sql(*args, **kwargs)
    
    async def exists(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.aexists(*args, **kwargs)
    
    async def paginate(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.apaginate(*args, **kwargs)
    
    async def read_by_id(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.aread_by_id(*args, **kwargs)
    
    async def read_one(self, *args, **kwargs):
        """Required by abstract class"""
        return await self.aread_one(*args, **kwargs)
    
    @asynccontextmanager
    async def transaction(self, isolation_level: str = "READ COMMITTED"):
        """Required by abstract class"""
        async with self.atransaction(isolation_level=isolation_level) as conn:
            yield conn
    
    def health_check(self) -> bool:
        """Sync health check"""
        return asyncio.run(self.ahealth_check())
    
    def get_stats(self) -> Dict[str, Any]:
        """Sync get stats"""
        return asyncio.run(self.aget_stats())
    
    def close(self):
        """Sync close"""
        asyncio.run(self.aclose())