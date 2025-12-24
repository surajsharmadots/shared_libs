# packages/core_postgres_db/core_postgres_db/sync_postgres.py
"""
Synchronous PostgreSQL client for legacy systems - Sonarqube compliant
"""
import logging
import time
from typing import Any, Dict, List, Optional, Union
from contextlib import contextmanager

from sqlalchemy import insert, select, update, delete, text, func, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from .base_crud import BaseDatabaseClient
from .base_operations import BaseDatabaseOperations
from .config import DatabaseConfig, get_database_config
from .constants import (
    DEFAULT_TIMEOUT_SECONDS, BULK_OPERATION_TIMEOUT, MAX_DEADLOCK_RETRIES,
    READ_COMMITTED
)
from .exceptions import (
    DatabaseError,TransactionError
)
from .types import (
    QueryOptions, BulkInsertOptions, UpdateOptions,
    PaginatedResult
)
from .decorators import retry_on_deadlock, timeout, log_query_execution
from .query_builder import QueryBuilder
from .transactions import TransactionManager
from .utils import rows_to_dicts, build_where_clause, chunk_list

logger = logging.getLogger(__name__)


class SyncPostgresDB(BaseDatabaseClient, BaseDatabaseOperations):
    """
    Synchronous PostgreSQL client for legacy systems
    
    Note: For e-commerce microservices, prefer AsyncPostgresDB
    This is provided for compatibility with sync codebases
    """
    
    def __init__(
        self,
        connection_string: Optional[str] = None,
        config: Optional[DatabaseConfig] = None,
        **kwargs
    ):
        """
        Initialize sync database connection
        
        Args:
            connection_string: PostgreSQL connection string
            config: Pre-configured DatabaseConfig object
            **kwargs: Configuration overrides
        """
        # Get configuration
        if config:
            self.config = config
        else:
            self.config = get_database_config(
                connection_string=connection_string,
                **kwargs
            )
        
        # Create sync engine
        self.engine: Engine = create_engine(
            self.config.connection_string,
            pool_size=self.config.pool_size,
            max_overflow=self.config.max_overflow,
            pool_timeout=self.config.pool_timeout,
            pool_recycle=self.config.pool_recycle,
            echo=self.config.echo,
            pool_pre_ping=True,
        )
        
        # Initialize metadata
        from sqlalchemy import MetaData
        self.metadata = MetaData(schema=self.config.schema)
        
        # Initialize base operations
        BaseDatabaseOperations.__init__(self, self.config, self.engine, self.metadata)
        
        # Initialize components
        self._query_builder = QueryBuilder()
        self._transaction_manager = TransactionManager(self.engine)
        
        logger.info(f"SyncPostgresDB initialized for schema: {self.config.schema}")
    
    # ==================== ATOMIC CREATE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    def create(
        self,
        table_name: str,
        data: Dict[str, Any],
        returning: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Atomic sync insert operation
        
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
            # Validate data
            self._validate_data(data, "create")
            
            # Build and execute INSERT
            stmt = insert(table).values(**data)
            if returning:
                stmt = stmt.returning(table)
            
            with self.engine.begin() as conn:
                result = conn.execute(stmt)
                
                if returning:
                    row = result.fetchone()
                    rows_affected = 1 if row else 0
                    self._record_query_metrics(
                        operation="create",
                        table_name=table_name,
                        start_time=start_time,
                        rows_affected=rows_affected
                    )
                    return dict(row._mapping) if row else None
                
                self._record_query_metrics(
                    operation="create",
                    table_name=table_name,
                    start_time=start_time,
                    rows_affected=1
                )
                return None
                
        except IntegrityError as e:
            self._handle_integrity_error(e, "create", table_name)
        except SQLAlchemyError as e:
            self._handle_database_error(e, "create", table_name)
    
    # ==================== ATOMIC BULK CREATE ====================
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=BULK_OPERATION_TIMEOUT)
    @log_query_execution
    def bulk_create(
        self,
        table_name: str,
        data_list: List[Dict[str, Any]],
        options: Optional[BulkInsertOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Atomic sync bulk insert with batch processing
        
        Args:
            table_name: Target table
            data_list: List of data dictionaries
            options: Bulk insert configuration
            
        Returns:
            List of inserted rows
        """
        if not data_list:
            return []
        
        opts = options or BulkInsertOptions()
        table = self._get_table(table_name)
        all_rows = []
        
        start_time = time.time()
        
        try:
            # Execute in single atomic transaction
            with self.engine.begin() as conn:
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
                        result = conn.execute(stmt)
                        all_rows.extend(rows_to_dicts(result))
                    else:
                        conn.execute(stmt)
                
                # Record metrics
                self._record_query_metrics(
                    operation="bulk_create",
                    table_name=table_name,
                    start_time=start_time,
                    rows_affected=len(data_list)
                )
                return all_rows
                
        except IntegrityError as e:
            self._handle_integrity_error(e, "bulk_create", table_name)
        except SQLAlchemyError as e:
            self._handle_database_error(e, "bulk_create", table_name)
    
    # ==================== READ OPERATIONS ====================
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    def read(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        options: Optional[QueryOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Sync read records with filtering
        
        Args:
            table_name: Target table
            conditions: WHERE conditions
            options: Query options
            
        Returns:
            List of records
        """
        start_time = time.time()
        table = self._get_table(table_name)
        
        # Validate conditions
        if conditions:
            self._validate_conditions(conditions)
        
        try:
            # Build query
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
            
            # Execute query
            with self.engine.connect() as conn:
                result = conn.execute(stmt)
                rows = rows_to_dicts(result)
                
                # Record metrics
                self._record_query_metrics(
                    operation="read",
                    table_name=table_name,
                    start_time=start_time,
                    rows_affected=len(rows)
                )
                return rows
                
        except SQLAlchemyError as e:
            self._handle_database_error(e, "read", table_name)
    
    # ==================== ATOMIC UPDATE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    def update(
        self,
        table_name: str,
        data: Dict[str, Any],
        conditions: Dict[str, Any],
        options: Optional[UpdateOptions] = None
    ) -> List[Dict[str, Any]]:
        """
        Atomic sync update operation
        
        Args:
            table_name: Target table
            data: Data to update
            conditions: WHERE conditions
            options: Update configuration
            
        Returns:
            List of updated rows
        """
        start_time = time.time()
        table = self._get_table(table_name)
        opts = options or UpdateOptions()
        
        # Validate
        self._validate_data(data, "update")
        self._validate_conditions(conditions)
        
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
                with self.engine.begin() as conn:
                    result = conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        self._record_query_metrics(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        self._record_query_metrics(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return []
            else:
                with self.engine.connect() as conn:
                    result = conn.execute(stmt)
                    
                    if opts.returning:
                        rows = rows_to_dicts(result)
                        self._record_query_metrics(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        self._record_query_metrics(
                            operation="update",
                            table_name=table_name,
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return []
                
        except IntegrityError as e:
            self._handle_integrity_error(e, "update", table_name)
        except SQLAlchemyError as e:
            self._handle_database_error(e, "update", table_name)
    
    # ==================== ATOMIC DELETE OPERATIONS ====================
    
    @retry_on_deadlock(max_retries=MAX_DEADLOCK_RETRIES)
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    @log_query_execution
    def delete(
        self,
        table_name: str,
        conditions: Dict[str, Any],
        returning: bool = False
    ) -> Union[int, List[Dict[str, Any]]]:
        """
        Atomic sync delete operation
        
        Args:
            table_name: Target table
            conditions: WHERE conditions
            returning: Whether to return deleted rows
            
        Returns:
            Number of deleted rows or list of deleted rows
        """
        start_time = time.time()
        table = self._get_table(table_name)
        
        self._validate_conditions(conditions)
        
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
                    self._record_query_metrics(
                        operation="delete",
                        table_name=table_name,
                        start_time=start_time,
                        rows_affected=len(rows)
                    )
                    return rows
                else:
                    rowcount = result.rowcount
                    self._record_query_metrics(
                        operation="delete",
                        table_name=table_name,
                        start_time=start_time,
                        rows_affected=rowcount
                    )
                    return rowcount
                
        except SQLAlchemyError as e:
            self._handle_database_error(e, "delete", table_name)
    
    # ==================== QUERY METHODS ====================
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    def read_one(
        self,
        table_name: str,
        conditions: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Read single record"""
        options = QueryOptions(limit=1)
        results = self.read(table_name, conditions, options)
        return results[0] if results else None
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
    def read_by_id(self, table_name: str, record_id: Any) -> Optional[Dict[str, Any]]:
        """Read record by ID"""
        return self.read_one(table_name, {"id": record_id})
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
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
    
    @timeout(seconds=DEFAULT_TIMEOUT_SECONDS)
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
    
    def paginate(
        self,
        table_name: str,
        conditions: Optional[Dict[str, Any]] = None,
        page: int = 1,
        per_page: int = 20,
        order_by: Optional[List[tuple]] = None
    ) -> PaginatedResult:
        """
        Paginate records
        
        Args:
            table_name: Target table
            conditions: Filter conditions
            page: Page number (1-indexed)
            per_page: Items per page
            order_by: Sorting criteria
            
        Returns:
            Paginated result with metadata
        """
        # Get total count
        total = self.count(table_name, conditions)
        
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
        items = self.read(table_name, conditions, options)
        
        return PaginatedResult(
            items=items,
            total=total,
            page=page,
            per_page=per_page,
            total_pages=total_pages
        )
    
    # ==================== RAW SQL WITH ATOMIC SUPPORT ====================
    
    @timeout(seconds=BULK_OPERATION_TIMEOUT)
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
                        self._record_query_metrics(
                            operation="raw_sql_write",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        self._record_query_metrics(
                            operation="raw_sql_write",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return rowcount
            else:
                with self.engine.connect() as conn:
                    result = conn.execute(text(sql_query), parameters or {})
                    
                    if fetch_results and result.returns_rows:
                        rows = rows_to_dicts(result)
                        self._record_query_metrics(
                            operation="raw_sql_read",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=len(rows)
                        )
                        return rows
                    else:
                        rowcount = result.rowcount
                        self._record_query_metrics(
                            operation="raw_sql_write",
                            table_name="raw_sql",
                            start_time=start_time,
                            rows_affected=rowcount
                        )
                        return rowcount
                    
        except SQLAlchemyError as e:
            self._handle_database_error(e, "raw_sql", "raw_sql")
    
    # ==================== TRANSACTION MANAGEMENT ====================
    
    @contextmanager
    def transaction(self, isolation_level: str = READ_COMMITTED):
        """
        Context manager for database transactions
        
        Usage:
            with db.transaction():
                db.create("users", user_data)
                db.update("profiles", profile_data, conditions)
        """
        with self._transaction_manager.begin(isolation_level=isolation_level) as conn:
            try:
                yield conn
            except Exception as e:
                logger.error(f"Transaction failed: {e}")
                raise TransactionError(f"Transaction failed: {e}")
    
    # ==================== ASYNC METHODS (NOT IMPLEMENTED) ====================
    
    async def acreate(self, *args, **kwargs):
        """Async create (not available in sync client)"""
        raise NotImplementedError("Async methods not available in SyncPostgresDB")
    
    async def aread(self, *args, **kwargs):
        """Async read (not available in sync client)"""
        raise NotImplementedError("Async methods not available in SyncPostgresDB")
    
    async def aupdate(self, *args, **kwargs):
        """Async update (not available in sync client)"""
        raise NotImplementedError("Async methods not available in SyncPostgresDB")
    
    async def adelete(self, *args, **kwargs):
        """Async delete (not available in sync client)"""
        raise NotImplementedError("Async methods not available in SyncPostgresDB")
    
    async def atransaction(self, *args, **kwargs):
        """Async transaction (not available in sync client)"""
        raise NotImplementedError("Async methods not available in SyncPostgresDB")
    
    # ==================== UTILITIES ====================
    
    def health_check(self) -> bool:
        """Check database health"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
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
            "query_stats": self.get_query_stats(),
            "schema": self.config.schema,
            "tables_cached": len(self._table_cache),
            "config": {
                "pool_size": self.config.pool_size,
                "max_overflow": self.config.max_overflow,
                "statement_timeout": self.config.statement_timeout,
                "use_ssl": self.config.use_ssl,
            }
        }
    
    def close(self):
        """Close database connection"""
        self.engine.dispose()
        logger.info("Sync database connection closed")