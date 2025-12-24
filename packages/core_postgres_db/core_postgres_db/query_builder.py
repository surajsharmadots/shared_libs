# packages/core_postgres_db/core_postgres_db/query_builder.py
"""
Advanced query building utilities - Sonarqube compliant
"""
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import Table, select, desc, asc, func
from sqlalchemy.sql import Select

from .utils import build_where_clause
from .constants import MAX_QUERY_LIMIT


class QueryBuilder:
    """Builds complex SQL queries with validation"""
    
    @staticmethod
    def build_select_query(
        table: Table,
        columns: Optional[List[str]] = None,
        conditions: Optional[Dict[str, Any]] = None,
        order_by: Optional[List[Tuple[str, bool]]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
        distinct: bool = False,
        for_update: bool = False,
        join_tables: Optional[List[Dict[str, Any]]] = None
    ) -> Select:
        """
        Build SELECT query with advanced features
        
        Args:
            table: SQLAlchemy Table object
            columns: List of column names to select
            conditions: WHERE conditions
            order_by: List of (column, ascending) tuples
            limit: Maximum number of rows
            offset: Number of rows to skip
            distinct: Whether to select distinct rows
            for_update: Whether to lock rows for update
            join_tables: List of join configurations
            
        Returns:
            SQLAlchemy Select statement
        """
        # Validate inputs
        QueryBuilder._validate_select_params(
            limit=limit, offset=offset, columns=columns
        )
        
        # Select columns
        if columns:
            cols = [table.c[col] for col in columns if col in table.c]
            if not cols:
                raise ValueError(f"No valid columns found in table {table.name}")
        else:
            cols = [table]
        
        # Start building query
        if distinct:
            stmt = select(*cols).distinct()
        else:
            stmt = select(*cols)
        
        # Add WHERE conditions
        if conditions:
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)
        
        # Add JOINs
        if join_tables:
            stmt = QueryBuilder._add_joins(stmt, table, join_tables)
        
        # Add ORDER BY
        if order_by:
            order_clauses = []
            for col_name, ascending in order_by:
                if col_name in table.c:
                    col = table.c[col_name]
                    order_clauses.append(asc(col) if ascending else desc(col))
            if order_clauses:
                stmt = stmt.order_by(*order_clauses)
        
        # Add LIMIT and OFFSET
        if limit is not None:
            stmt = stmt.limit(limit)
        if offset > 0:
            stmt = stmt.offset(offset)
        
        # Add FOR UPDATE if needed
        if for_update:
            stmt = stmt.with_for_update()
        
        return stmt
    
    @staticmethod
    def build_aggregate_query(
        table: Table,
        aggregate_func: str,
        aggregate_column: str,
        group_by: Optional[List[str]] = None,
        conditions: Optional[Dict[str, Any]] = None,
        having: Optional[Dict[str, Any]] = None
    ) -> Select:
        """
        Build aggregate query (SUM, COUNT, AVG, etc.)
        
        Args:
            table: SQLAlchemy Table object
            aggregate_func: Aggregate function (sum, count, avg, min, max)
            aggregate_column: Column to aggregate
            group_by: Columns to group by
            conditions: WHERE conditions
            having: HAVING conditions
            
        Returns:
            SQLAlchemy Select statement
        """
        # Validate aggregate function
        func_map = {
            "sum": func.sum,
            "count": func.count,
            "avg": func.avg,
            "min": func.min,
            "max": func.max
        }
        
        if aggregate_func not in func_map:
            raise ValueError(
                f"Unsupported aggregate function: {aggregate_func}. "
                f"Supported: {list(func_map.keys())}"
            )
        
        agg_func = func_map[aggregate_func]
        
        # Build columns
        select_cols = []
        
        # Add aggregate column
        if aggregate_column == "*":
            agg_expr = agg_func()
        else:
            if aggregate_column not in table.c:
                raise ValueError(f"Column '{aggregate_column}' not found in table")
            agg_expr = agg_func(table.c[aggregate_column])
        
        select_cols.append(agg_expr.label(f"{aggregate_func}_{aggregate_column}"))
        
        # Add GROUP BY columns
        if group_by:
            for col in group_by:
                if col in table.c:
                    select_cols.append(table.c[col])
        
        # Start building query
        stmt = select(*select_cols)
        
        # Add WHERE conditions
        if conditions:
            where_clauses = build_where_clause(table, conditions)
            if where_clauses:
                stmt = stmt.where(*where_clauses)
        
        # Add GROUP BY
        if group_by:
            group_cols = [table.c[col] for col in group_by if col in table.c]
            if group_cols:
                stmt = stmt.group_by(*group_cols)
        
        # Add HAVING
        if having:
            having_clauses = build_where_clause(table, having)
            if having_clauses:
                stmt = stmt.having(*having_clauses)
        
        return stmt
    
    @staticmethod
    def _validate_select_params(
        limit: Optional[int],
        offset: int,
        columns: Optional[List[str]]
    ) -> None:
        """Validate SELECT query parameters"""
        if limit is not None:
            if limit <= 0:
                raise ValueError("Limit must be positive")
            if limit > MAX_QUERY_LIMIT:
                raise ValueError(f"Limit cannot exceed {MAX_QUERY_LIMIT}")
        
        if offset < 0:
            raise ValueError("Offset cannot be negative")
        
        if columns and not isinstance(columns, list):
            raise ValueError("Columns must be a list")
    
    @staticmethod
    def _add_joins(stmt: Select, base_table: Table, join_configs: List[Dict[str, Any]]) -> Select:
        """Add JOIN clauses to query"""
        for join_config in join_configs:
            stmt = QueryBuilder._add_join(stmt, base_table, join_config)
        return stmt
    
    @staticmethod
    def _add_join(stmt: Select, base_table: Table, join_config: Dict[str, Any]) -> Select:
        """Add a single JOIN clause"""
        # Validate join config
        required_keys = ["table", "on"]
        for key in required_keys:
            if key not in join_config:
                raise ValueError(f"Join config missing required key: {key}")
        
        join_table = join_config["table"]
        on_clause = join_config["on"]
        join_type = join_config.get("type", "inner").lower()
        
        # Validate join type
        valid_join_types = ["inner", "left", "right", "full"]
        if join_type not in valid_join_types:
            raise ValueError(
                f"Invalid join type: {join_type}. "
                f"Valid options: {valid_join_types}"
            )
        
        # Apply join
        if join_type == "left":
            return stmt.join(join_table, on_clause, isouter=True)
        elif join_type == "right":
            return stmt.join(join_table, on_clause, isouter=True, full=False)
        elif join_type == "full":
            return stmt.join(join_table, on_clause, isouter=True, full=True)
        else:  # inner
            return stmt.join(join_table, on_clause)