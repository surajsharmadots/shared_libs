"""
Advanced query building utilities
"""
from typing import Any, Dict, List, Optional, Tuple, Union
from sqlalchemy import Table, select, and_, or_, not_, desc, asc, func
from sqlalchemy.sql import Select

from .utils import build_where_clause

class QueryBuilder:
    """Builds complex SQL queries"""
    
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
        # Select columns
        if columns:
            cols = [table.c[col] for col in columns if col in table.c]
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
            for join_config in join_tables:
                stmt = QueryBuilder._add_join(stmt, table, join_config)
        
        # Add ORDER BY
        if order_by:
            for col_name, ascending in order_by:
                if col_name in table.c:
                    col = table.c[col_name]
                    stmt = stmt.order_by(asc(col) if ascending else desc(col))
        
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
    def _add_join(stmt: Select, base_table: Table, join_config: Dict[str, Any]) -> Select:
        """Add JOIN clause to query"""
        join_type = join_config.get("type", "inner").lower()
        join_table = join_config["table"]
        on_clause = join_config.get("on")
        
        if join_type == "left":
            stmt = stmt.join(join_table, on_clause, isouter=True)
        elif join_type == "right":
            stmt = stmt.join(join_table, on_clause, isouter=True, full=False)
        elif join_type == "full":
            stmt = stmt.join(join_table, on_clause, isouter=True, full=True)
        else:  # inner
            stmt = stmt.join(join_table, on_clause)
        
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
        # Get aggregate function
        func_map = {
            "sum": func.sum,
            "count": func.count,
            "avg": func.avg,
            "min": func.min,
            "max": func.max
        }
        
        if aggregate_func not in func_map:
            raise ValueError(f"Unsupported aggregate function: {aggregate_func}")
        
        agg_func = func_map[aggregate_func]
        
        # Build columns
        select_cols = []
        
        # Add aggregate column
        if aggregate_column == "*":
            agg_expr = agg_func()
        else:
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