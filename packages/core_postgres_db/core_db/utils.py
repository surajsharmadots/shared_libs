"""
Utility functions for database operations
"""
import re
import logging
from typing import Any, Dict, List, Optional, Tuple
from sqlalchemy import Table, and_, or_, not_, Column
from sqlalchemy.sql import Select

logger = logging.getLogger(__name__)

# Table name validation
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def validate_table_name(name: str) -> bool:
    """Validate table name"""
    return bool(_TABLE_NAME_RE.match(name))

def safe_table_ref(name: str) -> str:
    """Protect against SQL injection in table names"""
    if not validate_table_name(name):
        raise ValueError(f"Invalid table name: {name}")
    return name

def rows_to_dicts(result) -> List[Dict[str, Any]]:
    """Convert SQLAlchemy result to list of dictionaries"""
    return [dict(row._mapping) for row in result]

def build_where_clause(table: Table, conditions: Dict[str, Any]) -> List:
    """
    Build WHERE clause from conditions dictionary
    
    Supports:
    - Simple equality: {"column": value}
    - Operators: {"column__eq": value, "column__gt": value}
    - IN clause: {"column__in": [value1, value2]}
    - LIKE: {"column__like": "pattern%"}
    - OR conditions: {"__or": [{"col1": val1}, {"col2": val2}]}
    """
    if not conditions:
        return []
    
    clauses = []
    
    for key, value in conditions.items():
        # Handle OR conditions
        if key == "__or" and isinstance(value, list):
            or_clauses = []
            for cond in value:
                or_clauses.extend(build_where_clause(table, cond))
            if or_clauses:
                clauses.append(or_(*or_clauses))
            continue
        
        # Handle AND conditions
        if key == "__and" and isinstance(value, list):
            and_clauses = []
            for cond in value:
                and_clauses.extend(build_where_clause(table, cond))
            if and_clauses:
                clauses.append(and_(*and_clauses))
            continue
        
        # Handle column operators
        if "__" in key:
            col_name, operator = key.rsplit("__", 1)
        else:
            col_name, operator = key, "eq"
        
        if col_name not in table.c:
            logger.warning(f"Column {col_name} not found in table {table.name}")
            continue
        
        column = table.c[col_name]
        
        # Apply operator
        if operator == "eq":
            clauses.append(column == value)
        elif operator == "ne":
            clauses.append(column != value)
        elif operator == "gt":
            clauses.append(column > value)
        elif operator == "ge":
            clauses.append(column >= value)
        elif operator == "lt":
            clauses.append(column < value)
        elif operator == "le":
            clauses.append(column <= value)
        elif operator == "in":
            if isinstance(value, list):
                clauses.append(column.in_(value))
            else:
                clauses.append(column == value)
        elif operator == "not_in":
            if isinstance(value, list):
                clauses.append(column.notin_(value))
            else:
                clauses.append(column != value)
        elif operator == "like":
            clauses.append(column.like(value))
        elif operator == "ilike":
            clauses.append(column.ilike(value))
        elif operator == "is_null":
            clauses.append(column.is_(None))
        elif operator == "is_not_null":
            clauses.append(column.is_not(None))
        elif operator == "between":
            if isinstance(value, tuple) and len(value) == 2:
                clauses.append(column.between(value[0], value[1]))
        else:
            logger.warning(f"Unknown operator: {operator}")
    
    return clauses

def paginate_query(stmt: Select, limit: Optional[int] = None, offset: int = 0) -> Select:
    """Apply pagination to query"""
    if limit is not None:
        stmt = stmt.limit(limit)
    if offset > 0:
        stmt = stmt.offset(offset)
    return stmt

def extract_returning_columns(table: Table, columns: Optional[List[str]] = None):
    """Extract columns for RETURNING clause"""
    if columns:
        return [table.c[col] for col in columns if col in table.c]
    return [table]