# packages/core_postgres_db/core_postgres_db/utils.py
"""
Utility functions for database operations - Sonarqube compliant
"""
import re
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from sqlalchemy import Table, and_, or_, not_, between
from sqlalchemy.sql import Select

from .constants import TABLE_NAME_PATTERN
from .types import Operator

logger = logging.getLogger(__name__)


class ValidationError(ValueError):
    """Custom validation error"""
    pass


def validate_table_name(name: str, pattern: str = TABLE_NAME_PATTERN) -> None:
    """
    Validate table name against SQL injection
    Raises ValidationError if invalid
    """
    if not name:
        raise ValidationError("Table name cannot be empty")
    
    if not re.match(pattern, name):
        raise ValidationError(
            f"Invalid table name '{name}'. "
            f"Must match pattern: {pattern}"
        )


def safe_table_ref(name: str, pattern: str = TABLE_NAME_PATTERN) -> str:
    """
    Get safe table reference with validation
    Returns validated table name
    """
    validate_table_name(name, pattern)
    return name


def rows_to_dicts(result) -> List[Dict[str, Any]]:
    """
    Convert SQLAlchemy result to list of dictionaries
    Handles empty results gracefully
    """
    if result is None:
        return []
    
    try:
        return [dict(row._mapping) for row in result]
    except AttributeError:
        # Handle case where result doesn't have _mapping
        logger.warning("Result doesn't have _mapping attribute")
        return []


class WhereClauseBuilder:
    """Builder for WHERE clauses with operator support"""
    
    # Operator to SQLAlchemy function mapping
    OPERATOR_MAP = {
        Operator.EQUAL.value: lambda col, val: col == val,
        Operator.NOT_EQUAL.value: lambda col, val: col != val,
        Operator.GREATER_THAN.value: lambda col, val: col > val,
        Operator.GREATER_EQUAL.value: lambda col, val: col >= val,
        Operator.LESS_THAN.value: lambda col, val: col < val,
        Operator.LESS_EQUAL.value: lambda col, val: col <= val,
        Operator.IN.value: lambda col, val: col.in_(val) if isinstance(val, list) else col == val,
        Operator.NOT_IN.value: lambda col, val: col.notin_(val) if isinstance(val, list) else col != val,
        Operator.LIKE.value: lambda col, val: col.like(val),
        Operator.ILIKE.value: lambda col, val: col.ilike(val),
        Operator.IS_NULL.value: lambda col, _: col.is_(None),
        Operator.IS_NOT_NULL.value: lambda col, _: col.is_not(None),
        Operator.BETWEEN.value: lambda col, val: between(col, val[0], val[1]) if isinstance(val, tuple) and len(val) == 2 else col == val,
    }
    
    @classmethod
    def build(cls, table: Table, conditions: Dict[str, Any]) -> List:
        """
        Build WHERE clause from conditions dictionary
        
        Supports:
        - Simple: {"name": "John"}
        - Operators: {"age__gt": 18}
        - OR: {"__or": [{"status": "active"}, {"status": "pending"}]}
        - AND: {"__and": [{"age__gt": 18}, {"status": "active"}]}
        - NOT: {"__not": {"status": "inactive"}}
        """
        if not conditions:
            return []
        
        clauses = []
        
        for key, value in conditions.items():
            # Handle logical operators
            if key == "__or" and isinstance(value, list):
                or_clauses = []
                for condition in value:
                    or_clauses.extend(cls.build(table, condition))
                if or_clauses:
                    clauses.append(or_(*or_clauses))
                continue
            
            if key == "__and" and isinstance(value, list):
                and_clauses = []
                for condition in value:
                    and_clauses.extend(cls.build(table, condition))
                if and_clauses:
                    clauses.append(and_(*and_clauses))
                continue
            
            if key == "__not" and isinstance(value, dict):
                not_clauses = cls.build(table, value)
                if not_clauses:
                    clauses.append(not_(*not_clauses))
                continue
            
            # Handle column operators
            clauses.append(cls._build_column_clause(table, key, value))
        
        return clauses
    
    @classmethod
    def _build_column_clause(cls, table: Table, key: str, value: Any):
        """Build clause for a single column"""
        # Extract column name and operator
        if "__" in key:
            col_name, operator_str = key.rsplit("__", 1)
            operator = Operator(operator_str)  # Will raise ValueError if invalid
        else:
            col_name, operator = key, Operator.EQUAL
        
        # Validate column exists
        if col_name not in table.c:
            raise ValidationError(f"Column '{col_name}' not found in table '{table.name}'")
        
        column = table.c[col_name]
        
        # Get operator function
        operator_func = cls.OPERATOR_MAP.get(operator.value)
        if not operator_func:
            raise ValidationError(f"Unsupported operator: {operator.value}")
        
        # Validate value for specific operators
        if operator == Operator.IN and not isinstance(value, list):
            logger.warning(f"IN operator expects list, got {type(value)} for column {col_name}")
        
        if operator == Operator.BETWEEN:
            if not (isinstance(value, tuple) and len(value) == 2):
                raise ValidationError(
                    f"BETWEEN operator expects tuple of length 2, "
                    f"got {type(value)} for column {col_name}"
                )
        
        # Apply operator
        return operator_func(column, value)


def build_where_clause(table: Table, conditions: Dict[str, Any]) -> List:
    """Public interface for building WHERE clauses"""
    return WhereClauseBuilder.build(table, conditions)


def paginate_query(
    stmt: Select, 
    page: int = 1, 
    per_page: int = 20
) -> Select:
    """
    Apply pagination to SQL query
    
    Args:
        stmt: SQLAlchemy select statement
        page: Page number (1-indexed)
        per_page: Items per page
    
    Returns:
        Paginated select statement
    """
    if page < 1:
        raise ValidationError("Page must be at least 1")
    if per_page <= 0:
        raise ValidationError("Per page must be positive")
    
    offset = (page - 1) * per_page
    return stmt.limit(per_page).offset(offset)


def format_datetime(value: Any) -> Optional[str]:
    """Format datetime values for database"""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def sanitize_sql_identifier(identifier: str) -> str:
    """Sanitize SQL identifier (table/column names)"""
    # Remove potentially dangerous characters
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', identifier)
    
    # Ensure it starts with letter or underscore
    if not re.match(r'^[a-zA-Z_]', sanitized):
        sanitized = '_' + sanitized
    
    return sanitized


def chunk_list(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size
    
    Args:
        data: List to chunk
        chunk_size: Size of each chunk
    
    Returns:
        List of chunks
    """
    if chunk_size <= 0:
        raise ValidationError("Chunk size must be positive")
    
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]