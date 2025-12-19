"""
Tests for utility functions
"""
import pytest
from sqlalchemy import Table, Column, Integer, String, MetaData

from core_postgresql_db.utils import (
    validate_table_name,
    safe_table_ref,
    rows_to_dicts,
    build_where_clause,
    paginate_query,
    extract_returning_columns
)

class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_validate_table_name_valid(self):
        """Test valid table names"""
        assert validate_table_name("users") is True
        assert validate_table_name("user_profiles") is True
        assert validate_table_name("user_123") is True
        assert validate_table_name("_users") is True
    
    def test_validate_table_name_invalid(self):
        """Test invalid table names"""
        assert validate_table_name("123users") is False
        assert validate_table_name("users-test") is False
        assert validate_table_name("users.test") is False
        assert validate_table_name("") is False
        assert validate_table_name("users; DROP TABLE users;") is False
    
    def test_safe_table_ref_valid(self):
        """Test safe table reference with valid names"""
        assert safe_table_ref("users") == "users"
        assert safe_table_ref("user_profiles") == "user_profiles"
    
    def test_safe_table_ref_invalid(self):
        """Test safe table reference with invalid names"""
        with pytest.raises(ValueError):
            safe_table_ref("123users")
        
        with pytest.raises(ValueError):
            safe_table_ref("users; DROP TABLE users;")
    
    def test_rows_to_dicts(self):
        """Test converting rows to dictionaries"""
        # Mock SQLAlchemy result
        class MockRow:
            def __init__(self, data):
                self._mapping = data
        
        mock_result = [
            MockRow({"id": 1, "name": "John"}),
            MockRow({"id": 2, "name": "Jane"})
        ]
        
        result = rows_to_dicts(mock_result)
        
        assert len(result) == 2
        assert result[0] == {"id": 1, "name": "John"}
        assert result[1] == {"id": 2, "name": "Jane"}
    
    def test_build_where_clause_simple(self):
        """Test building simple WHERE clause"""
        # Create mock table
        metadata = MetaData()
        table = Table(
            "users", metadata,
            Column("id", Integer),
            Column("name", String),
            Column("age", Integer),
            Column("active", Integer)
        )
        
        conditions = {"name": "John", "age": 30}
        clauses = build_where_clause(table, conditions)
        
        assert len(clauses) == 2
        assert str(clauses[0]) == "users.name = :name_1"
        assert str(clauses[1]) == "users.age = :age_1"
    
    def test_build_where_clause_operators(self):
        """Test building WHERE clause with operators"""
        metadata = MetaData()
        table = Table(
            "users", metadata,
            Column("id", Integer),
            Column("age", Integer)
        )
        
        conditions = {
            "age__gt": 20,
            "age__lt": 40,
            "id__in": [1, 2, 3],
            "name__like": "John%"
        }
        
        clauses = build_where_clause(table, conditions)
        
        # age__gt and age__lt should create clauses
        # name__like should be ignored (column doesn't exist)
        # id__in should create clause
        assert len(clauses) == 3
    
    def test_build_where_clause_or_condition(self):
        """Test building WHERE clause with OR condition"""
        metadata = MetaData()
        table = Table(
            "users", metadata,
            Column("id", Integer),
            Column("name", String),
            Column("active", Integer)
        )
        
        conditions = {
            "__or": [
                {"name": "John"},
                {"active": 1}
            ]
        }
        
        clauses = build_where_clause(table, conditions)
        
        assert len(clauses) == 1
        assert "OR" in str(clauses[0])
    
    def test_build_where_clause_and_condition(self):
        """Test building WHERE clause with AND condition"""
        metadata = MetaData()
        table = Table(
            "users", metadata,
            Column("id", Integer),
            Column("name", String)
        )
        
        conditions = {
            "__and": [
                {"id__gt": 10},
                {"name__like": "J%"}
            ]
        }
        
        clauses = build_where_clause(table, conditions)
        
        assert len(clauses) == 1
        assert "AND" in str(clauses[0])
    
    def test_paginate_query(self):
        """Test paginating query"""
        metadata = MetaData()
        table = Table("users", metadata, Column("id", Integer))
        
        from sqlalchemy import select
        stmt = select(table)
        
        # Test with limit
        paginated = paginate_query(stmt, limit=10, offset=0)
        assert "LIMIT 10" in str(paginated)
        
        # Test with offset
        paginated = paginate_query(stmt, limit=None, offset=5)
        assert "OFFSET 5" in str(paginated)
        
        # Test with both
        paginated = paginate_query(stmt, limit=10, offset=5)
        assert "LIMIT 10" in str(paginated)
        assert "OFFSET 5" in str(paginated)
    
    def test_extract_returning_columns(self):
        """Test extracting returning columns"""
        metadata = MetaData()
        table = Table(
            "users", metadata,
            Column("id", Integer),
            Column("name", String),
            Column("email", String)
        )
        
        # Test with specific columns
        columns = ["id", "name"]
        result = extract_returning_columns(table, columns)
        
        assert len(result) == 2
        assert result[0].name == "id"
        assert result[1].name == "name"
        
        # Test with all columns
        result = extract_returning_columns(table, None)
        assert len(result) == 3  # All columns
        
        # Test with invalid column (should be filtered)
        columns = ["id", "invalid_column"]
        result = extract_returning_columns(table, columns)
        assert len(result) == 1  # Only 'id' is valid
        assert result[0].name == "id"