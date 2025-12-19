"""
Tests for custom exceptions
"""
import pytest

from core_postgresql_db.exceptions import (
    DatabaseError,
    ConnectionError,
    TimeoutError,
    DuplicateEntryError,
    ConstraintViolationError,
    RecordNotFoundError,
    TransactionError
)

class TestExceptions:
    """Test custom exceptions"""
    
    def test_database_error(self):
        """Test DatabaseError"""
        error = DatabaseError("Test error")
        assert str(error) == "Test error"
        assert isinstance(error, Exception)
    
    def test_connection_error(self):
        """Test ConnectionError"""
        error = ConnectionError("Connection failed")
        assert str(error) == "Connection failed"
        assert isinstance(error, DatabaseError)
    
    def test_timeout_error(self):
        """Test TimeoutError"""
        error = TimeoutError("Query timeout")
        assert str(error) == "Query timeout"
        assert isinstance(error, DatabaseError)
    
    def test_duplicate_entry_error(self):
        """Test DuplicateEntryError"""
        error = DuplicateEntryError("Duplicate entry")
        assert str(error) == "Duplicate entry"
        assert isinstance(error, DatabaseError)
    
    def test_constraint_violation_error(self):
        """Test ConstraintViolationError"""
        error = ConstraintViolationError("Constraint violation")
        assert str(error) == "Constraint violation"
        assert isinstance(error, DatabaseError)
    
    def test_record_not_found_error(self):
        """Test RecordNotFoundError"""
        error = RecordNotFoundError("Record not found")
        assert str(error) == "Record not found"
        assert isinstance(error, DatabaseError)
    
    def test_transaction_error(self):
        """Test TransactionError"""
        error = TransactionError("Transaction failed")
        assert str(error) == "Transaction failed"
        assert isinstance(error, DatabaseError)
    
    def test_exception_chaining(self):
        """Test exception chaining"""
        try:
            raise ValueError("Original error")
        except ValueError as e:
            try:
                raise DatabaseError("Database error") from e
            except DatabaseError as db_e:
                assert str(db_e) == "Database error"
                assert db_e.__cause__ is e
                assert isinstance(db_e.__cause__, ValueError)