"""
Custom exceptions for database operations.
"""

class DatabaseError(Exception):
    """Base exception for all database errors."""
    pass

class ConnectionError(DatabaseError):
    """Raised when database connection fails."""
    pass

class TimeoutError(DatabaseError):
    """Raised when query times out."""
    pass

class DuplicateEntryError(DatabaseError):
    """Raised on duplicate key violation."""
    pass

class ConstraintViolationError(DatabaseError):
    """Raised on constraint violation."""
    pass

class RecordNotFoundError(DatabaseError):
    """Raised when record is not found."""
    pass

class InvalidQueryError(DatabaseError):
    """Raised when query is invalid."""
    pass

class TransactionError(DatabaseError):
    """Raised when transaction fails."""
    pass

class DeadlockError(DatabaseError):
    """Raised when deadlock is detected."""
    pass

class PermissionDeniedError(DatabaseError):
    """Raised when database permission is denied."""
    pass