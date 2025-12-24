# packages/core_postgres_db/core_postgres_db/exceptions.py
"""
Custom exceptions for database operations - Sonarqube compliant
"""


class DatabaseError(Exception):
    """Base exception for all database errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error
        self.message = message
    
    def __str__(self):
        if self.original_error:
            return f"{self.message} (Original: {self.original_error})"
        return self.message


class ConnectionError(DatabaseError):
    """Exception raised for connection-related errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Connection error: {message}", original_error)


class QueryError(DatabaseError):
    """Exception raised for query execution errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Query error: {message}", original_error)


class IntegrityError(DatabaseError):
    """Base class for integrity constraint violations"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Integrity error: {message}", original_error)


class DuplicateEntryError(IntegrityError):
    """Exception raised for duplicate key violations"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Duplicate entry: {message}", original_error)


class ConstraintViolationError(IntegrityError):
    """Exception raised for constraint violations"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Constraint violation: {message}", original_error)


class ForeignKeyViolationError(ConstraintViolationError):
    """Exception raised for foreign key violations"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Foreign key violation: {message}", original_error)


class TransactionError(DatabaseError):
    """Exception raised for transaction-related errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Transaction error: {message}", original_error)


class TimeoutError(DatabaseError):
    """Exception raised for query timeout"""
    
    def __init__(self, message: str, timeout_seconds: int = None):
        if timeout_seconds:
            message = f"Query timeout after {timeout_seconds} seconds: {message}"
        super().__init__(f"Timeout: {message}")


class ConfigurationError(DatabaseError):
    """Exception raised for configuration errors"""
    
    def __init__(self, message: str):
        super().__init__(f"Configuration error: {message}")


class ValidationError(DatabaseError):
    """Exception raised for data validation errors"""
    
    def __init__(self, message: str, field: str = None):
        if field:
            message = f"Validation error for field '{field}': {message}"
        super().__init__(f"Validation error: {message}")


class ResourceNotFoundError(DatabaseError):
    """Exception raised when a requested resource is not found"""
    
    def __init__(self, resource_type: str, resource_id: str = None):
        if resource_id:
            message = f"{resource_type} with ID '{resource_id}' not found"
        else:
            message = f"{resource_type} not found"
        super().__init__(message)


class DatabaseOperationError(DatabaseError):
    """Exception raised for general database operation errors"""
    
    def __init__(self, operation: str, message: str, original_error: Exception = None):
        super().__init__(f"Operation '{operation}' failed: {message}", original_error)
        self.operation = operation


# Exception hierarchy for better error handling
EXCEPTION_HIERARCHY = {
    DatabaseError: [
        ConnectionError,
        QueryError,
        IntegrityError,
        TransactionError,
        TimeoutError,
        ConfigurationError,
        ValidationError,
        ResourceNotFoundError,
        DatabaseOperationError,
    ],
    IntegrityError: [
        DuplicateEntryError,
        ConstraintViolationError,
    ],
    ConstraintViolationError: [
        ForeignKeyViolationError,
    ]
}


def is_database_error(error: Exception) -> bool:
    """Check if exception is a database-related error"""
    return isinstance(error, DatabaseError)


def wrap_database_error(error: Exception, context: str = None) -> DatabaseError:
    """
    Wrap a generic exception in appropriate database error
    
    Args:
        error: Original exception
        context: Additional context about where error occurred
    
    Returns:
        Appropriate DatabaseError subclass
    """
    error_message = str(error)
    error_lower = error_message.lower()
    
    # Map error types to appropriate exceptions
    if "duplicate" in error_lower or "unique" in error_lower:
        exc_class = DuplicateEntryError
    elif "foreign key" in error_lower:
        exc_class = ForeignKeyViolationError
    elif "constraint" in error_lower or "violates" in error_lower:
        exc_class = ConstraintViolationError
    elif "timeout" in error_lower:
        exc_class = TimeoutError
    elif "connection" in error_lower or "connect" in error_lower:
        exc_class = ConnectionError
    elif "transaction" in error_lower:
        exc_class = TransactionError
    else:
        exc_class = DatabaseError
    
    message = f"{context}: {error_message}" if context else error_message
    return exc_class(message, error)