"""
Custom exceptions for OpenSearch operations
"""


class OpenSearchError(Exception):
    """Base exception for all OpenSearch errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(message)
        self.original_error = original_error
        self.message = message
    
    def __str__(self):
        if self.original_error:
            return f"{self.message} (Original: {self.original_error})"
        return self.message


class ConnectionError(OpenSearchError):
    """Exception raised for connection-related errors"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Connection error: {message}", original_error)


class TimeoutError(OpenSearchError):
    """Exception raised for operation timeout"""
    
    def __init__(self, message: str, timeout_seconds: int = None):
        if timeout_seconds:
            message = f"Operation timeout after {timeout_seconds} seconds: {message}"
        super().__init__(f"Timeout error: {message}")


class AuthenticationError(OpenSearchError):
    """Exception raised for authentication failures"""
    
    def __init__(self, message: str, original_error: Exception = None):
        super().__init__(f"Authentication error: {message}", original_error)


class IndexNotFoundError(OpenSearchError):
    """Exception raised when index is not found"""
    
    def __init__(self, index_name: str, original_error: Exception = None):
        super().__init__(f"Index '{index_name}' not found", original_error)
        self.index_name = index_name


class DocumentNotFoundError(OpenSearchError):
    """Exception raised when document is not found"""
    
    def __init__(self, index_name: str, document_id: str, original_error: Exception = None):
        super().__init__(f"Document '{document_id}' not found in index '{index_name}'", original_error)
        self.index_name = index_name
        self.document_id = document_id


class BulkOperationError(OpenSearchError):
    """Exception raised for bulk operation failures"""
    
    def __init__(self, message: str, errors: list = None, original_error: Exception = None):
        if errors:
            error_count = len(errors)
            message = f"Bulk operation failed with {error_count} errors: {message}"
        super().__init__(f"Bulk operation error: {message}", original_error)
        self.errors = errors or []


class SearchQueryError(OpenSearchError):
    """Exception raised for invalid search queries"""
    
    def __init__(self, message: str, query: dict = None, original_error: Exception = None):
        super().__init__(f"Search query error: {message}", original_error)
        self.query = query


class MappingError(OpenSearchError):
    """Exception raised for mapping/validation errors"""
    
    def __init__(self, message: str, field: str = None, original_error: Exception = None):
        if field:
            message = f"Mapping error for field '{field}': {message}"
        super().__init__(f"Mapping error: {message}", original_error)
        self.field = field


class VersionConflictError(OpenSearchError):
    """Exception raised for version conflicts"""
    
    def __init__(self, index_name: str, document_id: str, original_error: Exception = None):
        message = f"Version conflict for document '{document_id}' in index '{index_name}'"
        super().__init__(message, original_error)
        self.index_name = index_name
        self.document_id = document_id


class ResourceExistsError(OpenSearchError):
    """Exception raised when resource already exists"""
    
    def __init__(self, resource_type: str, resource_name: str, original_error: Exception = None):
        message = f"{resource_type} '{resource_name}' already exists"
        super().__init__(message, original_error)
        self.resource_type = resource_type
        self.resource_name = resource_name


class ConfigurationError(OpenSearchError):
    """Exception raised for configuration errors"""
    
    def __init__(self, message: str):
        super().__init__(f"Configuration error: {message}")


# Exception hierarchy
EXCEPTION_HIERARCHY = {
    OpenSearchError: [
        ConnectionError,
        TimeoutError,
        AuthenticationError,
        IndexNotFoundError,
        DocumentNotFoundError,
        BulkOperationError,
        SearchQueryError,
        MappingError,
        VersionConflictError,
        ResourceExistsError,
        ConfigurationError,
    ]
}


def wrap_opensearch_error(error: Exception, context: str = None) -> OpenSearchError:
    """
    Wrap a generic exception in appropriate OpenSearch error
    
    Args:
        error: Original exception
        context: Additional context about where error occurred
    
    Returns:
        Appropriate OpenSearchError subclass
    """
    error_message = str(error).lower()
    error_type = type(error).__name__
    
    # Map error types to appropriate exceptions
    if "index_not_found" in error_message:
        exc_class = IndexNotFoundError
    elif "document_missing" in error_message or "not_found" in error_message:
        exc_class = DocumentNotFoundError
    elif "version_conflict" in error_message:
        exc_class = VersionConflictError
    elif "authentication" in error_message or "unauthorized" in error_message:
        exc_class = AuthenticationError
    elif "timeout" in error_message:
        exc_class = TimeoutError
    elif "connection" in error_message:
        exc_class = ConnectionError
    elif "bulk" in error_message:
        exc_class = BulkOperationError
    elif "query" in error_message or "search" in error_message:
        exc_class = SearchQueryError
    elif "resource_already_exists" in error_message:
        exc_class = ResourceExistsError
    elif "mapping" in error_message:
        exc_class = MappingError
    else:
        exc_class = OpenSearchError
    
    message = f"{context}: {error_message}" if context else error_message
    return exc_class(message, error)