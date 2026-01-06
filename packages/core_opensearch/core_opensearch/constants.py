"""
Constants for OpenSearch operations
"""

# Connection constants
DEFAULT_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_ON_TIMEOUT = True
CONNECTION_POOL_SIZE = 10

# Search constants
DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 100
DEFAULT_SEARCH_SIZE = 10
MAX_SEARCH_SIZE = 1000

# Bulk operations
DEFAULT_BULK_SIZE = 1000
MAX_BULK_SIZE = 5000
BULK_RETRY_ATTEMPTS = 3
BULK_RETRY_DELAY = 1.0

# Index constants
DEFAULT_SHARDS = 1
DEFAULT_REPLICAS = 1
MAX_RESULT_WINDOW = 10000
INDEX_REFRESH_INTERVAL = "1s"

# Query constants
DEFAULT_FUZZINESS = "AUTO"
DEFAULT_MIN_SHOULD_MATCH = "75%"
MAX_SUGGESTIONS = 10

# Performance monitoring
STATS_RETENTION_HOURS = 24
SLOW_QUERY_THRESHOLD = 1.0  # seconds

# AWS-specific
AWS_SERVICE_NAME = "es"
AWS_SERVICE_NAME_AOSS = "aoss"  # OpenSearch Serverless
DEFAULT_AWS_REGION = "us-east-1"

# Error patterns
INDEX_NOT_FOUND_ERROR = "index_not_found_exception"
DOCUMENT_NOT_FOUND_ERROR = "document_missing_exception"
VERSION_CONFLICT_ERROR = "version_conflict_engine_exception"