# packages/core_postgres_db/core_postgres_db/config.py
"""
Database configuration management - Sonarqube compliant
"""
import os
import logging
from typing import Optional
from dataclasses import dataclass, field
from urllib.parse import urlparse, parse_qs, urlencode

from .constants import (
    DEFAULT_POOL_SIZE, DEFAULT_MAX_OVERFLOW, DEFAULT_POOL_TIMEOUT,
    DEFAULT_POOL_RECYCLE, DEFAULT_STATEMENT_TIMEOUT
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Immutable database configuration
    Sonarqube compliant: dataclass with type hints
    """
    connection_string: str
    pool_size: int = DEFAULT_POOL_SIZE
    max_overflow: int = DEFAULT_MAX_OVERFLOW
    pool_timeout: int = DEFAULT_POOL_TIMEOUT
    pool_recycle: int = DEFAULT_POOL_RECYCLE
    statement_timeout: int = DEFAULT_STATEMENT_TIMEOUT
    schema: Optional[str] = None
    echo: bool = False
    use_ssl: bool = False
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if not self.connection_string:
            raise ValueError("Connection string cannot be empty")
        if self.pool_size <= 0:
            raise ValueError("Pool size must be positive")
        if self.max_overflow < 0:
            raise ValueError("Max overflow cannot be negative")


class ConfigLoader:
    """Load and validate database configuration"""
    
    @staticmethod
    def from_environment() -> DatabaseConfig:
        """Load configuration from environment variables"""
        connection_string = os.environ.get("DATABASE_URL")
        if not connection_string:
            raise ValueError("DATABASE_URL environment variable is required")
        
        schema = ConfigLoader._extract_schema_from_url(connection_string)
        
        # Apply SSL if required
        use_ssl = os.environ.get("DB_USE_SSL", "false").lower() == "true"
        if use_ssl:
            connection_string = ConfigLoader._add_ssl_to_url(connection_string)
        
        return DatabaseConfig(
            connection_string=connection_string,
            pool_size=int(os.environ.get("DB_POOL_SIZE", DEFAULT_POOL_SIZE)),
            max_overflow=int(os.environ.get("DB_MAX_OVERFLOW", DEFAULT_MAX_OVERFLOW)),
            pool_timeout=int(os.environ.get("DB_POOL_TIMEOUT", DEFAULT_POOL_TIMEOUT)),
            pool_recycle=int(os.environ.get("DB_POOL_RECYCLE", DEFAULT_POOL_RECYCLE)),
            statement_timeout=int(os.environ.get("DB_STATEMENT_TIMEOUT", DEFAULT_STATEMENT_TIMEOUT)),
            schema=schema or os.environ.get("DB_SCHEMA"),
            echo=os.environ.get("DB_ECHO", "false").lower() == "true",
            use_ssl=use_ssl,
        )
    
    @staticmethod
    def from_params(
        connection_string: str,
        schema: Optional[str] = None,
        pool_size: Optional[int] = None,
        max_overflow: Optional[int] = None,
        echo: bool = False,
        use_ssl: bool = False
    ) -> DatabaseConfig:
        """Load configuration from parameters"""
        final_schema = schema or ConfigLoader._extract_schema_from_url(connection_string)
        
        if use_ssl:
            connection_string = ConfigLoader._add_ssl_to_url(connection_string)
        
        return DatabaseConfig(
            connection_string=connection_string,
            pool_size=pool_size or DEFAULT_POOL_SIZE,
            max_overflow=max_overflow or DEFAULT_MAX_OVERFLOW,
            pool_timeout=DEFAULT_POOL_TIMEOUT,
            pool_recycle=DEFAULT_POOL_RECYCLE,
            statement_timeout=DEFAULT_STATEMENT_TIMEOUT,
            schema=final_schema,
            echo=echo,
            use_ssl=use_ssl,
        )
    
    @staticmethod
    def _extract_schema_from_url(url: str) -> Optional[str]:
        """Extract schema from PostgreSQL connection URL"""
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            
            # Check for schema in options parameter
            for option in query_params.get("options", []):
                if option.startswith("-csearch_path="):
                    return option.split("=", 1)[1]
            
            # Check for currentSchema parameter
            if "currentSchema" in query_params:
                return query_params["currentSchema"][0]
                
        except Exception as e:
            logger.warning(f"Failed to extract schema from URL: {e}")
        
        return None
    
    @staticmethod
    def _add_ssl_to_url(url: str) -> str:
        """Add SSL parameters to connection URL"""
        try:
            parsed = urlparse(url)
            query_params = parse_qs(parsed.query)
            
            # Add SSL parameters
            query_params.update({
                "sslmode": ["require"],
                "sslrootcert": [os.environ.get("DB_SSL_ROOT_CERT", "")],
                "sslcert": [os.environ.get("DB_SSL_CERT", "")],
                "sslkey": [os.environ.get("DB_SSL_KEY", "")],
            })
            
            # Rebuild URL with SSL parameters
            new_query = urlencode(query_params, doseq=True)
            return parsed._replace(query=new_query).geturl()
            
        except Exception as e:
            logger.warning(f"Failed to add SSL to URL, using original: {e}")
            return url


def get_database_config(
    connection_string: Optional[str] = None,
    **kwargs
) -> DatabaseConfig:
    """
    Main function to get database configuration
    Supports both environment variables and parameters
    """
    if connection_string:
        return ConfigLoader.from_params(connection_string=connection_string, **kwargs)
    return ConfigLoader.from_environment()