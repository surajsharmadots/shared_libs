"""
Database connection management with environment-based configuration
"""
import os
import logging
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode

from sqlalchemy import create_engine, MetaData, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
from sqlalchemy.engine import Engine

from .types import DatabaseConfig
from .exceptions import ConnectionError

logger = logging.getLogger(__name__)

def get_database_config(
    connection_string: Optional[str] = None,
    pool_size: Optional[int] = None,
    max_overflow: Optional[int] = None,
    pool_timeout: Optional[int] = None,
    pool_recycle: Optional[int] = None,
    statement_timeout: Optional[int] = None,
    schema: Optional[str] = None,
    echo: Optional[bool] = None,
    use_ssl: Optional[bool] = None,
) -> DatabaseConfig:
    """
    Get database configuration with environment variable fallback
    
    Priority: Function args > Environment variables > Default values
    """
    # Get connection string from args or environment
    db_url = connection_string or os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError(
            "Database URL must be provided either as argument "
            "or via DATABASE_URL environment variable"
        )
    
    # Extract schema from URL if not provided
    if not schema:
        schema = extract_schema_from_url(db_url)
    
    # Parse SSL mode from URL or environment
    use_ssl_env = use_ssl if use_ssl is not None else os.environ.get("DB_USE_SSL", "false").lower() == "true"
    
    # Build final URL with SSL if needed
    final_url = add_ssl_to_url(db_url) if use_ssl_env else db_url
    
    return DatabaseConfig(
        connection_string=final_url,
        pool_size=pool_size or int(os.environ.get("DB_POOL_SIZE", "20")),
        max_overflow=max_overflow or int(os.environ.get("DB_MAX_OVERFLOW", "30")),
        pool_timeout=pool_timeout or int(os.environ.get("DB_POOL_TIMEOUT", "30")),
        pool_recycle=pool_recycle or int(os.environ.get("DB_POOL_RECYCLE", "3600")),
        statement_timeout=statement_timeout or int(os.environ.get("DB_STATEMENT_TIMEOUT", "30000")),
        schema=schema,
        echo=echo or os.environ.get("DB_ECHO", "false").lower() == "true",
        use_ssl=use_ssl_env,
    )

def extract_schema_from_url(url: str) -> Optional[str]:
    """Extract schema from PostgreSQL connection URL"""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if "options" in params:
            for opt in params["options"]:
                if opt.startswith("-csearch_path="):
                    return opt.split("=", 1)[1]
    except Exception:
        pass
    return None

def add_ssl_to_url(url: str) -> str:
    """Add SSL parameters to PostgreSQL URL"""
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
        
        # Rebuild URL
        new_query = urlencode(query_params, doseq=True)
        return parsed._replace(query=new_query).geturl()
    except Exception as e:
        logger.warning(f"Failed to add SSL to URL: {e}")
        return url

class ConnectionManager:
    """Manages database connections with connection pooling"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.sync_engine: Optional[Engine] = None
        self.async_engine: Optional[AsyncEngine] = None
        self.metadata: Optional[MetaData] = None
        
        self._initialize_engines()
        self._warmup_connection()
    
    def _initialize_engines(self):
        """Initialize sync and async engines"""
        try:
            # Sync engine
            self.sync_engine = create_engine(
                self.config.connection_string,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle,
                echo=self.config.echo,
                pool_pre_ping=True,
                future=True,
                execution_options={
                    "timeout": self.config.statement_timeout / 1000,  # Convert to seconds
                    "schema_translate_map": {None: self.config.schema} if self.config.schema else None,
                }
            )
            
            # Async engine (convert sync URL to async)
            async_url = self.config.connection_string.replace(
                "postgresql://", "postgresql+asyncpg://"
            ).replace("postgresql+psycopg2://", "postgresql+asyncpg://")
            
            self.async_engine = create_async_engine(
                async_url,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle,
                echo=self.config.echo,
                pool_pre_ping=True,
                future=True,
                connect_args={
                    "command_timeout": self.config.statement_timeout / 1000,
                    "server_settings": {
                        "search_path": self.config.schema
                    } if self.config.schema else {}
                }
            )
            
            self.metadata = MetaData(schema=self.config.schema)
            
        except Exception as e:
            logger.error(f"Failed to initialize database engines: {e}")
            raise ConnectionError(f"Database connection failed: {e}")
    
    def _warmup_connection(self):
        """Warm up connection pool"""
        try:
            if self.sync_engine:
                with self.sync_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                    logger.info("Database connection warmed up successfully")
        except Exception as e:
            logger.warning(f"Connection warmup failed: {e}")
    
    def get_sync_engine(self) -> Engine:
        """Get sync engine"""
        if not self.sync_engine:
            raise ConnectionError("Sync engine not initialized")
        return self.sync_engine
    
    def get_async_engine(self) -> AsyncEngine:
        """Get async engine"""
        if not self.async_engine:
            raise ConnectionError("Async engine not initialized")
        return self.async_engine
    
    def dispose(self):
        """Dispose all connections"""
        try:
            if self.sync_engine:
                self.sync_engine.dispose()
            if self.async_engine:
                import asyncio
                asyncio.run(self.async_engine.dispose())
            logger.info("Database connections disposed")
        except Exception as e:
            logger.error(f"Error disposing connections: {e}")
    
    def health_check(self) -> bool:
        """Check database health"""
        try:
            with self.sync_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False