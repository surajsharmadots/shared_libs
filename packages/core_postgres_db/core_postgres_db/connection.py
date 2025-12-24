# packages/core_postgres_db/core_postgres_db/connection.py
"""
Connection manager with singleton pattern - Sonarqube compliant
"""
import logging
from typing import Dict

from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from .config import DatabaseConfig
from .exceptions import ConnectionError

logger = logging.getLogger(__name__)


class ConnectionManager:
    """
    Manages database connections with singleton pattern
    Ensures only one engine instance per configuration
    """
    
    # Singleton instances
    _sync_engines: Dict[str, Engine] = {}
    _async_engines: Dict[str, AsyncEngine] = {}
    _metadata_cache: Dict[str, MetaData] = {}
    
    def __init__(self, config: DatabaseConfig):
        """
        Initialize connection manager
        
        Args:
            config: Database configuration
        """
        self.config = config
        self._config_key = self._create_config_key(config)
    
    @staticmethod
    def _create_config_key(config: DatabaseConfig) -> str:
        """Create unique key for configuration"""
        return f"{config.connection_string}:{config.schema}:{config.pool_size}"
    
    def get_sync_engine(self) -> Engine:
        """Get or create sync engine (singleton)"""
        if self._config_key not in self._sync_engines:
            try:
                logger.info(f"Creating new sync engine for: {self.config.schema}")
                
                self._sync_engines[self._config_key] = create_engine(
                    self.config.connection_string,
                    pool_size=self.config.pool_size,
                    max_overflow=self.config.max_overflow,
                    pool_timeout=self.config.pool_timeout,
                    pool_recycle=self.config.pool_recycle,
                    echo=self.config.echo,
                    pool_pre_ping=True,
                    future=True,
                )
                
                logger.debug(f"Sync engine created successfully")
                
            except Exception as e:
                raise ConnectionError(
                    f"Failed to create sync engine: {e}",
                    original_error=e
                )
        
        return self._sync_engines[self._config_key]
    
    def get_async_engine(self) -> AsyncEngine:
        """Get or create async engine (singleton)"""
        if self._config_key not in self._async_engines:
            try:
                logger.info(f"Creating new async engine for: {self.config.schema}")
                
                # Convert to async URL
                async_url = self.config.connection_string.replace(
                    "postgresql://", "postgresql+asyncpg://"
                )
                
                self._async_engines[self._config_key] = create_async_engine(
                    async_url,
                    pool_size=self.config.pool_size,
                    max_overflow=self.config.max_overflow,
                    pool_timeout=self.config.pool_timeout,
                    pool_recycle=self.config.pool_recycle,
                    echo=self.config.echo,
                    pool_pre_ping=True,
                )
                
                logger.debug(f"Async engine created successfully")
                
            except Exception as e:
                raise ConnectionError(
                    f"Failed to create async engine: {e}",
                    original_error=e
                )
        
        return self._async_engines[self._config_key]
    
    def get_metadata(self) -> MetaData:
        """Get or create metadata (singleton)"""
        if self._config_key not in self._metadata_cache:
            self._metadata_cache[self._config_key] = MetaData(schema=self.config.schema)
        
        return self._metadata_cache[self._config_key]
    
    def health_check(self) -> bool:
        """Check database connection health"""
        try:
            engine = self.get_sync_engine()
            with engine.connect() as conn:
                result = conn.execute("SELECT 1")
                return result.scalar() == 1
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
    
    def dispose_all(self):
        """Dispose all engine instances"""
        logger.info("Disposing all database engines")
        
        for engine in self._sync_engines.values():
            try:
                engine.dispose()
            except Exception as e:
                logger.error(f"Error disposing sync engine: {e}")
        
        for engine in self._async_engines.values():
            try:
                import asyncio
                asyncio.run(engine.dispose())
            except Exception as e:
                logger.error(f"Error disposing async engine: {e}")
        
        self._sync_engines.clear()
        self._async_engines.clear()
        self._metadata_cache.clear()
        
        logger.info("All database engines disposed")
    
    @classmethod
    def get_instance_count(cls) -> Dict[str, int]:
        """Get count of engine instances"""
        return {
            "sync_engines": len(cls._sync_engines),
            "async_engines": len(cls._async_engines),
            "metadata_instances": len(cls._metadata_cache),
        }