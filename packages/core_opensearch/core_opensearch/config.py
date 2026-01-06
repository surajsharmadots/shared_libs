"""
OpenSearch configuration management with multi-cloud support
"""
import os
import logging
from typing import List, Optional, Tuple, Dict, Any
from dataclasses import dataclass, field
from urllib.parse import urlparse

from .constants import (
    DEFAULT_TIMEOUT, MAX_RETRIES, RETRY_ON_TIMEOUT,
    CONNECTION_POOL_SIZE, DEFAULT_AWS_REGION
)

logger = logging.getLogger(__name__)


@dataclass
class OpenSearchConfig:
    """
    Universal OpenSearch configuration supporting:
    - Self-hosted OpenSearch
    - AWS OpenSearch Service
    - OpenSearch Serverless
    - Docker/ECS deployments
    """
    hosts: List[str]
    http_auth: Optional[Tuple[str, str]] = None
    use_ssl: bool = True
    verify_certs: bool = True
    ssl_show_warn: bool = True
    timeout: int = DEFAULT_TIMEOUT
    max_retries: int = MAX_RETRIES
    retry_on_timeout: bool = RETRY_ON_TIMEOUT
    connection_pool_size: int = CONNECTION_POOL_SIZE
    
    # AWS-specific
    aws_region: Optional[str] = None
    aws_service: str = "es"
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    
    # Advanced options
    sniff_on_start: bool = False
    sniff_on_connection_fail: bool = False
    sniffer_timeout: int = 60
    sniff_timeout: int = 10
    
    # Custom headers
    headers: Dict[str, str] = field(default_factory=dict)
    
    # Extra kwargs for OpenSearch client
    extra_kwargs: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate configuration"""
        if not self.hosts:
            raise ValueError("At least one host is required")
        
        # Ensure hosts are properly formatted
        self.hosts = [self._normalize_host(host) for host in self.hosts]
        
        # Auto-detect AWS if endpoint contains .amazonaws.com
        if not self.aws_region and any('.amazonaws.com' in host for host in self.hosts):
            self.aws_region = self._extract_aws_region(self.hosts[0])
            logger.info(f"Auto-detected AWS region: {self.aws_region}")
        
        # Set AWS service type
        if self.aws_region:
            if any('aoss.' in host for host in self.hosts):
                self.aws_service = "aoss"  # OpenSearch Serverless
            else:
                self.aws_service = "es"  # OpenSearch Service
    
    @staticmethod
    def _normalize_host(host: str) -> str:
        """Normalize host URL"""
        if not host.startswith(('http://', 'https://')):
            host = f"https://{host}"  # Default to HTTPS
        
        # Remove trailing slash
        return host.rstrip('/')
    
    @staticmethod
    def _extract_aws_region(host: str) -> str:
        """Extract AWS region from endpoint"""
        try:
            # Format: search-domain.region.es.amazonaws.com
            parts = host.split('.')
            if len(parts) >= 3:
                return parts[-4]  # Region is typically 4th from end
        except Exception:
            pass
        return DEFAULT_AWS_REGION


class ConfigLoader:
    """Load OpenSearch configuration from various sources"""
    
    @staticmethod
    def from_environment() -> OpenSearchConfig:
        """
        Load configuration from environment variables
        Supports multiple deployment scenarios
        """
        # Get hosts from environment
        hosts_env = os.environ.get("OPENSEARCH_HOSTS")
        aws_endpoint = os.environ.get("AWS_OPENSEARCH_ENDPOINT")
        
        hosts = []
        
        if hosts_env:
            # Multiple hosts comma-separated
            hosts = [h.strip() for h in hosts_env.split(",")]
        elif aws_endpoint:
            # AWS OpenSearch endpoint
            hosts = [aws_endpoint]
        else:
            # Default to localhost
            hosts = ["localhost:9200"]
            logger.warning("No OpenSearch hosts configured, using localhost:9200")
        
        # Authentication
        http_auth = None
        username = os.environ.get("OPENSEARCH_USERNAME")
        password = os.environ.get("OPENSEARCH_PASSWORD")
        if username and password:
            http_auth = (username, password)
        
        # AWS credentials
        aws_region = os.environ.get("AWS_REGION")
        aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_session_token = os.environ.get("AWS_SESSION_TOKEN")
        
        return OpenSearchConfig(
            hosts=hosts,
            http_auth=http_auth,
            use_ssl=os.environ.get("OPENSEARCH_USE_SSL", "true").lower() == "true",
            verify_certs=os.environ.get("OPENSEARCH_VERIFY_CERTS", "true").lower() == "true",
            timeout=int(os.environ.get("OPENSEARCH_TIMEOUT", str(DEFAULT_TIMEOUT))),
            max_retries=int(os.environ.get("OPENSEARCH_MAX_RETRIES", str(MAX_RETRIES))),
            retry_on_timeout=os.environ.get("OPENSEARCH_RETRY_ON_TIMEOUT", "true").lower() == "true",
            aws_region=aws_region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=aws_session_token,
            headers=ConfigLoader._parse_headers(),
        )
    
    @staticmethod
    def from_params(
        hosts: List[str],
        http_auth: Optional[Tuple[str, str]] = None,
        use_ssl: bool = True,
        aws_region: Optional[str] = None,
        **kwargs
    ) -> OpenSearchConfig:
        """Load configuration from parameters"""
        return OpenSearchConfig(
            hosts=hosts,
            http_auth=http_auth,
            use_ssl=use_ssl,
            aws_region=aws_region,
            **kwargs
        )
    
    @staticmethod
    def _parse_headers() -> Dict[str, str]:
        """Parse custom headers from environment"""
        headers = {}
        header_prefix = "OPENSEARCH_HEADER_"
        
        for key, value in os.environ.items():
            if key.startswith(header_prefix):
                header_name = key[len(header_prefix):].replace('_', '-').title()
                headers[header_name] = value
        
        return headers


def get_opensearch_config(
    hosts: Optional[List[str]] = None,
    **kwargs
) -> OpenSearchConfig:
    """
    Main function to get OpenSearch configuration
    Supports both environment variables and parameters
    """
    if hosts:
        return ConfigLoader.from_params(hosts=hosts, **kwargs)
    return ConfigLoader.from_environment()