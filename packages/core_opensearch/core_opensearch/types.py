"""
Type definitions for OpenSearch operations
"""
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime

from .constants import DEFAULT_PAGE_SIZE


class SortOrder(str, Enum):
    """Sort order options"""
    ASC = "asc"
    DESC = "desc"


class SearchType(str, Enum):
    """Search type options"""
    QUERY_THEN_FETCH = "query_then_fetch"
    DFS_QUERY_THEN_FETCH = "dfs_query_then_fetch"


@dataclass
class SortOption:
    """Sort configuration"""
    field: str
    order: SortOrder = SortOrder.ASC
    missing: Optional[str] = None  # "_last", "_first", or custom value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to OpenSearch sort format"""
        sort_dict = {self.field: {"order": self.order.value}}
        if self.missing:
            sort_dict[self.field]["missing"] = self.missing
        return sort_dict


@dataclass
class FacetConfig:
    """Facet/aggregation configuration"""
    field: str
    name: Optional[str] = None
    size: int = 10
    min_doc_count: int = 1
    order: Optional[List[Tuple[str, str]]] = None
    
    def __post_init__(self):
        if not self.name:
            self.name = f"{self.field}_terms"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to OpenSearch aggregation format"""
        aggs = {
            "terms": {
                "field": self.field,
                "size": self.size,
                "min_doc_count": self.min_doc_count
            }
        }
        
        if self.order:
            aggs["terms"]["order"] = self.order
        
        return {self.name: aggs}


@dataclass
class SearchQuery:
    """Search query parameters"""
    query: Dict[str, Any]
    size: int = DEFAULT_PAGE_SIZE
    from_: int = 0
    sort: Optional[List[Union[str, SortOption, Dict[str, Any]]]] = None
    aggs: Optional[Dict[str, Any]] = None
    highlight: Optional[Dict[str, Any]] = None
    source: Optional[Union[bool, List[str]]] = None
    script_fields: Optional[Dict[str, Any]] = None
    track_scores: bool = False
    explain: bool = False
    version: bool = False
    search_type: SearchType = SearchType.QUERY_THEN_FETCH
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to OpenSearch search body"""
        body = {"query": self.query}
        
        if self.size != DEFAULT_PAGE_SIZE:
            body["size"] = self.size
        
        if self.from_ > 0:
            body["from"] = self.from_
        
        if self.sort:
            body["sort"] = self._format_sort(self.sort)
        
        if self.aggs:
            body["aggs"] = self.aggs
        
        if self.highlight:
            body["highlight"] = self.highlight
        
        if self.source is not None:
            body["_source"] = self.source
        
        if self.script_fields:
            body["script_fields"] = self.script_fields
        
        if self.track_scores:
            body["track_scores"] = True
        
        if self.explain:
            body["explain"] = True
        
        if self.version:
            body["version"] = True
        
        return body
    
    def _format_sort(self, sort_spec):
        """Format sort specification"""
        formatted = []
        for item in sort_spec:
            if isinstance(item, str):
                formatted.append(item)
            elif isinstance(item, SortOption):
                formatted.append(item.to_dict())
            elif isinstance(item, dict):
                formatted.append(item)
        return formatted


@dataclass
class IndexSettings:
    """Index settings configuration"""
    number_of_shards: int = 1
    number_of_replicas: int = 1
    refresh_interval: str = "1s"
    max_result_window: int = 10000
    analysis: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to OpenSearch settings format"""
        settings = {
            "index": {
                "number_of_shards": self.number_of_shards,
                "number_of_replicas": self.number_of_replicas,
                "refresh_interval": self.refresh_interval,
                "max_result_window": self.max_result_window,
            }
        }
        
        if self.analysis:
            settings["index"]["analysis"] = self.analysis
        
        return settings


@dataclass
class IndexMappings:
    """Index mappings configuration"""
    properties: Dict[str, Any]
    dynamic: str = "strict"  # "strict", "true", or "false"
    date_detection: bool = True
    numeric_detection: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to OpenSearch mappings format"""
        mappings = {
            "dynamic": self.dynamic,
            "date_detection": self.date_detection,
            "numeric_detection": self.numeric_detection,
            "properties": self.properties
        }
        
        return mappings


@dataclass
class BulkOperationResult:
    """Result of bulk operations"""
    total: int = 0
    successful: int = 0
    failed: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    took: int = 0
    has_errors: bool = False
    
    def add_error(self, error: Dict[str, Any]):
        """Add error to result"""
        self.errors.append(error)
        self.failed += 1
        self.has_errors = True


@dataclass
class SearchResult:
    """Search result with metadata"""
    hits: List[Dict[str, Any]]
    total: int
    took: int
    aggregations: Optional[Dict[str, Any]] = None
    scroll_id: Optional[str] = None
    shards: Optional[Dict[str, Any]] = None
    
    @property
    def has_hits(self) -> bool:
        """Check if there are any hits"""
        return len(self.hits) > 0
    
    @property
    def page_count(self, per_page: int = DEFAULT_PAGE_SIZE) -> int:
        """Calculate total pages"""
        return (self.total + per_page - 1) // per_page if self.total > 0 else 1


@dataclass
class IndexStats:
    """Index statistics"""
    name: str
    docs_count: int
    docs_deleted: int
    store_size: str
    store_size_bytes: int
    segments_count: int
    memory_size: str
    memory_size_bytes: int
    refresh_total: int
    refresh_time: str
    search_total: int
    search_time: str
    indexing_total: int
    indexing_time: str
    created_at: datetime
    last_updated: datetime