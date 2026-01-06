"""
OpenSearch query builder for e-commerce search patterns
"""
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta

from .constants import DEFAULT_FUZZINESS, DEFAULT_MIN_SHOULD_MATCH
from .exceptions import SearchQueryError


class OpenSearchQueryBuilder:
    """
    Builder for OpenSearch queries with e-commerce optimizations
    
    Features:
    - Product search with filters
    - Faceted search queries
    - Autocomplete and suggestion queries
    - Range and geo queries
    - Aggregation queries
    """
    
    @staticmethod
    def build_product_search_query(
        query_text: str,
        filters: Optional[Dict[str, Any]] = None,
        category: Optional[str] = None,
        price_range: Optional[tuple] = None,
        brand: Optional[Union[str, List[str]]] = None,
        attributes: Optional[Dict[str, Any]] = None,
        in_stock: bool = True
    ) -> Dict[str, Any]:
        """
        Build e-commerce product search query
        
        Args:
            query_text: Search query text
            filters: Additional filters
            category: Product category filter
            price_range: (min_price, max_price)
            brand: Brand filter (single or list)
            attributes: Product attributes filter
            in_stock: Filter by stock availability
            
        Returns:
            OpenSearch query DSL
        """
        must_clauses = []
        filter_clauses = []
        
        # Text search with boosting
        if query_text:
            must_clauses.append({
                "multi_match": {
                    "query": query_text,
                    "fields": [
                        "name^3",           # Highest boost for name
                        "description^2",    # Medium boost for description
                        "category^1.5",     # Some boost for category
                        "brand^1.2",        # Slight boost for brand
                        "tags^1",           # Standard boost for tags
                        "sku"               # No boost for SKU
                    ],
                    "fuzziness": DEFAULT_FUZZINESS,
                    "minimum_should_match": DEFAULT_MIN_SHOULD_MATCH,
                    "type": "best_fields"
                }
            })
        
        # Category filter
        if category:
            filter_clauses.append({
                "term": {"category.keyword": category}
            })
        
        # Price range filter
        if price_range:
            min_price, max_price = price_range
            filter_clauses.append({
                "range": {
                    "price": {
                        "gte": min_price,
                        "lte": max_price
                    }
                }
            })
        
        # Brand filter
        if brand:
            if isinstance(brand, list):
                filter_clauses.append({
                    "terms": {"brand.keyword": brand}
                })
            else:
                filter_clauses.append({
                    "term": {"brand.keyword": brand}
                })
        
        # Stock availability filter
        if in_stock:
            filter_clauses.append({
                "range": {
                    "stock": {
                        "gt": 0
                    }
                }
            })
        
        # Additional filters
        if filters:
            for field, value in filters.items():
                if isinstance(value, list):
                    filter_clauses.append({
                        "terms": {f"{field}.keyword": value}
                    })
                else:
                    filter_clauses.append({
                        "term": {f"{field}.keyword": value}
                    })
        
        # Attributes filter (nested)
        if attributes:
            for attr_name, attr_value in attributes.items():
                filter_clauses.append({
                    "nested": {
                        "path": "attributes",
                        "query": {
                            "bool": {
                                "must": [
                                    {"term": {"attributes.name.keyword": attr_name}},
                                    {"term": {"attributes.value.keyword": attr_value}}
                                ]
                            }
                        }
                    }
                })
        
        # Build final query
        query = {}
        
        if must_clauses:
            query["must"] = must_clauses
        
        if filter_clauses:
            query["filter"] = filter_clauses
        
        # If no must clauses, use filter only
        if not must_clauses and filter_clauses:
            return {
                "bool": {
                    "filter": filter_clauses
                }
            }
        
        # If no filter clauses, use must only
        if must_clauses and not filter_clauses:
            return {
                "bool": {
                    "must": must_clauses
                }
            }
        
        # Both must and filter clauses
        if must_clauses and filter_clauses:
            return {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            }
        
        # No clauses, return match_all
        return {"match_all": {}}
    
    @staticmethod
    def build_product_facets() -> Dict[str, Any]:
        """
        Build aggregations for product search facets
        
        Returns:
            Aggregations for facets
        """
        return {
            "categories": {
                "terms": {
                    "field": "category.keyword",
                    "size": 10,
                    "order": {"_count": "desc"}
                }
            },
            "brands": {
                "terms": {
                    "field": "brand.keyword",
                    "size": 10,
                    "order": {"_count": "desc"}
                }
            },
            "price_ranges": {
                "range": {
                    "field": "price",
                    "ranges": [
                        {"key": "Under ₹1000", "to": 1000},
                        {"key": "₹1000 - ₹5000", "from": 1000, "to": 5000},
                        {"key": "₹5000 - ₹10000", "from": 5000, "to": 10000},
                        {"key": "₹10000 - ₹20000", "from": 10000, "to": 20000},
                        {"key": "Above ₹20000", "from": 20000}
                    ]
                }
            },
            "ratings": {
                "histogram": {
                    "field": "average_rating",
                    "interval": 1,
                    "extended_bounds": {
                        "min": 0,
                        "max": 5
                    }
                }
            }
        }
    
    @staticmethod
    def build_autocomplete_query(
        field: str,
        prefix: str,
        size: int = 5
    ) -> Dict[str, Any]:
        """
        Build autocomplete/suggestion query
        
        Args:
            field: Field to search
            prefix: User input prefix
            size: Number of suggestions
            
        Returns:
            Suggestion query
        """
        return {
            "suggest": {
                "autocomplete": {
                    "prefix": prefix,
                    "completion": {
                        "field": f"{field}.suggest",
                        "size": size,
                        "skip_duplicates": True,
                        "fuzzy": {
                            "fuzziness": 1,
                            "min_length": 3,
                            "prefix_length": 1
                        }
                    }
                }
            }
        }
    
    @staticmethod
    def build_filter_query(
        field: str,
        operator: str,
        value: Any,
        value2: Any = None
    ) -> Dict[str, Any]:
        """
        Build filter query for different operators
        
        Args:
            field: Field name
            operator: Operator (eq, ne, gt, gte, lt, lte, in, range, exists)
            value: Filter value
            value2: Second value for range queries
            
        Returns:
            Filter query
        
        Raises:
            SearchQueryError: If operator is invalid
        """
        operator_map = {
            "eq": lambda f, v: {"term": {f: v}},
            "ne": lambda f, v: {"bool": {"must_not": [{"term": {f: v}}]}},
            "gt": lambda f, v: {"range": {f: {"gt": v}}},
            "gte": lambda f, v: {"range": {f: {"gte": v}}},
            "lt": lambda f, v: {"range": {f: {"lt": v}}},
            "lte": lambda f, v: {"range": {f: {"lte": v}}},
            "in": lambda f, v: {"terms": {f: v if isinstance(v, list) else [v]}},
            "range": lambda f, v: {"range": {f: {"gte": v[0], "lte": v[1]}}},
            "exists": lambda f, _: {"exists": {"field": f}},
            "missing": lambda f, _: {"bool": {"must_not": [{"exists": {"field": f}}]}},
            "prefix": lambda f, v: {"prefix": {f: v}},
            "wildcard": lambda f, v: {"wildcard": {f: v}},
            "regexp": lambda f, v: {"regexp": {f: v}},
        }
        
        if operator not in operator_map:
            raise SearchQueryError(f"Invalid operator: {operator}")
        
        # Handle range operator
        if operator == "range":
            if not isinstance(value, (tuple, list)) or len(value) != 2:
                raise SearchQueryError("Range operator requires tuple/list of length 2")
            return operator_map[operator](field, value)
        
        return operator_map[operator](field, value)
    
    @staticmethod
    def build_date_range_query(
        field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        time_zone: str = "UTC"
    ) -> Dict[str, Any]:
        """
        Build date range query
        
        Args:
            field: Date field name
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            time_zone: Timezone
            
        Returns:
            Date range query
        """
        range_query = {"range": {field: {"time_zone": time_zone}}}
        
        if start_date:
            range_query["range"][field]["gte"] = start_date.isoformat()
        
        if end_date:
            range_query["range"][field]["lte"] = end_date.isoformat()
        
        return range_query
    
    @staticmethod
    def build_recent_documents_query(
        field: str = "created_at",
        days: int = 30
    ) -> Dict[str, Any]:
        """
        Build query for recent documents
        
        Args:
            field: Date field name
            days: Number of days
            
        Returns:
            Recent documents query
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        return {
            "range": {
                field: {
                    "gte": cutoff_date.isoformat(),
                    "time_zone": "UTC"
                }
            }
        }
    
    @staticmethod
    def build_geo_distance_query(
        field: str,
        lat: float,
        lon: float,
        distance: str = "10km"
    ) -> Dict[str, Any]:
        """
        Build geo distance query
        
        Args:
            field: Geo point field name
            lat: Latitude
            lon: Longitude
            distance: Distance (e.g., "10km", "5mi")
            
        Returns:
            Geo distance query
        """
        return {
            "geo_distance": {
                "distance": distance,
                field: {
                    "lat": lat,
                    "lon": lon
                }
            }
        }
    
    @staticmethod
    def build_aggregation_query(
        aggregations: Dict[str, Any],
        query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Build aggregation query
        
        Args:
            aggregations: Aggregation definitions
            query: Optional query to filter documents
            
        Returns:
            Aggregation query
        """
        body = {"aggs": aggregations, "size": 0}
        
        if query:
            body["query"] = query
        
        return body
    
    @staticmethod
    def build_scoring_query(
        base_query: Dict[str, Any],
        scoring_functions: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Build query with custom scoring
        
        Args:
            base_query: Base query
            scoring_functions: List of scoring functions
            
        Returns:
            Function score query
        """
        return {
            "function_score": {
                "query": base_query,
                "functions": scoring_functions,
                "score_mode": "multiply",
                "boost_mode": "multiply"
            }
        }
    
    @staticmethod
    def build_more_like_this_query(
        fields: List[str],
        like_texts: List[str] = None,
        like_docs: List[Dict[str, str]] = None,
        min_term_freq: int = 1,
        max_query_terms: int = 12,
        min_doc_freq: int = 1
    ) -> Dict[str, Any]:
        """
        Build 'more like this' query
        
        Args:
            fields: Fields to compare
            like_texts: Texts to find similar documents to
            like_docs: Documents to find similar documents to
            min_term_freq: Minimum term frequency
            max_query_terms: Maximum query terms
            min_doc_freq: Minimum document frequency
            
        Returns:
            More like this query
        """
        mlt_query = {
            "more_like_this": {
                "fields": fields,
                "min_term_freq": min_term_freq,
                "max_query_terms": max_query_terms,
                "min_doc_freq": min_doc_freq,
                "min_word_length": 3
            }
        }
        
        if like_texts:
            mlt_query["more_like_this"]["like"] = like_texts
        
        if like_docs:
            if "like" not in mlt_query["more_like_this"]:
                mlt_query["more_like_this"]["like"] = []
            mlt_query["more_like_this"]["like"].extend(like_docs)
        
        return mlt_query