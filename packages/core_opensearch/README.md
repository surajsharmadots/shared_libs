# Core OpenSearch Wrapper

A high-performance, async-first OpenSearch client designed specifically for e-commerce search and analytics.

## Features

- 🔍 **Advanced Search** - Full-text, faceted, fuzzy, and geo search
- ⚡ **Real-time Indexing** - Near real-time document updates
- 📊 **Analytics** - Aggregations, metrics, and complex queries
- 🔄 **Bulk Operations** - Efficient bulk indexing with retry logic
- 🏪 **E-commerce Optimized** - Product search, autocomplete, recommendations
- ☁️ **Multi-cloud Support** - AWS OpenSearch, self-hosted, Docker
- 🔐 **Security** - AWS SigV4, TLS, authentication
- 📈 **Monitoring** - Performance metrics and query analytics

## Installation

```bash
# Install core package
pip install core-opensearch

# With AWS support (for AWS OpenSearch Service)
pip install "core-opensearch[aws]"

# Development dependencies
pip install "core-opensearch[dev]"