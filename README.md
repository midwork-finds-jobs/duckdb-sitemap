# DuckDB Sitemap Extension

A DuckDB extension for parsing XML sitemaps from websites, with automatic discovery via robots.txt.

## Features

- 🔍 **Automatic sitemap discovery** from `/robots.txt`
- 🗂️ **Sitemap index support** - recursively fetches nested sitemaps
- 🔄 **Retry logic** with exponential backoff and `Retry-After` header support
- 📦 **Gzip support** - automatically decompresses `.xml.gz` sitemaps
- 🌐 **Multiple namespace support** - handles both standard and Google sitemap schemas
- ⚡ **SQL filtering** - use WHERE clauses to filter URLs before processing

## Installation

```sql
INSTALL sitemap FROM community;
LOAD sitemap;
```

## Usage

### Basic Usage

```sql
-- Get all URLs from a sitemap
SELECT * FROM sitemap_urls('https://example.com');

-- Filter specific URLs
SELECT * FROM sitemap_urls('https://example.com')
WHERE url LIKE '%/blog/%';

-- Count URLs by type
SELECT
    CASE
        WHEN url LIKE '%/product/%' THEN 'product'
        WHEN url LIKE '%/blog/%' THEN 'blog'
        ELSE 'other'
    END as type,
    count(*) as count
FROM sitemap_urls('https://example.com')
GROUP BY type;
```

### Compose with http_get

Fetch page content for selected URLs:

```sql
SELECT s.url, h.body
FROM sitemap_urls('https://example.com') s
JOIN LATERAL (SELECT * FROM http_get(s.url)) h ON true
WHERE s.url LIKE '%/product/%'
LIMIT 10;
```

### Advanced Options

```sql
SELECT * FROM sitemap_urls(
    'https://example.com',
    follow_robots := true,      -- Parse robots.txt (default: true)
    max_depth := 3,             -- Max sitemap index nesting (default: 3)
    max_retries := 5,           -- Max retry attempts (default: 5)
    backoff_ms := 100,          -- Initial backoff in ms (default: 100)
    max_backoff_ms := 30000     -- Max backoff cap in ms (default: 30000)
);
```

### Save to Database

```sql
CREATE TABLE products AS
SELECT url, lastmod, changefreq, priority
FROM sitemap_urls('https://example.com')
WHERE url LIKE '%/product/%';
```

## Return Columns

| Column | Type | Description |
|--------|------|-------------|
| `url` | VARCHAR | Page URL |
| `lastmod` | VARCHAR | Last modification date (optional) |
| `changefreq` | VARCHAR | Change frequency hint (optional) |
| `priority` | VARCHAR | Priority hint 0.0-1.0 (optional) |

## How It Works

1. **Fetch robots.txt** - Looks for `Sitemap:` directives
2. **Parse sitemaps** - Handles both `<urlset>` and `<sitemapindex>` formats
3. **Recursive fetching** - Follows sitemap index references
4. **Retry on errors** - Automatically retries on 429, 5xx, and network failures
5. **Return results** - Streams URLs as a table for SQL filtering

## Retry Logic

- **Retryable errors**: 429 (rate limit), 500, 502, 503, 504, network failures
- **Exponential backoff**: 100ms → 200ms → 400ms → 800ms → ...
- **Respects Retry-After header** on 429 responses
- **Jitter**: Adds 10% randomness to prevent thundering herd

## Building from Source

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/midwork-finds-jobs/duckdb-sitemap.git
cd duckdb-sitemap

# Build
make GEN=ninja

# Test
./build/release/duckdb -c "SELECT * FROM sitemap_urls('https://example.com') LIMIT 5;"
```

## Dependencies

- libxml2 - XML parsing
- zlib - Gzip decompression
- http_request extension (from DuckDB community)

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.
