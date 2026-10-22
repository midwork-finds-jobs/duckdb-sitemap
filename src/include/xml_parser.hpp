#pragma once

#include <string>
#include <vector>
#include <libxml/parser.h>
#include <libxml/xpath.h>
#include <libxml/xpathInternals.h>

namespace duckdb {

struct SitemapEntry {
	std::string url;
	std::string lastmod;
	std::string changefreq;
	std::string priority;
};

enum class SitemapType {
	URLSET,      // Regular sitemap with <url> entries
	SITEMAPINDEX // Index pointing to other sitemaps
};

struct SitemapParseResult {
	SitemapType type;
	std::vector<SitemapEntry> urls;    // For URLSET
	std::vector<std::string> sitemaps; // For SITEMAPINDEX
	std::string error;
	bool success = false;
};

// RAII wrapper for libxml2 document
class XMLDocRAII {
public:
	xmlDocPtr doc = nullptr;
	xmlXPathContextPtr xpath_ctx = nullptr;

	explicit XMLDocRAII(const std::string &content);
	~XMLDocRAII();

	// Delete copy
	XMLDocRAII(const XMLDocRAII &) = delete;
	XMLDocRAII &operator=(const XMLDocRAII &) = delete;

	// Allow move
	XMLDocRAII(XMLDocRAII &&other) noexcept;
	XMLDocRAII &operator=(XMLDocRAII &&other) noexcept;

	bool IsValid() const {
		return doc != nullptr;
	}
};

class XmlParser {
public:
	static void Initialize();
	static void Cleanup();

	static SitemapParseResult ParseSitemap(const std::string &xml_content);
	//! Gunzip `compressed` into `output` (passes non-gzip data through); fails on corrupt/truncated data or when
	//! the output would exceed `max_output` bytes
	static bool DecompressGzip(const std::string &compressed, size_t max_output, std::string &output,
	                           std::string &error);
	static bool HasGzipMagic(const std::string &data);
	static bool IsGzipped(const std::string &url, const std::string &content_type);
	static std::vector<std::string> FindSitemapInHtml(const std::string &html_content);
};

} // namespace duckdb
