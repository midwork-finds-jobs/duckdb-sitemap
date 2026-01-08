#include "xml_parser.hpp"
#include <libxml/HTMLparser.h>
#include <zlib.h>
#include <cstring>
#include <algorithm>

namespace duckdb {

// Silent error handler for libxml2
static void SilentErrorHandler(void *ctx, const char *msg, ...) {
	// Suppress all libxml2 error output
}

// Initialize libxml2 (call once at extension load)
void XmlParser::Initialize() {
	xmlInitParser();
}

// Cleanup libxml2 (optional, for clean shutdown)
void XmlParser::Cleanup() {
	xmlCleanupParser();
}

// XMLDocRAII implementation
XMLDocRAII::XMLDocRAII(const std::string &content) {
	// Suppress error output
	xmlSetGenericErrorFunc(nullptr, SilentErrorHandler);

	doc = xmlReadMemory(content.c_str(), static_cast<int>(content.size()), nullptr, nullptr,
	                    XML_PARSE_NOERROR | XML_PARSE_NOWARNING | XML_PARSE_NONET);

	if (doc) {
		xpath_ctx = xmlXPathNewContext(doc);
		if (xpath_ctx) {
			// Register both sitemap namespace variants
			xmlXPathRegisterNs(xpath_ctx, BAD_CAST "sm",
			                   BAD_CAST "http://www.sitemaps.org/schemas/sitemap/0.9");
			xmlXPathRegisterNs(xpath_ctx, BAD_CAST "sm2",
			                   BAD_CAST "http://www.google.com/schemas/sitemap/0.84");
		}
	}
}

XMLDocRAII::~XMLDocRAII() {
	if (xpath_ctx) {
		xmlXPathFreeContext(xpath_ctx);
	}
	if (doc) {
		xmlFreeDoc(doc);
	}
}

XMLDocRAII::XMLDocRAII(XMLDocRAII &&other) noexcept : doc(other.doc), xpath_ctx(other.xpath_ctx) {
	other.doc = nullptr;
	other.xpath_ctx = nullptr;
}

XMLDocRAII &XMLDocRAII::operator=(XMLDocRAII &&other) noexcept {
	if (this != &other) {
		if (xpath_ctx) {
			xmlXPathFreeContext(xpath_ctx);
		}
		if (doc) {
			xmlFreeDoc(doc);
		}
		doc = other.doc;
		xpath_ctx = other.xpath_ctx;
		other.doc = nullptr;
		other.xpath_ctx = nullptr;
	}
	return *this;
}

// Get text content of first matching XPath node
static std::string GetXPathText(xmlXPathContextPtr ctx, xmlNodePtr node, const char *xpath) {
	// Set context node
	xmlXPathContextPtr local_ctx = xmlXPathNewContext(ctx->doc);
	if (!local_ctx) {
		return "";
	}

	// Register both namespace variants
	xmlXPathRegisterNs(local_ctx, BAD_CAST "sm", BAD_CAST "http://www.sitemaps.org/schemas/sitemap/0.9");
	xmlXPathRegisterNs(local_ctx, BAD_CAST "sm2", BAD_CAST "http://www.google.com/schemas/sitemap/0.84");

	local_ctx->node = node;

	xmlXPathObjectPtr result = xmlXPathEvalExpression(BAD_CAST xpath, local_ctx);
	std::string text;

	if (result && result->nodesetval && result->nodesetval->nodeNr > 0) {
		xmlNodePtr text_node = result->nodesetval->nodeTab[0];
		xmlChar *content = xmlNodeGetContent(text_node);
		if (content) {
			text = reinterpret_cast<const char *>(content);
			xmlFree(content);
		}
	}

	if (result) {
		xmlXPathFreeObject(result);
	}
	xmlXPathFreeContext(local_ctx);

	return text;
}

SitemapParseResult XmlParser::ParseSitemap(const std::string &xml_content) {
	SitemapParseResult result;

	XMLDocRAII doc(xml_content);
	if (!doc.IsValid()) {
		result.error = "Failed to parse XML";
		return result;
	}

	xmlNodePtr root = xmlDocGetRootElement(doc.doc);
	if (!root) {
		result.error = "No root element found";
		return result;
	}

	// Get root element name (without namespace prefix)
	std::string root_name = reinterpret_cast<const char *>(root->name);

	if (root_name == "sitemapindex") {
		// This is a sitemap index
		result.type = SitemapType::SITEMAPINDEX;

		// Try both namespace variants
		const char *xpath_variants[] = {"//sm:sitemap/sm:loc", "//sm2:sitemap/sm2:loc"};
		for (const char *xpath : xpath_variants) {
			xmlXPathObjectPtr sitemap_nodes = xmlXPathEvalExpression(BAD_CAST xpath, doc.xpath_ctx);

			if (sitemap_nodes && sitemap_nodes->nodesetval && sitemap_nodes->nodesetval->nodeNr > 0) {
				for (int i = 0; i < sitemap_nodes->nodesetval->nodeNr; i++) {
					xmlNodePtr node = sitemap_nodes->nodesetval->nodeTab[i];
					xmlChar *content = xmlNodeGetContent(node);
					if (content) {
						std::string loc = reinterpret_cast<const char *>(content);
						// Trim whitespace
						size_t start = loc.find_first_not_of(" \t\n\r");
						size_t end = loc.find_last_not_of(" \t\n\r");
						if (start != std::string::npos && end != std::string::npos) {
							result.sitemaps.push_back(loc.substr(start, end - start + 1));
						}
						xmlFree(content);
					}
				}
			}

			if (sitemap_nodes) {
				xmlXPathFreeObject(sitemap_nodes);
			}

			// If we found results, stop trying other namespaces
			if (!result.sitemaps.empty()) {
				break;
			}
		}

		result.success = true;

	} else if (root_name == "urlset") {
		// This is a regular sitemap
		result.type = SitemapType::URLSET;

		// Try both namespace variants
		const char *xpath_variants[] = {"//sm:url", "//sm2:url"};
		const char *loc_variants[] = {"sm:loc", "sm2:loc"};
		const char *lastmod_variants[] = {"sm:lastmod", "sm2:lastmod"};
		const char *changefreq_variants[] = {"sm:changefreq", "sm2:changefreq"};
		const char *priority_variants[] = {"sm:priority", "sm2:priority"};

		for (int ns_idx = 0; ns_idx < 2; ns_idx++) {
			xmlXPathObjectPtr url_nodes = xmlXPathEvalExpression(BAD_CAST xpath_variants[ns_idx], doc.xpath_ctx);

			if (url_nodes && url_nodes->nodesetval && url_nodes->nodesetval->nodeNr > 0) {
				for (int i = 0; i < url_nodes->nodesetval->nodeNr; i++) {
					xmlNodePtr url_node = url_nodes->nodesetval->nodeTab[i];

					SitemapEntry entry;
					entry.url = GetXPathText(doc.xpath_ctx, url_node, loc_variants[ns_idx]);
					entry.lastmod = GetXPathText(doc.xpath_ctx, url_node, lastmod_variants[ns_idx]);
					entry.changefreq = GetXPathText(doc.xpath_ctx, url_node, changefreq_variants[ns_idx]);
					entry.priority = GetXPathText(doc.xpath_ctx, url_node, priority_variants[ns_idx]);

					// Trim URL whitespace
					size_t start = entry.url.find_first_not_of(" \t\n\r");
					size_t end = entry.url.find_last_not_of(" \t\n\r");
					if (start != std::string::npos && end != std::string::npos) {
						entry.url = entry.url.substr(start, end - start + 1);
					}

					if (!entry.url.empty()) {
						result.urls.push_back(std::move(entry));
					}
				}
			}

			if (url_nodes) {
				xmlXPathFreeObject(url_nodes);
			}

			// If we found results, stop trying other namespaces
			if (!result.urls.empty()) {
				break;
			}
		}

		result.success = true;

	} else {
		result.error = "Unknown root element: " + root_name;
	}

	return result;
}

bool XmlParser::IsGzipped(const std::string &url, const std::string &content_type) {
	// Check URL extension
	if (url.length() >= 3) {
		std::string lower_url = url;
		std::transform(lower_url.begin(), lower_url.end(), lower_url.begin(),
		               [](unsigned char c) { return std::tolower(c); });
		if (lower_url.substr(lower_url.length() - 3) == ".gz") {
			return true;
		}
	}

	// Check content-type
	if (content_type.find("gzip") != std::string::npos) {
		return true;
	}

	// Check for gzip magic bytes
	// gzip files start with 0x1f 0x8b
	// (We'd need to check the actual content for this, but we don't have it here)

	return false;
}

std::string XmlParser::DecompressGzip(const std::string &compressed) {
	// Check for gzip magic bytes
	if (compressed.size() < 2 ||
	    static_cast<unsigned char>(compressed[0]) != 0x1f ||
	    static_cast<unsigned char>(compressed[1]) != 0x8b) {
		// Not gzipped, return as-is
		return compressed;
	}

	z_stream zs;
	memset(&zs, 0, sizeof(zs));

	// 16 + MAX_WBITS for gzip format
	if (inflateInit2(&zs, 16 + MAX_WBITS) != Z_OK) {
		return ""; // Decompression init failed
	}

	zs.next_in = reinterpret_cast<Bytef *>(const_cast<char *>(compressed.data()));
	zs.avail_in = static_cast<uInt>(compressed.size());

	std::string decompressed;
	char buffer[32768];

	int ret;
	do {
		zs.next_out = reinterpret_cast<Bytef *>(buffer);
		zs.avail_out = sizeof(buffer);

		ret = inflate(&zs, Z_NO_FLUSH);

		if (ret != Z_OK && ret != Z_STREAM_END && ret != Z_BUF_ERROR) {
			inflateEnd(&zs);
			return ""; // Decompression failed
		}

		size_t have = sizeof(buffer) - zs.avail_out;
		decompressed.append(buffer, have);

	} while (ret != Z_STREAM_END);

	inflateEnd(&zs);
	return decompressed;
}

std::vector<std::string> XmlParser::FindSitemapInHtml(const std::string &html_content) {
	std::vector<std::string> sitemaps;

	// Parse HTML with libxml2 in HTML mode
	xmlDocPtr doc = htmlReadMemory(html_content.c_str(), static_cast<int>(html_content.size()),
	                               nullptr, nullptr,
	                               HTML_PARSE_RECOVER | HTML_PARSE_NOERROR | HTML_PARSE_NOWARNING);

	if (!doc) {
		return sitemaps;
	}

	xmlXPathContextPtr xpath_ctx = xmlXPathNewContext(doc);
	if (!xpath_ctx) {
		xmlFreeDoc(doc);
		return sitemaps;
	}

	// Look for <link rel="sitemap"> tags
	const char *link_xpath = "//link[@rel='sitemap' or @rel='Sitemap']/@href";
	xmlXPathObjectPtr link_nodes = xmlXPathEvalExpression(BAD_CAST link_xpath, xpath_ctx);

	if (link_nodes && link_nodes->nodesetval) {
		for (int i = 0; i < link_nodes->nodesetval->nodeNr; i++) {
			xmlNodePtr node = link_nodes->nodesetval->nodeTab[i];
			xmlChar *href = xmlNodeGetContent(node);
			if (href) {
				sitemaps.push_back(reinterpret_cast<const char *>(href));
				xmlFree(href);
			}
		}
	}
	if (link_nodes) {
		xmlXPathFreeObject(link_nodes);
	}

	xmlXPathFreeContext(xpath_ctx);
	xmlFreeDoc(doc);

	return sitemaps;
}

} // namespace duckdb
