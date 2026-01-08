#include "sitemap_function.hpp"
#include "http_client.hpp"
#include "robots_parser.hpp"
#include "xml_parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>
#include <unordered_map>

namespace duckdb {

// Bind data for sitemap_urls() table function
struct SitemapBindData : public TableFunctionData {
	std::vector<std::string> base_urls;
	bool follow_robots = true;
	int max_depth = 3;
	bool ignore_errors = false;
	RetryConfig retry_config;
	std::string user_agent;
};

// Session-level cache for discovered sitemap URLs
struct SitemapCache {
	std::unordered_map<std::string, std::vector<std::string>> discovered_sitemaps;
	std::mutex cache_mutex;

	static SitemapCache &GetInstance() {
		static SitemapCache instance;
		return instance;
	}

	std::vector<std::string> Get(const std::string &base_url) {
		std::lock_guard<std::mutex> lock(cache_mutex);
		auto it = discovered_sitemaps.find(base_url);
		if (it != discovered_sitemaps.end()) {
			return it->second;
		}
		return {};
	}

	void Set(const std::string &base_url, const std::vector<std::string> &sitemaps) {
		std::lock_guard<std::mutex> lock(cache_mutex);
		discovered_sitemaps[base_url] = sitemaps;
	}
};

// Global state for sitemap_urls() table function
struct SitemapGlobalState : public GlobalTableFunctionState {
	std::vector<SitemapEntry> entries;
	idx_t current_idx = 0;
	std::vector<std::string> errors;
	bool fetch_complete = false;
	std::mutex mutex;

	idx_t MaxThreads() const override {
		return 1; // Single-threaded for HTTP fetching
	}
};

// Local state for per-thread execution
struct SitemapLocalState : public LocalTableFunctionState {
	idx_t local_idx = 0;
};

// Build full URL from base and path
static std::string BuildUrl(const std::string &base_url, const std::string &path) {
	// Remove trailing slash from base
	std::string base = base_url;
	while (!base.empty() && base.back() == '/') {
		base.pop_back();
	}

	// Ensure path starts with slash
	if (path.empty() || path[0] != '/') {
		return base + "/" + path;
	}
	return base + path;
}

// Fetch and process a single sitemap (may be urlset or sitemapindex)
static void FetchSitemap(ClientContext &context, const std::string &sitemap_url, SitemapGlobalState &state,
                         const SitemapBindData &bind_data, int current_depth) {
	if (current_depth > bind_data.max_depth) {
		return; // Prevent infinite recursion
	}

	auto response = HttpClient::Fetch(context, sitemap_url, bind_data.retry_config, bind_data.user_agent);

	if (!response.success) {
		std::lock_guard<std::mutex> lock(state.mutex);
		state.errors.push_back("Failed to fetch " + sitemap_url + ": " + response.error);
		return;
	}

	// Check if gzipped and decompress
	std::string content = response.body;
	if (XmlParser::IsGzipped(sitemap_url, response.content_type)) {
		content = XmlParser::DecompressGzip(response.body);
		if (content.empty()) {
			std::lock_guard<std::mutex> lock(state.mutex);
			state.errors.push_back("Failed to decompress gzipped sitemap: " + sitemap_url);
			return;
		}
	}

	// Parse the sitemap
	auto result = XmlParser::ParseSitemap(content);

	if (!result.success) {
		std::lock_guard<std::mutex> lock(state.mutex);
		state.errors.push_back("Failed to parse sitemap " + sitemap_url + ": " + result.error);
		return;
	}

	if (result.type == SitemapType::URLSET) {
		// Add URLs to state
		std::lock_guard<std::mutex> lock(state.mutex);
		state.entries.insert(state.entries.end(), result.urls.begin(), result.urls.end());
	} else {
		// Sitemap index - recursively fetch child sitemaps
		for (const auto &child_url : result.sitemaps) {
			FetchSitemap(context, child_url, state, bind_data, current_depth + 1);
		}
	}
}

// Bind function
static unique_ptr<FunctionData> SitemapBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<SitemapBindData>();

	// First positional argument is the base URL(s)
	if (input.inputs.empty()) {
		throw InvalidInputException("sitemap_urls() requires a base_url argument");
	}

	auto &first_param = input.inputs[0];

	// Handle both single string and list of strings
	if (first_param.type().id() == LogicalTypeId::VARCHAR) {
		// Single URL
		std::string url = first_param.GetValue<std::string>();
		// Auto-prepend https:// if no protocol specified
		if (url.find("://") == std::string::npos) {
			url = "https://" + url;
		}
		bind_data->base_urls.push_back(url);
	} else if (first_param.type().id() == LogicalTypeId::LIST) {
		// Array of URLs
		auto list_value = first_param;
		auto &children = ListValue::GetChildren(list_value);

		if (children.empty()) {
			throw InvalidInputException("sitemap_urls() requires at least one URL");
		}

		for (auto &child : children) {
			std::string url = child.GetValue<std::string>();
			// Auto-prepend https:// if no protocol specified
			if (url.find("://") == std::string::npos) {
				url = "https://" + url;
			}
			bind_data->base_urls.push_back(url);
		}
	} else {
		throw InvalidInputException("sitemap_urls() first argument must be VARCHAR or LIST(VARCHAR)");
	}

	// Get user agent from extension setting
	Value user_agent_value;
	if (context.TryGetCurrentSetting("sitemap_user_agent", user_agent_value)) {
		bind_data->user_agent = user_agent_value.GetValue<std::string>();
	}

	// Parse named parameters
	for (auto &kv : input.named_parameters) {
		auto key = StringUtil::Lower(kv.first);
		if (key == "follow_robots") {
			bind_data->follow_robots = kv.second.GetValue<bool>();
		} else if (key == "max_depth") {
			bind_data->max_depth = kv.second.GetValue<int>();
		} else if (key == "max_retries") {
			bind_data->retry_config.max_retries = kv.second.GetValue<int>();
		} else if (key == "backoff_ms") {
			bind_data->retry_config.initial_backoff_ms = kv.second.GetValue<int>();
		} else if (key == "max_backoff_ms") {
			bind_data->retry_config.max_backoff_ms = kv.second.GetValue<int>();
		} else if (key == "ignore_errors") {
			bind_data->ignore_errors = kv.second.GetValue<bool>();
		}
	}

	// Set return types
	names = {"url", "lastmod", "changefreq", "priority"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};

	return std::move(bind_data);
}

// Check if URL points directly to a sitemap file
static bool IsSitemapUrl(const std::string &url) {
	std::string lower_url = url;
	std::transform(lower_url.begin(), lower_url.end(), lower_url.begin(),
	               [](unsigned char c) { return std::tolower(c); });

	// Check for common sitemap file patterns
	return lower_url.find("sitemap") != std::string::npos &&
	       (lower_url.find(".xml") != std::string::npos ||
	        lower_url.find(".xml.gz") != std::string::npos);
}

// Discover sitemap URLs for a base URL using multiple fallback methods
static std::vector<std::string> DiscoverSitemapUrls(ClientContext &context, const std::string &base_url,
                                                     const SitemapBindData &bind_data) {
	auto &cache = SitemapCache::GetInstance();

	// If URL points directly to sitemap, use it without discovery
	if (IsSitemapUrl(base_url)) {
		return {base_url};
	}

	// Check cache first
	auto cached = cache.Get(base_url);
	if (!cached.empty()) {
		return cached;
	}

	std::vector<std::string> sitemap_urls;

	// 1. Try robots.txt
	if (bind_data.follow_robots) {
		std::string robots_url = BuildUrl(base_url, "/robots.txt");
		auto response = HttpClient::Fetch(context, robots_url, bind_data.retry_config, bind_data.user_agent);

		if (response.success) {
			sitemap_urls = RobotsParser::ParseSitemapUrls(response.body);
			if (!sitemap_urls.empty()) {
				cache.Set(base_url, sitemap_urls);
				return sitemap_urls;
			}
		}
	}

	// 2. Try /sitemap.xml
	std::string sitemap_xml_url = BuildUrl(base_url, "/sitemap.xml");
	auto sitemap_response = HttpClient::Fetch(context, sitemap_xml_url, bind_data.retry_config, bind_data.user_agent);
	if (sitemap_response.success) {
		sitemap_urls.push_back(sitemap_xml_url);
		cache.Set(base_url, sitemap_urls);
		return sitemap_urls;
	}

	// 3. Try /sitemap_index.xml
	std::string sitemap_index_url = BuildUrl(base_url, "/sitemap_index.xml");
	auto index_response = HttpClient::Fetch(context, sitemap_index_url, bind_data.retry_config, bind_data.user_agent);
	if (index_response.success) {
		sitemap_urls.push_back(sitemap_index_url);
		cache.Set(base_url, sitemap_urls);
		return sitemap_urls;
	}

	// 4. Try parsing HTML from homepage
	std::string homepage_url = base_url;
	auto html_response = HttpClient::Fetch(context, homepage_url, bind_data.retry_config, bind_data.user_agent);
	if (html_response.success) {
		auto html_sitemaps = XmlParser::FindSitemapInHtml(html_response.body);
		if (!html_sitemaps.empty()) {
			// Convert relative URLs to absolute
			for (auto &sitemap_url : html_sitemaps) {
				if (sitemap_url.find("://") == std::string::npos) {
					// Relative URL - make it absolute
					if (sitemap_url[0] == '/') {
						sitemap_url = base_url + sitemap_url;
					} else {
						sitemap_url = base_url + "/" + sitemap_url;
					}
				}
				sitemap_urls.push_back(sitemap_url);
			}
			cache.Set(base_url, sitemap_urls);
			return sitemap_urls;
		}
	}

	// Nothing found - return empty (will trigger error if ignore_errors=false)
	return sitemap_urls;
}

// Global init - fetch all sitemaps
static unique_ptr<GlobalTableFunctionState> SitemapInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<SitemapGlobalState>();
	auto &bind_data = input.bind_data->Cast<SitemapBindData>();

	// Process each base URL
	for (const auto &base_url : bind_data.base_urls) {
		// Discover sitemap URLs using fallback methods
		std::vector<std::string> sitemap_urls = DiscoverSitemapUrls(context, base_url, bind_data);

		// Track initial error count
		size_t initial_error_count = state->errors.size();
		size_t initial_entry_count = state->entries.size();

		// Fetch all sitemaps for this base URL
		for (const auto &sitemap_url : sitemap_urls) {
			FetchSitemap(context, sitemap_url, *state, bind_data, 0);
		}

		// Check if any URLs were found for this base_url
		bool found_urls = state->entries.size() > initial_entry_count;
		bool had_errors = state->errors.size() > initial_error_count;

		// If no URLs found and not ignoring errors, throw exception
		if (!found_urls && !bind_data.ignore_errors) {
			std::string error_msg = "Failed to find sitemap for " + base_url;
			if (had_errors && !state->errors.empty()) {
				// Include the last error message
				error_msg += ": " + state->errors.back();
			}
			throw IOException(error_msg);
		}
	}

	state->fetch_complete = true;
	return std::move(state);
}

// Local init
static unique_ptr<LocalTableFunctionState> SitemapInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	return make_uniq<SitemapLocalState>();
}

// Scan function - return entries in batches
static void SitemapScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<SitemapGlobalState>();

	idx_t count = 0;
	idx_t max_count = STANDARD_VECTOR_SIZE;

	while (count < max_count && state.current_idx < state.entries.size()) {
		auto &entry = state.entries[state.current_idx];

		output.SetValue(0, count, Value(entry.url));
		output.SetValue(1, count, entry.lastmod.empty() ? Value() : Value(entry.lastmod));
		output.SetValue(2, count, entry.changefreq.empty() ? Value() : Value(entry.changefreq));
		output.SetValue(3, count, entry.priority.empty() ? Value() : Value(entry.priority));

		state.current_idx++;
		count++;
	}

	output.SetCardinality(count);
}

void RegisterSitemapFunction(ExtensionLoader &loader) {
	// Register function with VARCHAR parameter (single URL)
	TableFunction sitemap_func("sitemap_urls", {LogicalType::VARCHAR}, SitemapScan, SitemapBind, SitemapInitGlobal);
	sitemap_func.init_local = SitemapInitLocal;

	// Named parameters
	sitemap_func.named_parameters["follow_robots"] = LogicalType::BOOLEAN;
	sitemap_func.named_parameters["max_depth"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["max_retries"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["backoff_ms"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["max_backoff_ms"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;

	loader.RegisterFunction(sitemap_func);

	// Register function with LIST parameter (array of URLs)
	TableFunction sitemap_func_list("sitemap_urls", {LogicalType::LIST(LogicalType::VARCHAR)}, SitemapScan, SitemapBind, SitemapInitGlobal);
	sitemap_func_list.init_local = SitemapInitLocal;

	// Named parameters
	sitemap_func_list.named_parameters["follow_robots"] = LogicalType::BOOLEAN;
	sitemap_func_list.named_parameters["max_depth"] = LogicalType::INTEGER;
	sitemap_func_list.named_parameters["max_retries"] = LogicalType::INTEGER;
	sitemap_func_list.named_parameters["backoff_ms"] = LogicalType::INTEGER;
	sitemap_func_list.named_parameters["max_backoff_ms"] = LogicalType::INTEGER;
	sitemap_func_list.named_parameters["ignore_errors"] = LogicalType::BOOLEAN;

	loader.RegisterFunction(sitemap_func_list);
}

} // namespace duckdb
