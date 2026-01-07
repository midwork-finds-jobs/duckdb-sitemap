#include "sitemap_function.hpp"
#include "http_client.hpp"
#include "robots_parser.hpp"
#include "xml_parser.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include <algorithm>

namespace duckdb {

// Bind data for sitemap_urls() table function
struct SitemapBindData : public TableFunctionData {
	std::string base_url;
	bool follow_robots = true;
	int max_depth = 3;
	RetryConfig retry_config;
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

	auto response = HttpClient::Fetch(context, sitemap_url, bind_data.retry_config);

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

	// First positional argument is the base URL
	if (input.inputs.empty()) {
		throw InvalidInputException("sitemap_urls() requires a base_url argument");
	}

	bind_data->base_url = input.inputs[0].GetValue<std::string>();

	// Auto-prepend https:// if no protocol specified
	if (bind_data->base_url.find("://") == std::string::npos) {
		bind_data->base_url = "https://" + bind_data->base_url;
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
		}
	}

	// Set return types
	names = {"url", "lastmod", "changefreq", "priority"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};

	return std::move(bind_data);
}

// Global init - fetch all sitemaps
static unique_ptr<GlobalTableFunctionState> SitemapInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<SitemapGlobalState>();
	auto &bind_data = input.bind_data->Cast<SitemapBindData>();

	std::vector<std::string> sitemap_urls;

	if (bind_data.follow_robots) {
		// Fetch robots.txt
		std::string robots_url = BuildUrl(bind_data.base_url, "/robots.txt");
		auto response = HttpClient::Fetch(context, robots_url, bind_data.retry_config);

		if (response.success) {
			sitemap_urls = RobotsParser::ParseSitemapUrls(response.body);
		}
	}

	// If no sitemaps found in robots.txt, try common locations
	if (sitemap_urls.empty()) {
		sitemap_urls.push_back(BuildUrl(bind_data.base_url, "/sitemap.xml"));
	}

	// Fetch all sitemaps
	for (const auto &sitemap_url : sitemap_urls) {
		FetchSitemap(context, sitemap_url, *state, bind_data, 0);
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
	TableFunction sitemap_func("sitemap_urls", {LogicalType::VARCHAR}, SitemapScan, SitemapBind, SitemapInitGlobal);

	sitemap_func.init_local = SitemapInitLocal;

	// Named parameters
	sitemap_func.named_parameters["follow_robots"] = LogicalType::BOOLEAN;
	sitemap_func.named_parameters["max_depth"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["max_retries"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["backoff_ms"] = LogicalType::INTEGER;
	sitemap_func.named_parameters["max_backoff_ms"] = LogicalType::INTEGER;

	loader.RegisterFunction(sitemap_func);
}

} // namespace duckdb
