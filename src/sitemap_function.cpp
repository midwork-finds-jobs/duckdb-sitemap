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
	std::vector<std::string> base_urls;
	bool follow_robots = true;
	int max_depth = 3;
	bool ignore_errors = false;
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

// Global init - fetch all sitemaps
static unique_ptr<GlobalTableFunctionState> SitemapInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<SitemapGlobalState>();
	auto &bind_data = input.bind_data->Cast<SitemapBindData>();

	// Process each base URL
	for (const auto &base_url : bind_data.base_urls) {
		std::vector<std::string> sitemap_urls;

		if (bind_data.follow_robots) {
			// Fetch robots.txt
			std::string robots_url = BuildUrl(base_url, "/robots.txt");
			auto response = HttpClient::Fetch(context, robots_url, bind_data.retry_config);

			if (response.success) {
				sitemap_urls = RobotsParser::ParseSitemapUrls(response.body);
			}
		}

		// If no sitemaps found in robots.txt, try common locations
		if (sitemap_urls.empty()) {
			sitemap_urls.push_back(BuildUrl(base_url, "/sitemap.xml"));
		}

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
			std::string error_msg = "Failed to fetch sitemap from " + base_url;
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
