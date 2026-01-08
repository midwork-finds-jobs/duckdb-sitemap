#include "bruteforce_function.hpp"
#include "bruteforce_finder.hpp"
#include "http_client.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// Build URL from base and path
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

// Scalar function implementation
static void BruteforceFindSitemapFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();

	// Get user agent from extension setting
	std::string user_agent;
	Value user_agent_value;
	if (context.TryGetCurrentSetting("sitemap_user_agent", user_agent_value)) {
		user_agent = user_agent_value.GetValue<std::string>();
	}

	// Get base_url from first argument
	auto &base_url_vector = args.data[0];
	UnifiedVectorFormat base_url_data;
	base_url_vector.ToUnifiedFormat(args.size(), base_url_data);
	auto base_urls = UnifiedVectorFormat::GetData<string_t>(base_url_data);

	RetryConfig retry_config;
	retry_config.max_retries = 0; // No retries for bruteforce (too many URLs to check)

	auto filenames = BruteforceFinder::GetFilenames();
	auto filetypes = BruteforceFinder::GetFiletypes();

	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto idx = base_url_data.sel->get_index(i);

		if (!base_url_data.validity.RowIsValid(idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		std::string base_url = base_urls[idx].GetString();

		// Auto-prepend https:// if no protocol
		if (base_url.find("://") == std::string::npos) {
			base_url = "https://" + base_url;
		}

		bool found = false;
		std::string found_url;

		// Try each combination of filename + filetype
		for (const auto &filename : filenames) {
			for (const auto &filetype : filetypes) {
				std::string url = BuildUrl(base_url, filename + "." + filetype);

				auto response = HttpClient::Fetch(context, url, retry_config, user_agent);

				// Check if we got a successful response with appropriate content type
				if (response.success && response.status_code >= 200 && response.status_code < 300) {
					std::string content_type_lower = response.content_type;
					std::transform(content_type_lower.begin(), content_type_lower.end(),
					              content_type_lower.begin(), ::tolower);

					// Check for xml, gzip, or plain text content types
					if (content_type_lower.find("xml") != std::string::npos ||
					    content_type_lower.find("gzip") != std::string::npos ||
					    content_type_lower.find("plain") != std::string::npos) {
						found_url = url;
						found = true;
						break;
					}
				}
			}

			if (found) {
				break;
			}
		}

		if (found) {
			result_data[i] = StringVector::AddString(result, found_url);
		} else {
			result_validity.SetInvalid(i);
		}
	}
}

void RegisterBruteforceFunction(ExtensionLoader &loader) {
	ScalarFunction bruteforce_func(
		"bruteforce_find_sitemap",
		{LogicalType::VARCHAR},
		LogicalType::VARCHAR,
		BruteforceFindSitemapFunction
	);

	loader.RegisterFunction(bruteforce_func);
}

} // namespace duckdb
