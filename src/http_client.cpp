#include "http_client.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include <thread>
#include <chrono>
#include <cmath>
#include <cstdlib>

namespace duckdb {

bool HttpClient::IsRetryable(int status_code) {
	// Network errors (connection failures)
	if (status_code <= 0) {
		return true;
	}
	// Rate limited
	if (status_code == 429) {
		return true;
	}
	// Server errors
	if (status_code >= 500 && status_code <= 504) {
		return true;
	}
	return false;
}

int HttpClient::ParseRetryAfter(const std::string &retry_after) {
	if (retry_after.empty()) {
		return 0;
	}

	// Try to parse as integer (seconds)
	try {
		int seconds = std::stoi(retry_after);
		return seconds * 1000; // Convert to milliseconds
	} catch (...) {
		// Could be HTTP-date format, but for simplicity we'll just return 0
		// and fall back to exponential backoff
		return 0;
	}
}

bool HttpClient::IsHttpUrl(const std::string &url) {
	auto lower = StringUtil::Lower(url);
	return StringUtil::StartsWith(lower, "http://") || StringUtil::StartsWith(lower, "https://");
}

HttpResponse HttpClient::ExecuteHttpGet(ClientContext &context, const std::string &url, const std::string &user_agent) {
	HttpResponse response;
	auto &db = DatabaseInstance::GetDatabase(context);

	// Core HTTPUtil has no TLS; httpfs replaces it with a TLS-capable client
	if (StringUtil::StartsWith(StringUtil::Lower(url), "https://")) {
		ExtensionHelper::TryAutoLoadExtension(db, "httpfs");
	}

	try {
		auto &http_util = HTTPUtil::Get(db);
		auto params = http_util.InitializeParameters(context, url);
		// Sitemap has its own retry loop (Retry-After aware)
		params->retries = 0;
		params->follow_location = true;

		HTTPHeaders headers(db);
		if (!user_agent.empty()) {
			headers["User-Agent"] = user_agent;
		}

		GetRequestInfo get_request(url, headers, *params, nullptr, nullptr);
		get_request.try_request = true;

		auto http_response = http_util.Request(get_request);
		if (!http_response) {
			response.error = "No response from HTTP request";
			return response;
		}
		if (http_response->HasRequestError()) {
			response.error = http_response->GetRequestError();
			return response;
		}
		response.status_code = static_cast<int>(http_response->status);
		response.body = std::move(http_response->body);
		response.content_type = http_response->GetHeaderValue("Content-Type");
		response.retry_after = http_response->GetHeaderValue("Retry-After");
	} catch (std::exception &ex) {
		ErrorData error(ex);
		response.error = error.RawMessage();
		response.status_code = 0;
		return response;
	}

	response.success = (response.status_code >= 200 && response.status_code < 300);
	if (!response.success && response.error.empty()) {
		response.error = "HTTP " + std::to_string(response.status_code);
	}
	return response;
}

HttpResponse HttpClient::ReadLocalFile(ClientContext &context, const std::string &url) {
	HttpResponse response;
	std::string path = url;
	if (StringUtil::StartsWith(StringUtil::Lower(path), "file://")) {
		path = path.substr(7);
	}
	try {
		auto &fs = FileSystem::GetFileSystem(context);
		auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
		auto size = handle->GetFileSize();
		response.body.resize(size);
		if (size > 0) {
			handle->Read(&response.body[0], size, 0);
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		response.error = error.RawMessage();
		response.status_code = 404;
		return response;
	}
	response.status_code = 200;
	response.success = true;
	return response;
}

HttpResponse HttpClient::Fetch(ClientContext &context, const std::string &url, const RetryConfig &config,
                               const std::string &user_agent) {
	if (!IsHttpUrl(url)) {
		return ReadLocalFile(context, url);
	}

	for (int attempt = 0; attempt <= config.max_retries; attempt++) {
		auto response = ExecuteHttpGet(context, url, user_agent);

		if (response.success) {
			return response;
		}

		// Check if we should retry
		if (!IsRetryable(response.status_code)) {
			return response; // Non-retryable error
		}

		// Check if we've exhausted retries
		if (attempt >= config.max_retries) {
			response.error = "Max retries exceeded for URL: " + url;
			return response;
		}

		// Calculate wait time
		int wait_ms;
		if (response.status_code == 429 && !response.retry_after.empty()) {
			// Respect Retry-After header
			wait_ms = ParseRetryAfter(response.retry_after);
			if (wait_ms <= 0) {
				// Fall back to exponential backoff
				wait_ms = static_cast<int>(config.initial_backoff_ms * std::pow(config.backoff_multiplier, attempt));
			}
		} else {
			// Exponential backoff
			wait_ms = static_cast<int>(config.initial_backoff_ms * std::pow(config.backoff_multiplier, attempt));
		}

		// Cap at max backoff
		wait_ms = std::min(wait_ms, config.max_backoff_ms);

		// Add jitter (10%)
		int jitter = wait_ms / 10;
		if (jitter > 0) {
			wait_ms += (std::rand() % (2 * jitter)) - jitter;
		}

		// Wait before retry
		std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
	}

	// Should not reach here
	HttpResponse response;
	response.error = "Max retries exceeded";
	return response;
}

} // namespace duckdb
