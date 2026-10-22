#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {

struct HttpResponse {
	int status_code = 0;
	//! Raw (possibly binary / gzipped) response body
	std::string body;
	std::string content_type;
	std::string retry_after;
	std::string error;
	bool success = false;
};

struct RetryConfig {
	int max_retries = 5;
	int initial_backoff_ms = 100;
	double backoff_multiplier = 2.0;
	int max_backoff_ms = 30000;
};

class HttpClient {
public:
	//! Fetch an http(s) URL with retries, or read a local file (file:// URL or plain path)
	static HttpResponse Fetch(ClientContext &context, const std::string &url, const RetryConfig &config,
	                          const std::string &user_agent = "");
	static bool IsHttpUrl(const std::string &url);

private:
	static HttpResponse ExecuteHttpGet(ClientContext &context, const std::string &url, const std::string &user_agent);
	static HttpResponse ReadLocalFile(ClientContext &context, const std::string &url);
	static bool IsRetryable(int status_code);
	static int ParseRetryAfter(const std::string &retry_after);
};

} // namespace duckdb
