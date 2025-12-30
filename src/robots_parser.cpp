#include "robots_parser.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace duckdb {

// Trim whitespace from both ends of a string
static std::string Trim(const std::string &str) {
	size_t start = 0;
	size_t end = str.length();

	while (start < end && std::isspace(static_cast<unsigned char>(str[start]))) {
		start++;
	}

	while (end > start && std::isspace(static_cast<unsigned char>(str[end - 1]))) {
		end--;
	}

	return str.substr(start, end - start);
}

// Case-insensitive starts with check
static bool StartsWithCaseInsensitive(const std::string &str, const std::string &prefix) {
	if (str.length() < prefix.length()) {
		return false;
	}

	for (size_t i = 0; i < prefix.length(); i++) {
		if (std::tolower(static_cast<unsigned char>(str[i])) !=
		    std::tolower(static_cast<unsigned char>(prefix[i]))) {
			return false;
		}
	}

	return true;
}

std::vector<std::string> RobotsParser::ParseSitemapUrls(const std::string &robots_txt_content) {
	std::vector<std::string> sitemaps;

	std::istringstream stream(robots_txt_content);
	std::string line;

	const std::string sitemap_prefix = "sitemap:";

	while (std::getline(stream, line)) {
		// Trim the line
		line = Trim(line);

		// Skip empty lines and comments
		if (line.empty() || line[0] == '#') {
			continue;
		}

		// Check for Sitemap: directive (case-insensitive)
		if (StartsWithCaseInsensitive(line, sitemap_prefix)) {
			// Extract URL after "Sitemap:"
			std::string url = Trim(line.substr(sitemap_prefix.length()));

			// Skip empty URLs
			if (!url.empty()) {
				sitemaps.push_back(url);
			}
		}
	}

	return sitemaps;
}

} // namespace duckdb
