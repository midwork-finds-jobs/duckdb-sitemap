#pragma once

#include <string>
#include <vector>

namespace duckdb {

class RobotsParser {
public:
	static std::vector<std::string> ParseSitemapUrls(const std::string &robots_txt_content);
};

} // namespace duckdb
