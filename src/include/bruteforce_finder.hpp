#pragma once

#include <string>
#include <vector>

namespace duckdb {

class BruteforceFinder {
public:
	static std::vector<std::string> GetFilenames();
	static std::vector<std::string> GetFiletypes();
};

} // namespace duckdb
