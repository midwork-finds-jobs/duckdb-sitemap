#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterBruteforceFunction(ExtensionLoader &loader);

} // namespace duckdb
