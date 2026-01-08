#define DUCKDB_EXTENSION_MAIN

#include "sitemap_extension.hpp"
#include "sitemap_function.hpp"
#include "bruteforce_function.hpp"
#include "xml_parser.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);

	// Register sitemap_user_agent setting
	config.AddExtensionOption("sitemap_user_agent",
	                          "User agent string for sitemap HTTP requests",
	                          LogicalType::VARCHAR,
	                          Value("DuckDB-Sitemap/1.0"));

	Connection conn(db);

	// Install and load http_request from community
	auto install_result = conn.Query("INSTALL http_request FROM community");
	if (install_result->HasError()) {
		throw IOException("Sitemap extension requires http_request extension. Failed to install: " +
		                  install_result->GetError());
	}

	auto load_result = conn.Query("LOAD http_request");
	if (load_result->HasError()) {
		throw IOException("Sitemap extension requires http_request extension. Failed to load: " +
		                  load_result->GetError());
	}

	// Initialize libxml2
	XmlParser::Initialize();

	// Register sitemap_urls() table function
	RegisterSitemapFunction(loader);

	// Register bruteforce_find_sitemap() scalar function
	RegisterBruteforceFunction(loader);
}

void SitemapExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string SitemapExtension::Name() {
	return "sitemap";
}

std::string SitemapExtension::Version() const {
#ifdef EXT_VERSION_SITEMAP
	return EXT_VERSION_SITEMAP;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(sitemap, loader) {
	duckdb::LoadInternal(loader);
}
}
